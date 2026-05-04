using System.Runtime.InteropServices;
using System.Text;

namespace Honker;

internal sealed class UpdatePoller : IDisposable
{
    private delegate IntPtr WatcherOpen(
        [MarshalAs(UnmanagedType.LPUTF8Str)] string dbPath,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string? backend,
        byte[] error,
        UIntPtr errorLen);

    private delegate int WatcherWait(IntPtr watcher, ulong timeoutMs);
    private delegate void WatcherClose(IntPtr watcher);

    private readonly IntPtr _library;
    private readonly IntPtr _watcher;
    private readonly WatcherWait _wait;
    private readonly WatcherClose _close;
    private bool _disposed;

    public UpdatePoller(string dbPath, string extensionPath, string? watcherBackend)
    {
        _library = NativeLibrary.Load(extensionPath);
        try
        {
            var open = Marshal.GetDelegateForFunctionPointer<WatcherOpen>(
                NativeLibrary.GetExport(_library, "honker_watcher_open")
            );
            _wait = Marshal.GetDelegateForFunctionPointer<WatcherWait>(
                NativeLibrary.GetExport(_library, "honker_watcher_wait")
            );
            _close = Marshal.GetDelegateForFunctionPointer<WatcherClose>(
                NativeLibrary.GetExport(_library, "honker_watcher_close")
            );

            var error = new byte[1024];
            _watcher = open(dbPath, watcherBackend, error, (UIntPtr)error.Length);
            if (_watcher == IntPtr.Zero)
            {
                throw new InvalidOperationException(DecodeError(error));
            }
        }
        catch
        {
            NativeLibrary.Free(_library);
            throw;
        }
    }

    public async Task WaitForChangeAsync(TimeSpan? timeout, CancellationToken cancellationToken)
    {
        if (timeout is not null && timeout <= TimeSpan.Zero)
        {
            return;
        }

        // The native `_wait` blocks the calling thread for up to its
        // timeout. Wrapping in Task.Run pushes the block onto the
        // thread pool so the caller's runtime thread stays free to
        // service other awaits.
        //
        // Cancellation can't preempt the native wait (no API to do so),
        // so we still chunk into smaller blocks (default 500 ms) to
        // re-check the cancellation token periodically. 500 ms is
        // generous enough that overhead is negligible but tight enough
        // that a cancel surfaces quickly.
        var deadline = timeout is null ? (DateTime?)null : DateTime.UtcNow + timeout.Value;
        var watcher = _watcher;
        var wait = _wait;
        while (deadline is null || DateTime.UtcNow < deadline.Value)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var remaining = deadline is null ? TimeSpan.FromMilliseconds(500) : deadline.Value - DateTime.UtcNow;
            if (deadline is not null && remaining <= TimeSpan.Zero)
            {
                return;
            }

            var chunk = (ulong)Math.Min(500, Math.Max(1, (int)Math.Ceiling(remaining.TotalMilliseconds)));
            var code = await Task.Run(() => wait(watcher, chunk), cancellationToken).ConfigureAwait(false);
            switch (code)
            {
                case 1:
                    return;
                case 0:
                    continue;
                default:
                    throw new InvalidOperationException("honker update watcher closed or died");
            }
        }
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }
        _disposed = true;
        _close(_watcher);
        NativeLibrary.Free(_library);
    }

    private static string DecodeError(byte[] error)
    {
        var len = Array.IndexOf(error, (byte)0);
        if (len < 0)
        {
            len = error.Length;
        }
        return len == 0 ? "unknown watcher error" : Encoding.UTF8.GetString(error, 0, len);
    }
}
