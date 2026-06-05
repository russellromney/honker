using System.Runtime.InteropServices;
using System.Text;

namespace Honker;

internal sealed class UpdatePoller : IDisposable
{
    private delegate IntPtr WatcherOpenV2(
        [MarshalAs(UnmanagedType.LPUTF8Str)] string dbPath,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string? backend,
        ulong watcherPollIntervalMs,
        byte[] error,
        UIntPtr errorLen);

    private delegate int WatcherWait(IntPtr watcher, ulong timeoutMs);
    private delegate void WatcherClose(IntPtr watcher);

    private readonly IntPtr _library;
    private readonly IntPtr _watcher;
    private readonly WatcherWait _wait;
    private readonly WatcherClose _close;
    private bool _disposed;

    public UpdatePoller(string dbPath, string extensionPath, string? watcherBackend, TimeSpan updatePollInterval)
    {
        if (updatePollInterval <= TimeSpan.Zero)
        {
            throw new ArgumentException("UpdatePollInterval must be positive", nameof(updatePollInterval));
        }
        _library = NativeLibrary.Load(extensionPath);
        try
        {
            var open = Marshal.GetDelegateForFunctionPointer<WatcherOpenV2>(
                NativeLibrary.GetExport(_library, "honker_watcher_open_v2")
            );
            _wait = Marshal.GetDelegateForFunctionPointer<WatcherWait>(
                NativeLibrary.GetExport(_library, "honker_watcher_wait")
            );
            _close = Marshal.GetDelegateForFunctionPointer<WatcherClose>(
                NativeLibrary.GetExport(_library, "honker_watcher_close")
            );

            var error = new byte[1024];
            var pollIntervalMs = Math.Max(1UL, (ulong)Math.Ceiling(updatePollInterval.TotalMilliseconds));
            _watcher = open(dbPath, watcherBackend, pollIntervalMs, error, (UIntPtr)error.Length);
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

        var deadline = timeout is null ? (DateTime?)null : DateTime.UtcNow + timeout.Value;
        while (deadline is null || DateTime.UtcNow < deadline.Value)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var remaining = deadline is null ? TimeSpan.FromMilliseconds(100) : deadline.Value - DateTime.UtcNow;
            if (deadline is not null && remaining <= TimeSpan.Zero)
            {
                return;
            }

            var chunk = Math.Min(100, Math.Max(1, (int)Math.Ceiling(remaining.TotalMilliseconds)));
            var code = _wait(_watcher, (ulong)chunk);
            switch (code)
            {
                case 1:
                    return;
                case 0:
                    await Task.Yield();
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
