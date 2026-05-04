using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Channels;

namespace Honker;

/// <summary>
/// Bridges honker-core's blocking native watcher into idiomatic async
/// .NET. ONE background thread per poller calls the native `_wait` in
/// a loop; when it returns a wake, the dispatcher fans out to every
/// active <see cref="WaitForChangeAsync"/> caller via a per-call
/// <see cref="Channel{T}"/>.
///
/// This keeps the .NET thread pool free — a hundred concurrent
/// subscribers cost ONE blocking thread, not a hundred. Earlier
/// implementations chunked the native wait into 100 ms-500 ms blocks
/// per caller, which exhausted the pool under any real concurrency
/// (xUnit running multiple async tests in parallel hit it routinely).
/// </summary>
internal sealed class UpdatePoller : IDisposable
{
    private delegate IntPtr WatcherOpen(
        [MarshalAs(UnmanagedType.LPUTF8Str)] string dbPath,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string? backend,
        byte[] error,
        UIntPtr errorLen);

    private delegate int WatcherWait(IntPtr watcher, ulong timeoutMs);
    private delegate void WatcherClose(IntPtr watcher);
    private delegate void WatcherSignalClose(IntPtr watcher);

    /// <summary>
    /// How long the dispatcher's native call blocks per loop iteration.
    /// Long so the thread isn't spinning, short enough that Dispose's
    /// shutdown latency is bounded.
    /// </summary>
    private const ulong DispatchChunkMs = 60_000;

    private readonly IntPtr _library;
    private readonly IntPtr _watcher;
    private readonly WatcherWait _wait;
    private readonly WatcherClose _close;
    private readonly WatcherSignalClose _signalClose;
    private readonly Thread _dispatcher;
    private readonly object _waitersLock = new();
    private readonly List<Channel<bool>> _waiters = new();
    private volatile bool _shutdown;
    private volatile bool _watcherDied;
    private bool _disposed;

    public UpdatePoller(string dbPath, string extensionPath, string? watcherBackend)
    {
        _library = NativeLibrary.Load(extensionPath);
        try
        {
            var open = Marshal.GetDelegateForFunctionPointer<WatcherOpen>(
                NativeLibrary.GetExport(_library, "honker_watcher_open"));
            _wait = Marshal.GetDelegateForFunctionPointer<WatcherWait>(
                NativeLibrary.GetExport(_library, "honker_watcher_wait"));
            _close = Marshal.GetDelegateForFunctionPointer<WatcherClose>(
                NativeLibrary.GetExport(_library, "honker_watcher_close"));
            _signalClose = Marshal.GetDelegateForFunctionPointer<WatcherSignalClose>(
                NativeLibrary.GetExport(_library, "honker_watcher_signal_close"));

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

        _dispatcher = new Thread(DispatchLoop)
        {
            IsBackground = true,
            Name = "honker-update-dispatcher",
        };
        _dispatcher.Start();
    }

    /// <summary>
    /// Await the next database update. Returns when:
    ///   - the watcher fires (a commit was observed), OR
    ///   - <paramref name="timeout"/> elapses (returns silently), OR
    ///   - <paramref name="cancellationToken"/> fires (throws OCE), OR
    ///   - the watcher dies (throws InvalidOperationException).
    ///
    /// Pure async — does not block the calling thread or any thread
    /// pool thread. The single dispatcher thread handles the native
    /// blocking call for everyone.
    /// </summary>
    public async Task WaitForChangeAsync(TimeSpan? timeout, CancellationToken cancellationToken)
    {
        if (timeout is not null && timeout <= TimeSpan.Zero)
        {
            return;
        }

        cancellationToken.ThrowIfCancellationRequested();

        // Each waiter gets a private channel. Bounded(1) is enough —
        // multiple wakes between this caller's await and the next are
        // semantically equivalent to one wake.
        var channel = Channel.CreateBounded<bool>(new BoundedChannelOptions(1)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
            SingleReader = true,
            SingleWriter = false,
        });

        lock (_waitersLock)
        {
            if (_watcherDied)
            {
                throw new InvalidOperationException("honker update watcher closed or died");
            }
            if (_shutdown)
            {
                throw new ObjectDisposedException(nameof(UpdatePoller));
            }
            _waiters.Add(channel);
        }

        try
        {
            using var timeoutCts = timeout.HasValue
                ? CancellationTokenSource.CreateLinkedTokenSource(cancellationToken)
                : null;
            timeoutCts?.CancelAfter(timeout!.Value);
            var token = timeoutCts?.Token ?? cancellationToken;

            try
            {
                await channel.Reader.ReadAsync(token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
                when (timeout.HasValue && !cancellationToken.IsCancellationRequested)
            {
                // Timeout expired naturally — caller's contract is
                // "return silently on timeout."
                return;
            }
            catch (ChannelClosedException)
            {
                throw new InvalidOperationException("honker update watcher closed or died");
            }
        }
        finally
        {
            lock (_waitersLock)
            {
                _waiters.Remove(channel);
            }
            channel.Writer.TryComplete();
        }
    }

    private void DispatchLoop()
    {
        while (!_shutdown)
        {
            var code = _wait(_watcher, DispatchChunkMs);
            if (_shutdown)
            {
                return;
            }

            switch (code)
            {
                case 1:
                    // Wake — fan out to every active waiter.
                    Channel<bool>[] snapshot;
                    lock (_waitersLock)
                    {
                        snapshot = _waiters.ToArray();
                    }
                    foreach (var ch in snapshot)
                    {
                        // DropOldest channel: TryWrite always succeeds.
                        ch.Writer.TryWrite(true);
                    }
                    break;
                case 0:
                    // Timeout on this dispatch chunk; loop and call
                    // _wait again. Cheap on the dispatcher thread,
                    // invisible to waiters.
                    break;
                default:
                    // -1 closed, -2 internal panic. Watcher won't
                    // recover. Mark dead, complete all pending
                    // waiters with a closed channel, and exit.
                    _watcherDied = true;
                    Channel<bool>[] dyingSnapshot;
                    lock (_waitersLock)
                    {
                        dyingSnapshot = _waiters.ToArray();
                    }
                    foreach (var ch in dyingSnapshot)
                    {
                        ch.Writer.TryComplete(
                            new InvalidOperationException("honker update watcher closed or died"));
                    }
                    return;
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
        _shutdown = true;

        // Use-after-free hazard: honker_watcher_close drops the handle
        // (including its receiver). If the dispatcher is mid-recv on
        // that receiver, the drop is a UAF and recv panics. Sequence:
        //
        //   1. signal_close: unsubscribes the handle's sender. Any
        //      blocked recv_timeout returns Disconnected (`-1` from
        //      _wait). Handle stays valid.
        //   2. Dispatcher sees -1, completes waiters, returns.
        //   3. Join the dispatcher — guarantees no thread is inside
        //      _wait anymore.
        //   4. Now safe to call close to free the handle.
        _signalClose(_watcher);
        if (!_dispatcher.Join(TimeSpan.FromSeconds(5)))
        {
            // Dispatcher didn't exit in 5s. Force the close anyway —
            // the worst that can happen now is the same UAF the split
            // was meant to avoid, but we're already in trouble.
        }
        _close(_watcher);

        // Snapshot + complete any stragglers, in case a caller is
        // still inside WaitForChangeAsync when we get here.
        Channel<bool>[] snapshot;
        lock (_waitersLock)
        {
            snapshot = _waiters.ToArray();
            _waiters.Clear();
        }
        foreach (var ch in snapshot)
        {
            ch.Writer.TryComplete();
        }

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
