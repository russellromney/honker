using Microsoft.Data.Sqlite;

namespace Honker;

public sealed class OpenOptions
{
    public string? ExtensionPath { get; init; }
    public TimeSpan UpdatePollInterval { get; init; } = TimeSpan.FromMilliseconds(5);
    /// <summary>
    /// Update-detection backend implemented by honker-core through the
    /// loaded Honker extension. Accepted aliases match the core contract:
    /// null / "", "poll", "polling", "kernel", "kernel-watcher", "shm",
    /// and "shm-fast-path".
    /// </summary>
    public string? WatcherBackend { get; init; }

    internal string BuildConnectionString(string path)
    {
        var builder = new SqliteConnectionStringBuilder
        {
            DataSource = path,
            Mode = SqliteOpenMode.ReadWriteCreate,
            Cache = SqliteCacheMode.Default,
            Pooling = false,
        };

        return builder.ToString();
    }
}
