namespace Honker;

/// <summary>
/// Locates the Honker SQLite loadable extension bundled in this package.
/// </summary>
/// <remarks>
/// For code that wants to load Honker onto a <c>SqliteConnection</c> it
/// already owns — an EF Core or Dapper connection, say — instead of
/// going through <see cref="Database.Open(string, OpenOptions?)"/>.
/// Enqueueing outside the application's transaction loses atomicity,
/// which is the reason to do it this way.
///
/// The package already knew how to find the extension; it just had no
/// way to tell you. This exposes the same resolver
/// <see cref="Database"/> uses, so both agree by construction.
/// </remarks>
public static class HonkerExtension
{
    /// <summary>
    /// The SQLite entry point exported by the extension.
    /// </summary>
    /// <remarks>
    /// SQLite normally derives this from the file name, so it only
    /// matters when loading the extension under a name other than
    /// <c>libhonker_ext.{so,dylib}</c> / <c>honker_ext.dll</c>.
    /// </remarks>
    public const string Entrypoint = "sqlite3_honkerext_init";

    /// <summary>
    /// Absolute path to the extension.
    /// </summary>
    /// <param name="options">
    /// Optional overrides. <c>ExtensionPath</c> wins, then
    /// <c>HONKER_EXTENSION_PATH</c>, then the bundled native asset.
    /// </param>
    /// <exception cref="InvalidOperationException">
    /// Thrown when no extension is found. There is no silent fallback.
    /// </exception>
    public static string Locate(OpenOptions? options = null) =>
        Database.LocateExtension(options ?? new OpenOptions());
}
