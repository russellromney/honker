/**
 * The SQLite entry point exported by the Honker extension.
 *
 * SQLite normally derives this from the file name, so you only need it
 * when loading the extension under a name other than
 * `libhonker_ext.{so,dylib}` / `honker_ext.dll`.
 */
export declare const EXTENSION_ENTRYPOINT: 'sqlite3_honkerext_init'

/**
 * Absolute path to the bundled Honker SQLite extension.
 *
 * Resolution order: `HONKER_EXTENSION_PATH`, then the platform package
 * installed alongside this one, then an in-repo `target/release` build.
 * Throws naming every path searched when none exists.
 *
 * Does not load the native addon, so it costs nothing to call from a
 * process that already has its own SQLite.
 */
export declare function extensionPath(): string

/** The extension path and its entry point together. */
export declare function extensionInfo(): { path: string; entrypoint: string }
