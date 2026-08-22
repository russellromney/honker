package dev.honker;

import java.nio.file.Path;

/**
 * Locates the Honker SQLite loadable extension bundled in this artifact.
 *
 * <p>For code that wants to load Honker onto a JDBC connection it
 * already owns — a Hibernate or jOOQ connection, say — instead of going
 * through {@link Database}. Enqueueing outside the application's
 * transaction loses atomicity, which is the reason to do it this way.
 *
 * <p>The jar already knew how to find the extension; it just had no way
 * to tell you. This exposes the same resolver {@link Database} uses, so
 * both agree by construction.
 *
 * <pre>{@code
 * Path ext = HonkerExtension.path();
 * try (Statement stmt = conn.createStatement()) {
 *     stmt.execute("SELECT load_extension('" + ext + "', '"
 *         + HonkerExtension.entrypoint() + "')");
 *     stmt.execute("SELECT honker_bootstrap()");
 * }
 * }</pre>
 */
public final class HonkerExtension {

    private HonkerExtension() {
    }

    /**
     * The SQLite entry point exported by the extension.
     *
     * <p>SQLite normally derives this from the file name, so it only
     * matters when loading the extension under a name other than
     * {@code libhonker_ext.{so,dylib}} / {@code honker_ext.dll}.
     *
     * @return the entry point symbol
     */
    public static String entrypoint() {
        return NativeLoader.ENTRYPOINT;
    }

    /**
     * Absolute path to the extension, extracting the packaged copy to a
     * temporary file if that is where it lives.
     *
     * <p>Resolution order is {@code HONKER_EXTENSION_PATH}, then the
     * bundled native asset, then a local build. There is no silent
     * fallback.
     *
     * @return the extension path
     * @throws HonkerLoadException when no extension is found
     */
    public static Path path() {
        return path(OpenOptions.builder().build());
    }

    /**
     * Absolute path to the extension, honouring
     * {@link OpenOptions#extensionPath()} when set.
     *
     * @param options open options whose extension path takes precedence
     * @return the extension path
     * @throws HonkerLoadException when no extension is found
     */
    public static Path path(OpenOptions options) {
        return NativeLoader.resolve(options);
    }
}
