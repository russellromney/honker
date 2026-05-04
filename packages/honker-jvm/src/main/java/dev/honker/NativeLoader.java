package dev.honker;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

final class NativeLoader {
    static final String ENTRYPOINT = "sqlite3_honkerext_init";

    private NativeLoader() {
    }

    static Path resolve(OpenOptions options) {
        List<Path> searched = new ArrayList<>();
        Path explicit = options.extensionPath();
        if (explicit != null) {
            Path p = explicit.toAbsolutePath().normalize();
            if (Files.isRegularFile(p)) {
                return p;
            }
            searched.add(p);
            throw new HonkerLoadException("Honker extension path does not exist: " + p);
        }

        String env = System.getenv("HONKER_EXTENSION_PATH");
        if (env != null && !env.isBlank()) {
            Path p = Path.of(env).toAbsolutePath().normalize();
            if (Files.isRegularFile(p)) {
                return p;
            }
            searched.add(p);
        }

        Path packaged = extractPackaged();
        if (packaged != null) {
            return packaged;
        }

        for (Path candidate : localBuildCandidates()) {
            searched.add(candidate);
            if (Files.isRegularFile(candidate)) {
                return candidate;
            }
        }

        throw new HonkerLoadException(
            "Could not find Honker SQLite extension for platform " + platform()
                + ". Searched: " + searched
                + ". Set HONKER_EXTENSION_PATH or OpenOptions.extensionPath(...)."
        );
    }

    private static Path extractPackaged() {
        String resource = "/runtimes/" + platform() + "/native/" + libraryName();
        try (var in = NativeLoader.class.getResourceAsStream(resource)) {
            if (in == null) {
                return null;
            }
            Path dir = Files.createTempDirectory("honker-jvm-native-");
            Path out = dir.resolve(libraryName());
            Files.copy(in, out);
            out.toFile().deleteOnExit();
            dir.toFile().deleteOnExit();
            return out;
        } catch (IOException e) {
            throw new HonkerLoadException("Failed to extract packaged Honker extension", e);
        }
    }

    private static List<Path> localBuildCandidates() {
        List<Path> out = new ArrayList<>();
        Path cwd = Path.of(System.getProperty("user.dir")).toAbsolutePath().normalize();
        for (Path p = cwd; p != null; p = p.getParent()) {
            out.add(p.resolve("target/debug").resolve(libraryName()));
            out.add(p.resolve("target/release").resolve(libraryName()));
        }
        return out;
    }

    private static String libraryName() {
        String os = System.getProperty("os.name", "").toLowerCase();
        if (os.contains("win")) {
            return "honker_ext.dll";
        }
        if (os.contains("mac") || os.contains("darwin")) {
            return "libhonker_ext.dylib";
        }
        return "libhonker_ext.so";
    }

    private static String platform() {
        String os = System.getProperty("os.name", "").toLowerCase();
        String arch = System.getProperty("os.arch", "").toLowerCase();
        String osPart;
        if (os.contains("win")) {
            osPart = "win";
        } else if (os.contains("mac") || os.contains("darwin")) {
            osPart = "osx";
        } else {
            osPart = "linux";
        }
        String archPart = (arch.contains("aarch64") || arch.contains("arm64")) ? "arm64" : "x64";
        return osPart + "-" + archPart;
    }
}
