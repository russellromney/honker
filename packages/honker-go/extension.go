package honker

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// ExtensionEntrypoint is the SQLite entry point exported by the Honker
// extension.
//
// When no entry point is given, SQLite derives one from the file name.
// That works for the canonical libhonker_ext.{so,dylib} /
// honker_ext.dll. Pass this explicitly if the library is named anything
// else — the derivation is version-dependent.
const ExtensionEntrypoint = "sqlite3_honkerext_init"

// ExtensionFilename is the extension's file name on this platform.
func ExtensionFilename() string {
	switch runtime.GOOS {
	case "windows":
		return "honker_ext.dll"
	case "darwin":
		return "libhonker_ext.dylib"
	default:
		return "libhonker_ext.so"
	}
}

// ExtensionPath locates the Honker SQLite extension.
//
// Go modules cannot ship a compiled binary, so unlike the Python, Ruby,
// .NET, and JVM packages there is nothing bundled to fall back on. Set
// HONKER_EXTENSION_PATH, or download the extension from a Honker
// release:
//
//	https://github.com/russellromney/honker/releases
//
// Resolution order is HONKER_EXTENSION_PATH, then the extension next to
// the running binary, then the working directory. A miss returns an
// error naming every path tried; nothing is guessed.
//
// Use it to fill in the extensionPath argument to Open, or to load
// Honker onto a *sql.DB you already own.
func ExtensionPath() (string, error) {
	if env := os.Getenv("HONKER_EXTENSION_PATH"); env != "" {
		if isFile(env) {
			return env, nil
		}
		return "", fmt.Errorf("honker: HONKER_EXTENSION_PATH does not exist: %s", env)
	}

	name := ExtensionFilename()
	var searched []string

	if exe, err := os.Executable(); err == nil {
		searched = append(searched, filepath.Join(filepath.Dir(exe), name))
	}
	if cwd, err := os.Getwd(); err == nil {
		searched = append(searched, filepath.Join(cwd, name))
	}

	for _, candidate := range searched {
		if isFile(candidate) {
			return candidate, nil
		}
	}

	return "", fmt.Errorf(
		"honker: SQLite extension not found. The Go binding ships no binary — "+
			"set HONKER_EXTENSION_PATH, or download honker-ext-<target>.tar.gz "+
			"from https://github.com/russellromney/honker/releases and extract "+
			"%s from it, or build it with "+
			"`cargo build --release -p honker-extension`. Keep the file name as-is: "+
			"SQLite derives the extension entry point from it. Searched: %s",
		name,
		strings.Join(searched, ", "),
	)
}

func isFile(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.Mode().IsRegular()
}
