# lib/honker

Ruby source for the `honker` gem.

## The bundled SQLite extension

Released **platform gems** also ship the prebuilt Honker SQLite loadable
extension in this directory. It is not checked into the source
repository: it is a build artifact, gitignored, copied in at release
time by `scripts/copy-ruby-extension.sh` and verified by
`scripts/proof/check-ruby-gem.rb`.

When it is present, `Honker::Database.new` finds and loads it
automatically, so a platform gem needs no `extension_path:`.

| Gem platform    | Bundled extension file |
| --------------- | ---------------------- |
| `x86_64-linux`  | `libhonker_ext.so`     |
| `aarch64-linux` | `libhonker_ext.so`     |
| `arm64-darwin`  | `libhonker_ext.dylib`  |

The artifact name follows the target OS: `libhonker_ext.so` on Linux,
`libhonker_ext.dylib` on macOS, and `honker_ext.dll` on Windows.

The **generic gem** (used on every other platform, and for `github:`
installs) ships no prebuilt extension. Instead it bundles the Rust crate
source under `ext/honker/` and compiles the extension into this
directory on install, which needs a [Rust toolchain](https://rustup.rs).
