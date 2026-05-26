# frozen_string_literal: true
#
# Builds the Honker SQLite loadable extension when the generic (source)
# gem is installed. Precompiled platform gems ship the extension and do
# not run this. A Rust toolchain is required; see https://rustup.rs.
#
# RubyGems runs this script and then `make`. It does the cargo build
# itself, drops the artifact in lib/honker/, and writes a no-op Makefile.

require "rbconfig"
require "fileutils"

ext_dir = __dir__

manifest = [
  File.join(ext_dir, "honker-extension", "Cargo.toml"),                 # vendored in the gem
  File.expand_path("../../../../honker-extension/Cargo.toml", ext_dir), # repo checkout (github:)
].find { |path| File.file?(path) }

if manifest.nil?
  abort "honker: honker-extension crate source not found; cannot build the extension"
end

cargo_found = system("cargo", "--version", out: File::NULL, err: File::NULL)
unless cargo_found
  abort <<~MSG
    honker: cannot build the SQLite extension because the Rust toolchain
    (cargo) was not found on PATH.

    This is the honker source gem; it compiles the extension on install.
    To fix this, either:
      * install Rust from https://rustup.rs and reinstall honker, or
      * use a precompiled platform gem (published for x86_64 Linux,
        arm64 Linux, and Apple Silicon macOS).
  MSG
end

ext_name =
  case RbConfig::CONFIG.fetch("host_os")
  when /mswin|mingw|cygwin/ then "honker_ext.dll"
  when /darwin/ then "libhonker_ext.dylib"
  else "libhonker_ext.so"
  end

target_dir = File.join(ext_dir, "target")

puts "honker: building the SQLite extension with cargo"
system(
  { "CARGO_TARGET_DIR" => target_dir },
  "cargo", "build", "--release", "--manifest-path", manifest,
  exception: true,
)

artifact = File.join(target_dir, "release", ext_name)
abort "honker: cargo build did not produce #{artifact}" unless File.file?(artifact)

dest_dir = File.expand_path("../../lib/honker", ext_dir)
FileUtils.mkdir_p(dest_dir)
FileUtils.cp(artifact, File.join(dest_dir, ext_name))
FileUtils.rm_rf(target_dir)
puts "honker: extension ready at lib/honker/#{ext_name}"

# RubyGems runs `make` after this script; the build is already done, so
# the Makefile only needs targets that succeed as no-ops.
File.write(File.join(ext_dir, "Makefile"), <<~MAKEFILE)
  all:
  clean:
  install:
MAKEFILE
