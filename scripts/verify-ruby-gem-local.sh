#!/usr/bin/env bash
set -euo pipefail

# Local pre-push check for the honker Ruby gem. Not run by CI; it is a
# developer convenience that reproduces in one command what CI checks
# across ci.yml (specs) and release-ruby.yml (gem build, proof, smoke).
#
# Builds and installs both the precompiled platform gem and the
# compile-on-install generic gem, runs a queue round-trip smoke test
# against each, and runs the specs.
#
#   scripts/verify-ruby-gem-local.sh            host checks
#   scripts/verify-ruby-gem-local.sh --docker   also a Linux build in Docker
#
# Needs ruby, gem, bundle, and cargo on PATH; docker too for --docker.

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUBY_PKG="$ROOT/packages/honker-ruby"
WITH_DOCKER=0
[[ "${1:-}" == "--docker" ]] && WITH_DOCKER=1

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
cd "$ROOT"

echo "== build extension =="
cargo build --release -p honker-extension

echo
echo "== platform gem =="
scripts/copy-ruby-extension.sh
platform="$(ruby -e 'p = Gem::Platform.local; print [p.cpu, p.os].join("-")')"
( cd "$RUBY_PKG" && HONKER_GEM_PLATFORM="$platform" \
    gem build honker.gemspec --output "$TMP/platform.gem" )
ruby scripts/proof/check-ruby-gem.rb "$TMP/platform.gem"
GEM_HOME="$TMP/platform-home" GEM_PATH="$TMP/platform-home" \
  gem install --no-document "$TMP/platform.gem"
GEM_HOME="$TMP/platform-home" GEM_PATH="$TMP/platform-home" \
  ruby scripts/proof/ruby-gem-smoke.rb

echo
echo "== generic gem (compiles on install) =="
scripts/copy-ruby-sources.sh
( cd "$RUBY_PKG" && gem build honker.gemspec --output "$TMP/generic.gem" )
ruby scripts/proof/check-ruby-gem.rb --generic "$TMP/generic.gem"
GEM_HOME="$TMP/generic-home" GEM_PATH="$TMP/generic-home" \
  gem install --no-document "$TMP/generic.gem"
GEM_HOME="$TMP/generic-home" GEM_PATH="$TMP/generic-home" \
  ruby scripts/proof/ruby-gem-smoke.rb

echo
echo "== specs =="
(
  cd "$RUBY_PKG"
  bundle install --quiet
  for spec in honker_spec smoke_spec parity_spec extension_resolution_spec; do
    echo "  -- $spec --"
    bundle exec ruby -Ilib "spec/$spec.rb"
  done
)

if [[ "$WITH_DOCKER" == "1" ]]; then
  echo
  echo "== docker: generic gem compiles on install (linux/amd64) =="
  docker run --rm --platform linux/amd64 -v "$ROOT:/work" -w /work rust:latest \
    bash -euo pipefail -c '
      export DEBIAN_FRONTEND=noninteractive
      apt-get update -qq
      apt-get install -y -qq --no-install-recommends ruby ruby-dev make >/dev/null
      scripts/copy-ruby-sources.sh >/dev/null
      cd packages/honker-ruby
      gem build honker.gemspec --output /tmp/honker.gem >/dev/null
      ruby ../../scripts/proof/check-ruby-gem.rb --generic /tmp/honker.gem
      gem install --no-document /tmp/honker.gem >/dev/null
      ruby ../../scripts/proof/ruby-gem-smoke.rb
    '

  echo
  echo "== docker: install without Rust fails cleanly (linux/amd64) =="
  no_rust_out="$(docker run --rm --platform linux/amd64 \
    -v "$TMP/generic.gem:/tmp/honker.gem:ro" ruby:3.3 \
    bash -c 'gem install --no-document /tmp/honker.gem' 2>&1 || true)"
  if echo "$no_rust_out" | grep -q "Rust toolchain"; then
    echo "  install failed with the expected Rust-toolchain error"
  else
    echo "  ERROR: expected the missing-Rust error message; got:" >&2
    echo "$no_rust_out" >&2
    exit 1
  fi
fi

echo
echo "ruby gem verification ok"
