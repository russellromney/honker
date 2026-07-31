# Contributing

Short notes on repo layout, tests, and releases.

## Layout

- `honker-core/` — shared Rust crate (rlib). Published to crates.io.
- `honker-extension/` — SQLite loadable extension (cdylib). Published to crates.io.
- `packages/` — language bindings, maintained in-tree: `honker` (Python), `honker-node`, `honker-rs`, `honker-go`, `honker-ruby`, `honker-bun`, `honker-ex`, `honker-cpp`, `honker-dotnet`, `honker-jvm`, `honker-kotlin`.
- `site/` — honker.dev (Astro Starlight; git submodule).
- `tests/` — cross-binding integration tests.

## Running tests

```bash
make test           # rust core + python + node, fast
make test-python-slow   # soak + real-time cron (~2 min)
make test-all       # everything
```

Bindings under `packages/` have their own test runners; see each binding's README for language-specific commands (`cargo test`, `bun test`, `mix test`, `bundle exec ruby spec/*.rb`, etc.).

## Releases

Crate releases are tag-triggered. Bump the version in the crate's `Cargo.toml`, refresh `Cargo.lock`, commit, then tag. The lock refresh is not optional: CI and the release workflow pass `--locked` everywhere, so a commit that bumps `Cargo.toml` without the matching `Cargo.lock` line fails with "the lock file needs to be updated but --locked was passed" — after the tag is already pushed.

```bash
# honker-core
# edit honker-core/Cargo.toml → version = "0.1.1"
cargo check --workspace   # refreshes Cargo.lock with the new version
git commit -am "honker-core v0.1.1: <summary>"
git tag core-v0.1.1
git push origin main core-v0.1.1

# honker-extension
git tag ext-v0.1.1
git push origin ext-v0.1.1
```

GitHub Actions (`.github/workflows/release-crates.yml`) picks up tags matching `core-v*` / `ext-v*` and builds, verifies, and smoke-tests the crate artifacts. It does not publish — `cargo publish` is a manual step after the proof job is green. The same holds for every other ecosystem's release workflow.

Each language binding lives in `packages/` in this repo and has its own release tag prefix; see each binding's README.

## Making changes

- PRs welcome; CI must be green before merge.
- Keep PRs focused: one feature or fix, not mixed refactors.
- No framework plugins (FastAPI/Django/Flask/etc) — see README for rationale.
