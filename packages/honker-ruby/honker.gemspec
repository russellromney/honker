# frozen_string_literal: true

require_relative "lib/honker/version"

Gem::Specification.new do |spec|
  spec.name          = "honker"
  spec.version       = Honker::VERSION
  spec.authors       = ["Russell Romney"]

  spec.summary       = "Durable queues, streams, pub/sub, and scheduler on SQLite."
  spec.description   = <<~DESC.strip
    Ruby binding for Honker — a SQLite-native task runtime. Queues,
    streams, pub/sub, time-trigger scheduler, results, locks, rate limits, all
    in one .db file. Thin wrapper around the Honker SQLite loadable
    extension; no Redis, no external broker.
  DESC
  spec.homepage      = "https://honker.dev"
  spec.licenses      = ["MIT", "Apache-2.0"]
  spec.required_ruby_version = ">= 3.0.0"

  spec.metadata["homepage_uri"]    = spec.homepage
  spec.metadata["source_code_uri"] = "https://github.com/russellromney/honker"
  spec.metadata["documentation_uri"] = "https://honker.dev/"
  spec.metadata["rubygems_mfa_required"] = "true"

  # HONKER_GEM_PLATFORM is set by the release workflow when building a
  # precompiled platform gem: it bundles the prebuilt extension next to
  # the Ruby source in lib/honker/. Without it `gem build` produces the
  # generic gem, which ships the Rust crate source and compiles the
  # extension on install (see ext/honker/extconf.rb).
  gem_platform = ENV.fetch("HONKER_GEM_PLATFORM", nil)
  generic_gem = gem_platform.nil? || gem_platform.empty?
  spec.platform = gem_platform unless generic_gem

  extension_files = Dir.glob("lib/honker/{libhonker_ext.*,honker_ext.dll}")
  ruby_files = Dir.glob("lib/**/*") - extension_files
  base_files = ruby_files +
    %w[honker.gemspec README.md LICENSE LICENSE-MIT LICENSE-APACHE]

  if generic_gem
    spec.extensions = ["ext/honker/extconf.rb"]
    spec.files = base_files + Dir.glob("ext/**/*").select { |f| File.file?(f) }
  else
    spec.files = base_files + extension_files
  end
  spec.require_paths = ["lib"]

  # CoreWatcher calls into the extension over Fiddle. Declared
  # explicitly because fiddle stopped being a default gem in Ruby 3.5.
  spec.add_dependency "fiddle", "~> 1.0"
  # Honker loads the SQLite extension directly, so the Ruby sqlite3
  # binding must expose Database#enable_load_extension/#load_extension.
  # sqlite3 2.0.4 is the first line compatible with our Ruby >= 3.0 floor
  # that reliably ships those APIs in the supported native builds.
  spec.add_dependency "sqlite3", ">= 2.0.4", "< 3"
end
