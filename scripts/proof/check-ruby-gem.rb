#!/usr/bin/env ruby
# frozen_string_literal: true
#
# Release proof for honker gems.
#
#   check-ruby-gem.rb GEM [GEM ...]             # platform gem checks
#   check-ruby-gem.rb --generic GEM [GEM ...]   # generic gem checks
#
# A platform gem must be platform-specific and bundle the SQLite
# loadable extension. The generic gem must instead ship the Rust crate
# source and declare an extension that compiles it on install.

require "rubygems/package"

def bundles_extension?(package)
  package.contents.any? do |name|
    name.match?(%r{\Alib/honker/(?:lib)?honker_ext\.(?:so|dylib|dll)\z})
  end
end

def check_platform_gem(path)
  package = Gem::Package.new(path)
  if package.spec.platform == Gem::Platform::RUBY
    abort "ruby gem is not platform-specific: #{path}"
  end
  abort "ruby gem is missing bundled SQLite extension: #{path}" unless bundles_extension?(package)
end

def check_generic_gem(path)
  package = Gem::Package.new(path)
  unless package.spec.platform == Gem::Platform::RUBY
    abort "generic ruby gem must not be platform-specific: #{path}"
  end
  abort "generic ruby gem must not bundle an extension: #{path}" if bundles_extension?(package)

  if package.spec.extensions.empty?
    abort "generic ruby gem must declare an extension to compile on install: #{path}"
  end

  contents = package.contents
  unless contents.include?("ext/honker/extconf.rb")
    abort "generic ruby gem is missing ext/honker/extconf.rb: #{path}"
  end
  unless contents.any? { |name| name.start_with?("ext/honker/honker-extension/") }
    abort "generic ruby gem is missing the vendored Rust crate source: #{path}"
  end
end

def main
  args = ARGV.dup
  generic = args.delete("--generic")
  abort "usage: check-ruby-gem.rb [--generic] GEM [GEM ...]" if args.empty?

  args.each do |path|
    generic ? check_generic_gem(path) : check_platform_gem(path)
  end
end

main if $PROGRAM_NAME == __FILE__
