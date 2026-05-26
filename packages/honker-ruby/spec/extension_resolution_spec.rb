# frozen_string_literal: true
#
# Unit tests for Honker::ExtensionResolver, the lookup that lets
# Honker::Database.new resolve the bundled SQLite extension without an
# explicit extension_path.
#
# Run: bundle exec ruby -Ilib spec/extension_resolution_spec.rb

require "tmpdir"
require "minitest/autorun"
require "honker"

class ExtensionResolverTest < Minitest::Test
  def test_explicit_path_is_returned_unchanged
    resolver = Honker::ExtensionResolver.new(env: nil)
    assert_equal(
      "/somewhere/libhonker_ext.so",
      resolver.resolve("/somewhere/libhonker_ext.so"),
    )
  end

  def test_explicit_path_beats_a_set_env_var
    resolver = Honker::ExtensionResolver.new(env: "/env/libhonker_ext.so")
    assert_equal(
      "/explicit/libhonker_ext.so",
      resolver.resolve("/explicit/libhonker_ext.so"),
    )
  end

  def test_env_path_is_used_when_it_exists
    Dir.mktmpdir do |dir|
      ext = File.join(dir, "libhonker_ext.so")
      File.write(ext, "")
      resolver = Honker::ExtensionResolver.new(env: ext)
      assert_equal ext, resolver.resolve
    end
  end

  def test_missing_env_path_raises
    resolver = Honker::ExtensionResolver.new(env: "/no/such/ext.so")
    error = assert_raises(Honker::Error) { resolver.resolve }
    assert_includes error.message, "HONKER_EXTENSION_PATH"
  end

  def test_empty_env_falls_through_to_the_bundled_extension
    Dir.mktmpdir do |dir|
      ext = File.join(dir, "libhonker_ext.so")
      File.write(ext, "")
      resolver = Honker::ExtensionResolver.new(env: "", bundled: ext)
      assert_equal ext, resolver.resolve
    end
  end

  def test_uses_the_bundled_extension_when_no_override_is_given
    Dir.mktmpdir do |dir|
      ext = File.join(dir, "libhonker_ext.so")
      File.write(ext, "")
      resolver = Honker::ExtensionResolver.new(env: nil, bundled: ext)
      assert_equal ext, resolver.resolve
    end
  end

  def test_missing_bundled_extension_raises
    resolver = Honker::ExtensionResolver.new(env: nil, bundled: "/no/such/ext.so")
    error = assert_raises(Honker::Error) { resolver.resolve }
    assert_includes error.message, "not found"
  end

  def test_database_consults_the_resolver_when_extension_path_omitted
    consulted = Class.new(StandardError)
    resolver = Object.new
    resolver.define_singleton_method(:resolve) { |_extension_path| raise consulted }

    Dir.mktmpdir do |dir|
      assert_raises(consulted) do
        Honker::Database.new(File.join(dir, "app.db"), extension_resolver: resolver)
      end
    end
  end
end
