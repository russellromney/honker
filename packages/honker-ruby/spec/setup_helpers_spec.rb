# frozen_string_literal: true
#
# Unit tests for the Honker module-level setup helpers
# (Honker.extension_path, .load_extension, .bootstrap, .setup,
# .sequel_after_connect) that collapse the extension-load + bootstrap
# ceremony every Ruby ORM integration was copy-pasting.
#
# Run: bundle exec ruby -Ilib spec/setup_helpers_spec.rb

require "tmpdir"
require "minitest/autorun"
require "honker"

REPO_ROOT = File.expand_path("../../..", __dir__) unless defined?(REPO_ROOT)

unless defined?(find_extension)
  def find_extension
    %w[
      target/release/libhonker_ext.dylib
      target/release/libhonker_ext.so
      target/release/libhonker_extension.dylib
      target/release/libhonker_extension.so
    ].each do |rel|
      p = File.join(REPO_ROOT, rel)
      return p if File.exist?(p)
    end
    nil
  end
end

unless defined?(require_load_extension_support!)
  def require_load_extension_support!
    return if SQLite3::Database.new(":memory:").respond_to?(:enable_load_extension)

    message = "sqlite3 gem lacks loadable-extension support"
    if ENV["HONKER_REQUIRE_RUBY_EXTENSION_LOADING"] == "1"
      flunk message
    end
    skip message
  end
end

class HonkerSetupHelpersTest < Minitest::Test
  # Records every method call so tests can assert ordering and arguments
  # without needing a real SQLite handle.
  class FakeConn
    attr_reader :calls

    def initialize
      @calls = []
    end

    def enable_load_extension(flag)
      @calls << [:enable_load_extension, flag]
    end

    def load_extension(path)
      @calls << [:load_extension, path]
    end

    def execute(sql)
      @calls << [:execute, sql]
    end
  end

  # Resolver double that records every override it sees and returns a
  # canned path (or a path derived from the override, for forward-through
  # tests).
  def fake_resolver(&block)
    block ||= ->(_override) { "/resolved/lib.so" }
    received = []
    resolver = Object.new
    resolver.define_singleton_method(:resolve) do |override = nil|
      received << override
      block.call(override)
    end
    [resolver, received]
  end

  def test_extension_path_with_no_args_delegates_to_resolver_with_nil
    resolver, received = fake_resolver
    Honker::ExtensionResolver.stub :new, resolver do
      assert_equal "/resolved/lib.so", Honker.extension_path
    end
    assert_equal [nil], received
  end

  def test_extension_path_forwards_explicit_override_to_resolver
    resolver, received = fake_resolver { |override| override }
    Honker::ExtensionResolver.stub :new, resolver do
      assert_equal "/explicit", Honker.extension_path("/explicit")
    end
    assert_equal ["/explicit"], received
  end

  def test_load_extension_toggles_enable_around_load
    conn = FakeConn.new
    Honker.load_extension(conn, extension_path: "/somewhere/ext.so")
    assert_equal(
      [
        [:enable_load_extension, true],
        [:load_extension, "/somewhere/ext.so"],
        [:enable_load_extension, false],
      ],
      conn.calls,
    )
  end

  def test_load_extension_passes_override_through_resolver
    resolver, received = fake_resolver { |override| "resolved:#{override}" }
    conn = FakeConn.new
    Honker::ExtensionResolver.stub :new, resolver do
      Honker.load_extension(conn, extension_path: "/x")
    end
    assert_equal ["/x"], received
    assert_equal [:load_extension, "resolved:/x"], conn.calls[1]
  end

  def test_bootstrap_runs_honker_bootstrap_sql
    conn = FakeConn.new
    Honker.bootstrap(conn)
    assert_equal [[:execute, "SELECT honker_bootstrap()"]], conn.calls
  end

  def test_setup_calls_load_extension_then_bootstrap_in_order
    conn = FakeConn.new
    resolver, = fake_resolver { |_| "/ext.so" }
    Honker::ExtensionResolver.stub :new, resolver do
      Honker.setup(conn)
    end
    assert_equal(
      [
        [:enable_load_extension, true],
        [:load_extension, "/ext.so"],
        [:enable_load_extension, false],
        [:execute, "SELECT honker_bootstrap()"],
      ],
      conn.calls,
    )
  end

  def test_setup_with_bootstrap_false_skips_bootstrap
    conn = FakeConn.new
    resolver, = fake_resolver { |_| "/ext.so" }
    Honker::ExtensionResolver.stub :new, resolver do
      Honker.setup(conn, bootstrap: false)
    end
    refute_includes conn.calls.map(&:first), :execute
  end

  def test_sequel_after_connect_returns_a_proc_that_calls_setup
    conn = FakeConn.new
    resolver, = fake_resolver { |_| "/ext.so" }
    after_connect = Honker::ExtensionResolver.stub :new, resolver do
      proc_ = Honker.sequel_after_connect
      proc_.call(conn)
      proc_
    end
    assert_kind_of Proc, after_connect
    assert_equal :load_extension, conn.calls[1].first
    assert_equal [:execute, "SELECT honker_bootstrap()"], conn.calls.last
  end

  def test_sequel_after_connect_forwards_bootstrap_false
    conn = FakeConn.new
    resolver, = fake_resolver { |_| "/ext.so" }
    Honker::ExtensionResolver.stub :new, resolver do
      Honker.sequel_after_connect(bootstrap: false).call(conn)
    end
    refute_includes conn.calls.map(&:first), :execute
  end

  def test_setup_on_real_sqlite_bootstraps_the_schema
    ext = find_extension
    skip "honker extension not built — run `cargo build -p honker-extension --release`" unless ext
    require_load_extension_support!

    Dir.mktmpdir do |dir|
      conn = SQLite3::Database.new(File.join(dir, "real.db"))
      Honker.setup(conn, extension_path: ext)
      rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = '_honker_live'",
      )
      refute_empty rows, "Honker.setup should bootstrap the _honker_live table"
    ensure
      conn&.close
    end
  end
end
