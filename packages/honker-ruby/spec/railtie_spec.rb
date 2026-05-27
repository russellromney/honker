# frozen_string_literal: true
#
# Tests for Honker::Railtie. Skipped entirely when railties is not
# installed in the dev/test gemset — Rails is not a runtime dependency
# of honker.
#
# Run: bundle exec ruby -Ilib spec/railtie_spec.rb

require "tmpdir"
require "minitest/autorun"
require "minitest/mock"

begin
  require "rails/railtie"
rescue LoadError
  warn "skipping spec/railtie_spec.rb: railties not installed"
  return
end

require "honker"

class HonkerRailtieTest < Minitest::Test
  HONKER_LIB = File.expand_path("../lib", __dir__)

  def test_requiring_honker_with_rails_loaded_defines_the_railtie
    assert defined?(Honker::Railtie), "Honker::Railtie should be loaded when Rails is defined"
    assert_operator Honker::Railtie, :<, ::Rails::Railtie
  end

  def test_requiring_honker_without_rails_does_not_define_the_railtie
    script = <<~RUBY
      $LOAD_PATH.unshift(#{HONKER_LIB.inspect})
      require "honker"
      puts(defined?(Honker::Railtie) ? "defined" : "undefined")
    RUBY
    out = IO.popen([RbConfig.ruby, "-e", script], &:read)
    assert_equal "undefined", out.strip
  end

  def test_after_initialize_hook_bootstraps_active_record_primary_connection
    fake_connection = Object.new
    stub_ar_base = Module.new
    stub_ar_base.define_singleton_method(:connection) { fake_connection }
    stub_ar = Module.new
    stub_ar.const_set(:Base, stub_ar_base)
    Object.const_set(:ActiveRecord, stub_ar) unless defined?(::ActiveRecord)

    received = []
    begin
      Honker.stub :bootstrap, ->(conn) { received << conn } do
        ActiveSupport.run_load_hooks(:after_initialize, Object.new)
      end
    ensure
      Object.send(:remove_const, :ActiveRecord) if ::ActiveRecord.equal?(stub_ar)
    end

    assert_equal [fake_connection], received
  end
end
