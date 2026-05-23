# frozen_string_literal: true

require "rails/railtie"

module Honker
  class Railtie < ::Rails::Railtie
    config.after_initialize do
      Honker.bootstrap(ActiveRecord::Base.connection)
    end
  end
end
