#!/usr/bin/env ruby
# frozen_string_literal: true
#
# Release proof: install-and-run check for the honker gem. Assumes the
# gem is already installed; exercises a queue round-trip through the
# bundled SQLite extension (no HONKER_EXTENSION_PATH, no extension_path:
# argument), so it fails if the gem did not bundle a working extension.

require "tmpdir"
require "honker"

Dir.mktmpdir("honker-ruby-proof-") do |dir|
  db = Honker::Database.new(File.join(dir, "app.db"))
  q = db.queue("emails")
  id = q.enqueue({ to: "alice@example.com" })
  job = q.claim_one("worker-1")
  raise "claim failed" unless job && job.id == id
  raise "payload mismatch" unless job.payload["to"] == "alice@example.com"
  raise "ack failed" unless job.ack
  db.close
end

puts "ruby gem smoke ok"
