# frozen_string_literal: true
#
# Basic example: enqueue a few jobs, claim them, ack each.
#
# From a checkout, build the extension and point Honker at it:
#   cargo build --release -p honker-extension
#   HONKER_EXTENSION_PATH=target/release/libhonker_ext.so ruby examples/basic.rb

$LOAD_PATH.unshift(File.expand_path("../lib", __dir__))
require "honker"

db = Honker::Database.new("demo.db")
q  = db.queue("emails")

3.times do |i|
  q.enqueue({ to: "user-#{i}@example.com" })
end

loop do
  job = q.claim_one("worker-1")
  break if job.nil?

  puts "processing job #{job.id}: to=#{job.payload['to']}"
  job.ack
end

db.close
