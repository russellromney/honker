# ActiveRecord, as guides/orm/ruby.mdx shows it: the sqlite3 adapter's
# `extensions:` key fed from Honker.extension_path, then
# honker_bootstrap once after connect.
#
# The doc shows this as database.yml; establish_connection takes the
# same hash, so this is the same code path without a Rails app.

require "json"
require "active_record"
require "honker"

db = ENV.fetch("HONKER_TEST_DB")

ActiveRecord::Base.establish_connection(
  adapter: "sqlite3",
  database: db,
  timeout: 5000,
  extensions: [Honker.extension_path]
)

conn = ActiveRecord::Base.connection
conn.execute("SELECT honker_bootstrap()")

# Bound parameters throughout, so the adapter's own binding is what is
# exercised rather than values interpolated into SQL text.
payload = JSON.generate({ "to" => "alice@example.com" })

id = conn.exec_query(
  "SELECT honker_enqueue(?, ?, NULL, NULL, ?, ?, NULL) AS id",
  "enqueue",
  [ "emails", payload, 0, 3 ]
).rows.first.first
raise "expected a job id, got #{id.inspect}" unless id.is_a?(Integer) && id > 0

claimed_json = conn.exec_query(
  "SELECT honker_claim_batch(?, ?, ?, ?) AS jobs",
  "claim",
  [ "emails", "w1", 8, 300 ]
).rows.first.first
claimed = JSON.parse(claimed_json)
raise "expected one claimed job, got #{claimed_json}" unless claimed.length == 1
raise "claimed the wrong job: #{claimed_json}" unless claimed.first["id"] == id

acked = conn.exec_query(
  "SELECT honker_ack(?, ?) AS ok", "ack", [ id, "w1" ]
).rows.first.first
raise "ack must match the claim, got #{acked.inspect}" unless acked == 1

puts "PASS ruby-activerecord"
