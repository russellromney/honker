# ActiveRecord, as guides/orm/ruby.mdx shows it.
# docs:start example

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

catalog_path = ENV["HONKER_ORM_SURFACE"] || File.expand_path("../surface.json", __dir__)
catalog = JSON.parse(File.read(catalog_path))

def as_int(value)
  Integer(value)
end

def as_text(value)
  value.nil? ? "" : value.to_s
end

def resolve(token, prefix, vars)
  return token unless token.is_a?(String)
  return "#{prefix}_#{token[4..]}" if token.start_with?("$ns:")
  if token.start_with?("$json:")
    ids = token[6..].split(",").map { |k| as_int(vars.fetch(k)) }
    return JSON.generate(ids)
  end
  return vars.fetch(token[1..]) if token.start_with?("$")
  token
end

def resolve_text(text, prefix, vars)
  out = text.dup
  vars.each { |k, v| out = out.gsub("$#{k}", as_text(v)) }
  out.gsub("$ns:", "#{prefix}_")
end

def check(expect, result, prefix, vars)
  case expect["kind"]
  when "int_gt" then raise "got #{result.inspect}" unless as_int(result) > expect["n"]
  when "int_eq" then raise "got #{result.inspect}" unless as_int(result) == expect["n"]
  when "int_ge" then raise "got #{result.inspect}" unless as_int(result) >= expect["n"]
  when "int_gt_ref" then raise "got #{result.inspect}" unless as_int(result) > as_int(vars.fetch(expect["ref"]))
  when "eq_ref" then raise "got #{result.inspect}" unless as_int(result) == as_int(vars.fetch(expect["ref"]))
  when "json_len"
    parsed = JSON.parse(as_text(result))
    raise "got #{result.inspect}" unless parsed.length == expect["n"]
  when "json_id_eq_ref"
    parsed = JSON.parse(as_text(result))
    raise "got #{result.inspect}" unless parsed.length == 1 && as_int(parsed[0]["id"]) == as_int(vars.fetch(expect["ref"]))
  when "contains"
    needle = resolve_text(expect["s"], prefix, vars)
    raise "#{needle.inspect} not in #{result.inspect}" unless as_text(result).include?(needle)
  when "empty_string"
    raise "expected empty string, got #{result.inspect}" unless as_text(result) == ""
  when "is_null"
    raise "expected NULL, got #{result.inspect}" unless result.nil?
  else
    raise "unknown expect kind #{expect["kind"]}"
  end
end

vars = {}
catalog.fetch("steps").each do |step|
  args = step.fetch("args").map { |a| resolve(a, "rb", vars) }
  begin
    result = conn.exec_query(step.fetch("sql"), step.fetch("id"), args).rows.first&.first
  rescue StandardError => e
    raise "#{step["id"]} failed: #{e}"
  end
  vars[step["store"]] = result if step["store"]
  if step["expect"]
    begin
      check(step["expect"], result, "rb", vars)
    rescue StandardError => e
      raise "#{step["id"]}: #{e}"
    end
  end
end

conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER)")

job_id = nil
ActiveRecord::Base.transaction do
  conn.exec_query("INSERT INTO orders (id, user_id) VALUES (?, ?)", "ins", [42, 1])
  job_id = conn.exec_query(
    "SELECT honker_enqueue(?, ?, NULL, NULL, ?, ?, NULL)",
    "enq",
    ["rb_atomic", JSON.generate({ "order_id" => 42 }), 0, 3]
  ).rows.first.first
end
raise "missing committed order" unless conn.select_value("SELECT COUNT(*) FROM orders WHERE id = 42") == 1
job = conn.exec_query("SELECT honker_get_job(?)", "get", [job_id]).rows.first.first
raise "missing committed job" unless job.to_s.include?("order_id")

rolled = nil
ActiveRecord::Base.transaction do
  conn.exec_query("INSERT INTO orders (id, user_id) VALUES (?, ?)", "ins", [43, 1])
  rolled = conn.exec_query(
    "SELECT honker_enqueue(?, ?, NULL, NULL, ?, ?, NULL)",
    "enq",
    ["rb_atomic", JSON.generate({ "order_id" => 43 }), 0, 3]
  ).rows.first.first
  raise ActiveRecord::Rollback
end
raise "rollback left an order" unless conn.select_value("SELECT COUNT(*) FROM orders WHERE id = 43") == 0
job = conn.exec_query("SELECT honker_get_job(?)", "get", [rolled]).rows.first.first
raise "rollback left a job" unless job.to_s == ""

puts "PASS ruby-activerecord"
# docs:end example
