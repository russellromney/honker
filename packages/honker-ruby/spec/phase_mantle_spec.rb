# frozen_string_literal: true
#
# Phase Mantle: Scheduler#pause/resume/list/update + Queue#cancel/get_job.

require "tmpdir"
require "minitest/autorun"
require "honker"

REPO_ROOT = File.expand_path("../../..", __dir__) unless defined?(REPO_ROOT)

unless defined?(find_extension)
  def find_extension
    %w[
      target/debug/libhonker_ext.dylib
      target/debug/libhonker_ext.so
      target/debug/libhonker_extension.dylib
      target/debug/libhonker_extension.so
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

class PhaseMantleTest < Minitest::Test
  def setup
    ext = find_extension
    skip "honker extension not built — run `cargo build -p honker-extension --release`" unless ext
    @ext = ext
    @tmpdir = Dir.mktmpdir("honker-mantle-")
    @db_path = File.join(@tmpdir, "t.db")
  end

  def teardown
    FileUtils.remove_entry(@tmpdir) if @tmpdir && File.directory?(@tmpdir)
  end

  def open_db
    Honker::Database.new(@db_path, extension_path: @ext)
  end

  def test_schedule_list_round_trips_fields
    db = open_db
    sched = Honker::Scheduler.new(db)
    sched.add(name: "recap", queue: "emails", schedule: "0 9 * * 1",
              payload: { team: "premier-league" }, priority: 3)
    sched.add(name: "sync", queue: "syncs", schedule: "@every 1h",
              payload: nil)

    rows = sched.list
    assert_equal 2, rows.length
    recap = rows.find { |r| r["name"] == "recap" }
    refute_nil recap
    assert_equal "emails", recap["queue"]
    assert_equal 3, recap["priority"]
    assert_equal true, recap["enabled"]
    assert_equal({ "team" => "premier-league" }, JSON.parse(recap["payload"]))
  end

  def test_pause_resume_idempotent
    db = open_db
    sched = Honker::Scheduler.new(db)
    sched.add(name: "a", queue: "q", schedule: "0 9 * * *", payload: nil)

    assert_equal true, sched.pause("a")
    assert_equal false, sched.pause("a") # already paused
    assert_equal false, sched.pause("missing")

    paused = sched.list.find { |r| r["name"] == "a" }
    assert_equal false, paused["enabled"]

    assert_equal true, sched.resume("a")
    assert_equal false, sched.resume("a")

    enabled = sched.list.find { |r| r["name"] == "a" }
    assert_equal true, enabled["enabled"]
  end

  def test_update_mutates_and_noop
    db = open_db
    sched = Honker::Scheduler.new(db)
    sched.add(name: "t", queue: "q", schedule: "0 9 * * *",
              payload: { v: 1 }, priority: 0)

    assert_equal true, sched.update("t", payload: { v: 99 }, priority: 5)
    row = sched.list.find { |r| r["name"] == "t" }
    assert_equal 99, JSON.parse(row["payload"])["v"]
    assert_equal 5, row["priority"]

    before = row["next_fire_at"]
    assert_equal true, sched.update("t", schedule: "*/5 * * * *")
    row = sched.list.find { |r| r["name"] == "t" }
    assert_equal "*/5 * * * *", row["cron_expr"]
    refute_equal before, row["next_fire_at"]

    # No-op + missing
    assert_equal false, sched.update("t")
    assert_equal false, sched.update("missing", payload: {})
  end

  def test_queue_cancel_and_get_job
    db = open_db
    q = db.queue("emails")
    job_id = q.enqueue({ to: "alice@example.com" })

    row = q.get_job(job_id)
    refute_nil row
    assert_equal "emails", row["queue"]
    assert_equal "pending", row["state"]
    assert_equal job_id, row["id"]

    assert_equal true, q.cancel(job_id)
    assert_equal false, q.cancel(job_id) # idempotent
    assert_nil q.get_job(job_id)
    assert_nil q.claim_one("worker-1")
  end

  def test_cancel_processing_invalidates_ack
    db = open_db
    q = db.queue("emails")
    job_id = q.enqueue({ to: "x" })
    job = q.claim_one("worker-1")
    assert_equal job_id, job.id

    assert_equal true, q.cancel(job_id)
    assert_equal false, job.ack
  end

  def test_paused_schedule_does_not_emit_on_tick
    db = open_db
    sched = Honker::Scheduler.new(db)
    sched.add(name: "due", queue: "emails", schedule: "@every 1s",
              payload: { x: 1 })
    sleep 1.1
    assert_equal true, sched.pause("due")

    fires = sched.tick(Time.now.to_i + 5)
    assert_empty fires, "paused schedule must not emit; got #{fires.inspect}"

    assert_equal true, sched.resume("due")
    fires2 = sched.tick(Time.now.to_i + 5)
    refute_empty fires2, "resumed schedule should emit; got #{fires2.inspect}"
  end

  def test_get_job_misses_after_ack
    db = open_db
    q = db.queue("emails")
    job_id = q.enqueue({ to: "x" })
    job = q.claim_one("worker-1")
    assert_equal true, job.ack
    # Row gone after ack — get_job misses just like after cancel.
    assert_nil q.get_job(job_id)
  end

  def test_update_payload_null_vs_omitted
    db = open_db
    sched = Honker::Scheduler.new(db)
    sched.add(name: "t", queue: "q", schedule: "0 9 * * *", payload: { v: 1 })

    # Omitted payload — leaves it alone.
    assert_equal true, sched.update("t", priority: 7)
    row = sched.list[0]
    assert_equal({ "v" => 1 }, JSON.parse(row["payload"]))

    # payload: nil — explicitly write JSON null.
    assert_equal true, sched.update("t", payload: nil)
    row = sched.list[0]
    assert_nil JSON.parse(row["payload"])
  end
end
