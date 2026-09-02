# frozen_string_literal: true
#
# Full job details on claimed jobs and read-only snapshots (issue #136).
# Every one of the twelve fields the SQL ABI returns is asserted against
# the value it was enqueued or claimed with — not merely for presence.
#
# Run: bundle exec ruby -Ilib spec/job_fields_spec.rb

require "tmpdir"
require "json"
require "minitest/autorun"
require "honker"

REPO_ROOT = File.expand_path("../../..", __dir__) unless defined?(REPO_ROOT)

def find_ext
  %w[
    target/debug/libhonker_ext.dylib
    target/debug/libhonker_ext.so
    target/release/libhonker_ext.dylib
    target/release/libhonker_ext.so
  ].each do |rel|
    p = File.join(REPO_ROOT, rel)
    return p if File.exist?(p)
  end
  nil
end

# Same helper the other specs use: under
# HONKER_REQUIRE_RUBY_EXTENSION_LOADING=1 a missing capability is a
# failure, not a silent pass.
unless defined?(require_load_extension_support!)
  def require_load_extension_support!
    return if SQLite3::Database.new(":memory:").respond_to?(:enable_load_extension)

    message = "sqlite3 gem lacks loadable-extension support"
    flunk message if ENV["HONKER_REQUIRE_RUBY_EXTENSION_LOADING"] == "1"
    skip message
  end
end

# A missing build artifact must not let this acceptance spec pass while
# asserting nothing. Locally it still skips; under the CI flag it fails.
unless defined?(require_built_extension!)
  def require_built_extension!(ext)
    return if ext

    message = "honker extension not built"
    flunk message if ENV["HONKER_REQUIRE_RUBY_EXTENSION_LOADING"] == "1"
    skip message
  end
end

class HonkerJobFieldsTest < Minitest::Test
  # Distinct values so no assertion can pass by two numbers happening
  # to be equal: priority, max_attempts, visibility timeout, delay, and
  # the expiry window all differ from each other and from the defaults.
  PRIORITY = 7
  MAX_ATTEMPTS = 5
  VISIBILITY_S = 11
  DELAY_S = 300
  EXPIRES_S = 900

  def setup
    require_load_extension_support!
    ext = find_ext
    require_built_extension!(ext)
    @tmpdir = Dir.mktmpdir("honker-job-fields-")
    @db = Honker::Database.new(File.join(@tmpdir, "t.db"), extension_path: ext)
    @q = @db.queue("details",
                   visibility_timeout_s: VISIBILITY_S,
                   max_attempts: MAX_ATTEMPTS)
  end

  def teardown
    @db&.close
    FileUtils.remove_entry(@tmpdir) if @tmpdir && File.directory?(@tmpdir)
  end

  def db_now
    @db.db.get_first_row("SELECT unixepoch()")[0]
  end

  def test_pending_snapshot_carries_every_field
    payload = { "to" => "alice@example.com", "v" => 2 }
    before = db_now
    id = @q.enqueue(payload, priority: PRIORITY, expires: EXPIRES_S)
    after = db_now

    snap = @q.get_job(id)
    refute_nil snap
    assert_instance_of Honker::JobSnapshot, snap

    assert_equal id, snap.id
    assert_equal "details", snap.queue
    # Snapshot payloads are raw JSON text (see JobSnapshot docs).
    assert_equal JSON.dump(payload), snap.payload
    assert_equal payload, JSON.parse(snap.payload)
    assert_equal "pending", snap.state
    assert_equal PRIORITY, snap.priority
    assert_nil snap.worker_id, "an unclaimed job has no worker"
    assert_nil snap.claim_expires_at, "an unclaimed job has no claim deadline"
    assert_equal 0, snap.attempts
    assert_equal MAX_ATTEMPTS, snap.max_attempts
    assert_operator snap.created_at, :>=, before
    assert_operator snap.created_at, :<=, after
    # No delay: run_at is the enqueue instant, not a default of 0 and
    # not a future deadline.
    assert_operator snap.run_at, :>=, before
    assert_operator snap.run_at, :<=, after
    # expires_at is created_at + EXPIRES_S. enqueue() reads unixepoch()
    # once for the offset and the row defaults created_at from a second
    # read, so allow one second of skew — still far from any other
    # constant in this test.
    assert_in_delta snap.created_at + EXPIRES_S, snap.expires_at, 1
  end

  def test_snapshot_without_expiry_has_nil_expires_at
    id = @q.enqueue({ "x" => 1 })
    assert_nil @q.get_job(id).expires_at, "expires_at must be nil, not 0"
  end

  def test_delayed_job_reports_its_run_at
    id = @q.enqueue({ "x" => 1 }, delay: DELAY_S)
    snap = @q.get_job(id)

    assert_equal "pending", snap.state
    assert_in_delta snap.created_at + DELAY_S, snap.run_at, 1
    # And it is genuinely in the future, not a stale creation stamp.
    assert_operator snap.run_at, :>, db_now
    assert_nil @q.claim_one("worker-early"), "a delayed job is not claimable"
  end

  def test_claimed_job_carries_every_field
    payload = { "to" => "bob@example.com", "v" => 2 }
    id = @q.enqueue(payload, priority: PRIORITY, expires: EXPIRES_S)
    pending = @q.get_job(id)

    before = db_now
    job = @q.claim_one("worker-a")
    after = db_now
    refute_nil job

    assert_equal id, job.id
    assert_equal "details", job.queue_name
    # Claimed payloads are decoded, unlike snapshot payloads.
    assert_equal payload, job.payload
    assert_equal "processing", job.state
    assert_equal PRIORITY, job.priority
    assert_equal pending.run_at, job.run_at
    assert_equal "worker-a", job.worker_id
    assert_equal 1, job.attempts, "the claim increments attempts"
    assert_equal MAX_ATTEMPTS, job.max_attempts
    assert_equal pending.created_at, job.created_at
    assert_equal pending.expires_at, job.expires_at
    # The claim deadline is the claim instant plus this queue's
    # visibility timeout — not the default 300s.
    assert_operator job.claim_expires_at, :>=, before + VISIBILITY_S
    assert_operator job.claim_expires_at, :<=, after + VISIBILITY_S
  end

  # test_claimed_job_carries_every_field compares run_at against a
  # snapshot of an *undelayed* enqueue, where run_at == created_at to the
  # second, so it cannot tell the two fields apart. A delay will not fix
  # that: a delayed job is not claimable (see the test above). Back-dating
  # an absolute run_at keeps the job claimable and makes the two differ.
  def test_claimed_job_reports_its_own_run_at_not_created_at
    run_at = db_now - 100
    id = @q.enqueue({ "x" => 1 }, run_at: run_at)

    snap = @q.get_job(id)
    assert_equal run_at, snap.run_at
    refute_equal snap.created_at, snap.run_at

    job = @q.claim_one("worker-a")
    refute_nil job, "a back-dated job must still be claimable"
    assert_equal id, job.id
    assert_equal run_at, job.run_at
    refute_equal job.created_at, job.run_at
    assert_equal snap.created_at, job.created_at
  end

  def test_processing_snapshot_matches_the_claim_then_misses_after_ack
    id = @q.enqueue({ "to" => "carol@example.com" }, priority: PRIORITY)
    job = @q.claim_one("worker-b")

    # A second reader sees the in-flight claim's details.
    snap = @q.get_job(id)
    assert_equal "processing", snap.state
    assert_equal "worker-b", snap.worker_id
    assert_equal job.claim_expires_at, snap.claim_expires_at
    assert_operator snap.claim_expires_at, :>, snap.created_at
    assert_equal 1, snap.attempts
    assert_equal PRIORITY, snap.priority
    assert_equal MAX_ATTEMPTS, snap.max_attempts

    assert job.ack, "fresh claim should ack"
    assert_nil @q.get_job(id), "the row is gone after ack"
  end

  def test_retry_bumps_attempts_and_pushes_run_at_out
    id = @q.enqueue({ "x" => 1 })
    job = @q.claim_one("worker-c")
    assert_equal 1, job.attempts

    assert job.retry(delay_s: DELAY_S, error: "boom")
    snap = @q.get_job(id)
    assert_equal "pending", snap.state
    assert_equal 1, snap.attempts, "attempts survives the retry"
    assert_operator snap.run_at, :>=, db_now + DELAY_S - 1
    assert_nil snap.worker_id, "retry clears the worker"
    assert_nil snap.claim_expires_at, "retry clears the claim deadline"
  end
end
