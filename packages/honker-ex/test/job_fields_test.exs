defmodule HonkerJobFieldsTest do
  @moduledoc """
  Full job details on claimed jobs and read-only snapshots (issue #136).
  Every one of the twelve fields the SQL ABI returns is asserted against
  the value it was enqueued or claimed with — not merely for presence.
  """
  use ExUnit.Case, async: false

  alias Honker.{Job, JobSnapshot, Queue}

  # Distinct values so no assertion can pass by two numbers happening to
  # be equal: priority, max_attempts, visibility timeout, delay, and the
  # expiry window all differ from each other and from the defaults.
  @priority 7
  @max_attempts 5
  @visibility_s 11
  @delay_s 300
  @expires_s 900

  @candidates [
    "target/debug/libhonker_ext.dylib",
    "target/debug/libhonker_ext.so",
    "target/release/libhonker_ext.dylib",
    "target/release/libhonker_ext.so"
  ]

  @repo_root Path.expand("../../..", __DIR__)

  @extension_path Enum.find_value(@candidates, fn rel ->
                    p = Path.join(@repo_root, rel)
                    if File.exists?(p), do: p, else: nil
                  end)

  # A real ExUnit skip. `setup` used to return `{:skip, reason}`, which
  # ExUnit rejects outright — "expected ExUnit setup callback in
  # HonkerJobFieldsTest to return the atom :ok, a keyword, or a map" — so
  # the intended graceful skip was dead code and a missing extension
  # produced six confusing failures instead of one clear message.
  # `skip: false` is not a skip, so this only fires when the extension is
  # genuinely absent; in CI the build step runs first, so it never does.
  @moduletag skip: @extension_path == nil && "honker extension not built"

  setup do
    dir = Path.join(System.tmp_dir!(), "honker-job-fields-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    {:ok, db} = Honker.open(Path.join(dir, "t.db"), extension_path: @extension_path)

    db =
      Honker.configure_queue(db, "details",
        visibility_timeout_s: @visibility_s,
        max_attempts: @max_attempts
      )

    on_exit(fn ->
      Honker.close(db)
      File.rm_rf!(dir)
    end)

    {:ok, %{db: db}}
  end

  defp db_now(db) do
    {:ok, [now]} = Honker.query_first(db.conn, "SELECT unixepoch()", [])
    now
  end

  test "a pending snapshot carries every field", %{db: db} do
    payload = %{"to" => "alice@example.com", "v" => 2}
    before = db_now(db)
    {:ok, id} = Queue.enqueue(db, "details", payload, priority: @priority, expires: @expires_s)
    after_ = db_now(db)

    {:ok, snap} = Queue.get_job(db, id)
    assert %JobSnapshot{} = snap

    assert snap.id == id
    assert snap.queue == "details"
    # Snapshot payloads are raw JSON text (see Honker.JobSnapshot docs).
    assert snap.payload == Jason.encode!(payload)
    assert Jason.decode!(snap.payload) == payload
    assert snap.state == "pending"
    assert snap.priority == @priority
    assert snap.worker_id == nil, "an unclaimed job has no worker"
    assert snap.claim_expires_at == nil, "an unclaimed job has no claim deadline"
    assert snap.attempts == 0
    assert snap.max_attempts == @max_attempts
    assert snap.created_at >= before and snap.created_at <= after_
    # No delay: run_at is the enqueue instant, not a default of 0 and
    # not a future deadline.
    assert snap.run_at >= before and snap.run_at <= after_
    # expires_at is created_at + @expires_s. enqueue() reads unixepoch()
    # once for the offset and the row defaults created_at from a second
    # read, so allow one second of skew — still far from any other
    # constant in this test.
    assert abs(snap.expires_at - (snap.created_at + @expires_s)) <= 1
  end

  test "a job enqueued without :expires has a nil expires_at", %{db: db} do
    {:ok, id} = Queue.enqueue(db, "details", %{"x" => 1})
    {:ok, snap} = Queue.get_job(db, id)
    assert snap.expires_at == nil, "expires_at must be nil, not 0"
  end

  test "a delayed job reports its run_at", %{db: db} do
    {:ok, id} = Queue.enqueue(db, "details", %{"x" => 1}, delay: @delay_s)
    {:ok, snap} = Queue.get_job(db, id)

    assert snap.state == "pending"
    assert abs(snap.run_at - (snap.created_at + @delay_s)) <= 1
    # And it is genuinely in the future, not a stale creation stamp.
    assert snap.run_at > db_now(db)
    assert {:ok, nil} = Queue.claim_one(db, "details", "worker-early")
  end

  test "a claimed job carries every field", %{db: db} do
    payload = %{"to" => "bob@example.com", "v" => 2}
    {:ok, id} = Queue.enqueue(db, "details", payload, priority: @priority, expires: @expires_s)
    {:ok, pending} = Queue.get_job(db, id)

    before = db_now(db)
    {:ok, job} = Queue.claim_one(db, "details", "worker-a")
    after_ = db_now(db)
    assert %Job{} = job

    assert job.id == id
    assert job.queue == "details"
    # Claimed payloads are decoded, unlike snapshot payloads.
    assert job.payload == payload
    assert job.state == "processing"
    assert job.priority == @priority
    assert job.run_at == pending.run_at
    assert job.worker_id == "worker-a"
    assert job.attempts == 1, "the claim increments attempts"
    assert job.max_attempts == @max_attempts
    assert job.created_at == pending.created_at
    assert job.expires_at == pending.expires_at
    # The claim deadline is the claim instant plus this queue's
    # visibility timeout — not the default 300s.
    assert job.claim_expires_at >= before + @visibility_s
    assert job.claim_expires_at <= after_ + @visibility_s
  end

  # "a claimed job carries every field" compares run_at against a snapshot
  # of an *undelayed* enqueue, where run_at == created_at to the second, so
  # it cannot tell the two fields apart. A delay will not fix that: a
  # delayed job is not claimable (see the test above). Back-dating an
  # absolute run_at keeps the job claimable and makes the two differ.
  test "a claimed job reports its own run_at, not its created_at", %{db: db} do
    run_at = db_now(db) - 100
    {:ok, id} = Queue.enqueue(db, "details", %{"x" => 1}, run_at: run_at)

    {:ok, snap} = Queue.get_job(db, id)
    assert snap.run_at == run_at
    refute snap.run_at == snap.created_at

    {:ok, job} = Queue.claim_one(db, "details", "worker-a")
    assert job != nil, "a back-dated job must still be claimable"
    assert job.id == id
    assert job.run_at == run_at
    refute job.run_at == job.created_at
    assert job.created_at == snap.created_at
  end

  test "a processing snapshot matches the claim, then misses after ack", %{db: db} do
    {:ok, id} = Queue.enqueue(db, "details", %{"to" => "carol@example.com"}, priority: @priority)
    {:ok, job} = Queue.claim_one(db, "details", "worker-b")

    # A second reader sees the in-flight claim's details.
    {:ok, snap} = Queue.get_job(db, id)
    assert snap.state == "processing"
    assert snap.worker_id == "worker-b"
    assert snap.claim_expires_at == job.claim_expires_at
    assert snap.claim_expires_at > snap.created_at
    assert snap.attempts == 1
    assert snap.priority == @priority
    assert snap.max_attempts == @max_attempts

    assert {:ok, true} = Job.ack(db, job)
    assert {:ok, nil} = Queue.get_job(db, id)
  end

  test "retry keeps attempts and pushes run_at out", %{db: db} do
    {:ok, id} = Queue.enqueue(db, "details", %{"x" => 1})
    {:ok, job} = Queue.claim_one(db, "details", "worker-c")
    assert job.attempts == 1

    assert {:ok, true} = Job.retry(db, job, @delay_s, "boom")
    {:ok, snap} = Queue.get_job(db, id)
    assert snap.state == "pending"
    assert snap.attempts == 1, "attempts survives the retry"
    assert snap.run_at >= db_now(db) + @delay_s - 1
    assert snap.worker_id == nil, "retry clears the worker"
    assert snap.claim_expires_at == nil, "retry clears the claim deadline"
  end
end
