# frozen_string_literal: true
#
# Run: bundle exec rspec spec/

require "tmpdir"
require "minitest/autorun"
require "rbconfig"
require "honker"

REPO_ROOT = File.expand_path("../../..", __dir__)

def find_extension
  candidates = %w[
    target/debug/libhonker_ext.dylib
    target/debug/libhonker_ext.so
    target/debug/libhonker_extension.dylib
    target/debug/libhonker_extension.so
    target/release/libhonker_ext.dylib
    target/release/libhonker_ext.so
    target/release/libhonker_extension.dylib
    target/release/libhonker_extension.so
  ]
  candidates.each do |rel|
    p = File.join(REPO_ROOT, rel)
    return p if File.exist?(p)
  end
  nil
end

def require_load_extension_support!
  return if SQLite3::Database.new(":memory:").respond_to?(:enable_load_extension)

  message = "sqlite3 gem lacks loadable-extension support"
  if ENV["HONKER_REQUIRE_RUBY_EXTENSION_LOADING"] == "1"
    flunk message
  end
  skip message
end

def queue_worker_helper(path, ext, backend, worker_id, ready_path, result_path)
  db = Honker::Database.new(
    path,
    extension_path: ext,
    watcher_backend: backend.empty? ? nil : backend,
  )
  q = db.queue("shared")
  File.write(ready_path, "ready")
  processed = []

  loop do
    job = q.claim_one(worker_id)
    if job
      payload = job.payload
      raise "ack failed for job #{job.id}" unless job.ack
      break if payload["stop"]

      processed << payload["i"]
      next
    end

    raise "watcher timed out before stop job" unless db.wait_for_update(10)
  end

  db.close
  File.write(result_path, processed.join("\n"))
end

def queue_writer_helper(path, ext, first, count)
  db = Honker::Database.new(path, extension_path: ext)
  q = db.queue("shared")
  first.upto(first + count - 1) { |i| q.enqueue({ "i" => i }) }
  db.close
end

if ARGV.first == "--honker-queue-worker"
  _, path, ext, backend, worker_id, ready_path, result_path = ARGV
  queue_worker_helper(path, ext, backend, worker_id, ready_path, result_path)
  exit! 0
elsif ARGV.first == "--honker-queue-writer"
  _, path, ext, first, count = ARGV
  queue_writer_helper(path, ext, Integer(first), Integer(count))
  exit! 0
end

class HonkerWatcherBackendOptionTest < Minitest::Test
  def require_extension_loading!
    require_load_extension_support!
  end

  def test_watcher_backends_detect_commits
    require_extension_loading!
    ext = find_extension
    skip "honker extension not built — run `cargo build -p honker-extension --release`" unless ext

    [nil, "", "poll", "polling", "kernel", "kernel-watcher", "shm", "shm-fast-path"].each do |backend|
      Dir.mktmpdir("honker-ruby-watchers-") do |dir|
        path = File.join(dir, "t.db")
        begin
          db = Honker::Database.new(path, extension_path: ext, watcher_backend: backend)
        rescue ArgumentError => e
          if e.message.include?("requires the") ||
             e.message.include?("-shm unavailable") ||
             e.message.include?("unsupported SQLite layout")
            next
          end
          raise
        end
        writer = Honker::Database.new(path, extension_path: ext)
        writer.notify("backend", { backend: backend })
        assert db.wait_for_update(2), "backend #{backend.inspect} did not observe commit"
        writer.close
        db.close
      end
    end
  end

  def test_custom_watcher_poll_interval_detects_commits
    require_extension_loading!
    ext = find_extension
    skip "honker extension not built — run `cargo build -p honker-extension --release`" unless ext

    Dir.mktmpdir("honker-ruby-watch-interval-") do |dir|
      path = File.join(dir, "t.db")
      db = Honker::Database.new(path, extension_path: ext, watcher_poll_interval_ms: 25)
      writer = Honker::Database.new(path, extension_path: ext)
      writer.notify("interval", { ok: true })
      assert db.wait_for_update(2), "custom watcher poll interval did not observe commit"
      writer.close
      db.close
    end
  end

  def test_accepts_polling_watcher_backend_aliases
    [nil, "", "poll", "polling"].each do |backend|
      err = assert_raises(Exception) do
        Honker::Database.new(
          File.join(Dir.tmpdir, "honker-ruby-missing.db"),
          extension_path: "/missing/libhonker_ext.so",
          watcher_backend: backend,
        )
      end
      refute_kind_of ArgumentError, err
      refute_match(/watcher backend/, err.message)
    end
  end

  def test_rejects_unknown_watcher_backend
    require_extension_loading!
    ext = find_extension
    skip "honker extension not built — run `cargo build -p honker-extension --release`" unless ext

    ["bogus", "KERNEL", " polling ", :polling].each do |backend|
      err = assert_raises(ArgumentError) do
        Honker::Database.new(
          File.join(Dir.tmpdir, "honker-ruby-missing.db"),
          extension_path: ext,
          watcher_backend: backend,
        )
      end
      assert_match(/unknown watcher backend/, err.message)
    end
  end
end

class HonkerWatcherBackendQueueTest < Minitest::Test
  BACKENDS = [nil, "kernel", "shm"].freeze

  def require_extension_loading!
    require_load_extension_support!
  end

  def unavailable?(error)
    error.message.include?("requires the") ||
      error.message.include?("-shm unavailable") ||
      error.message.include?("unsupported SQLite layout")
  end

  def bootstrap(path, ext, backend)
    db = Honker::Database.new(path, extension_path: ext, watcher_backend: backend)
    q = db.queue("shared")
    id = q.enqueue({ "bootstrap" => true })
    job = q.claim_one("bootstrap")
    assert_equal id, job.id
    assert job.ack
    db.close
    :ok
  rescue ArgumentError => e
    return :skip if unavailable?(e)

    raise
  end

  def spawn_worker(path, ext, backend, worker_id, dir)
    ready_path = File.join(dir, "#{worker_id}.ready")
    result_path = File.join(dir, "#{worker_id}.result")
    pid = Process.spawn(
      RbConfig.ruby,
      __FILE__,
      "--honker-queue-worker",
      path,
      ext,
      backend || "",
      worker_id,
      ready_path,
      result_path,
    )
    [pid, ready_path, result_path]
  end

  def spawn_writer(path, ext, first, count)
    Process.spawn(
      RbConfig.ruby,
      __FILE__,
      "--honker-queue-writer",
      path,
      ext,
      first.to_s,
      count.to_s,
    )
  end

  def enqueue_range(path, ext, first, count)
    db = Honker::Database.new(path, extension_path: ext)
    q = db.queue("shared")
    first.upto(first + count - 1) { |i| q.enqueue({ "i" => i }) }
    db.close
  end

  def enqueue_stops(path, ext, count)
    db = Honker::Database.new(path, extension_path: ext)
    q = db.queue("shared")
    count.times { q.enqueue({ "stop" => true }) }
    db.close
  end

  def wait_ready(path)
    200.times do
      return if File.exist?(path)

      sleep 0.025
    end
    flunk "worker did not become ready: #{path}"
  end

  def wait_process(pid)
    Process.wait(pid)
    assert $?.success?, "child process #{pid} failed with #{$?.inspect}"
  end

  def read_result(path)
    return [] unless File.exist?(path)

    File.read(path).lines.map { |line| Integer(line) }
  end

  def maybe_pin_shm(path, ext, backend)
    return nil unless backend == "shm"

    db = Honker::Database.new(path, extension_path: ext)
    db.db.get_first_value("PRAGMA data_version")
    db
  end

  def assert_int_set(values, count)
    assert_equal (0...count).to_a, values.sort
  end

  def each_backend
    require_extension_loading!
    ext = find_extension
    skip "honker extension not built — run `cargo build -p honker-extension --release`" unless ext

    BACKENDS.each do |backend|
      Dir.mktmpdir("honker-ruby-queue-watchers-") do |dir|
        path = File.join(dir, "q.db")
        next if bootstrap(path, ext, backend) == :skip

        pin = maybe_pin_shm(path, ext, backend)
        begin
          yield path, ext, backend
        ensure
          pin&.close
        end
      end
    end
  end

  def test_queue_backends_drain_1_writer_1_worker_without_fallback_polling
    each_backend do |path, ext, backend|
      worker, ready, result = spawn_worker(path, ext, backend, "w1", File.dirname(path))
      wait_ready(ready)
      enqueue_range(path, ext, 0, 25)
      enqueue_stops(path, ext, 1)
      wait_process(worker)
      assert_int_set(read_result(result), 25)
    end
  end

  def test_queue_backends_drain_1_writer_many_workers_without_double_claims
    each_backend do |path, ext, backend|
      workers = 3.times.map { |i| spawn_worker(path, ext, backend, "w#{i}", File.dirname(path)) }
      workers.each { |_, ready, _| wait_ready(ready) }
      enqueue_range(path, ext, 0, 60)
      enqueue_stops(path, ext, workers.length)
      results = workers.flat_map do |pid, _, result|
        wait_process(pid)
        read_result(result)
      end
      assert_int_set(results, 60)
      assert_equal 60, results.uniq.length
    end
  end

  def test_queue_backends_drain_many_writers_1_worker_without_fallback_polling
    each_backend do |path, ext, backend|
      worker, ready, result = spawn_worker(path, ext, backend, "solo", File.dirname(path))
      wait_ready(ready)
      writers = [0, 20, 40].map { |offset| spawn_writer(path, ext, offset, 20) }
      writers.each { |pid| wait_process(pid) }
      enqueue_stops(path, ext, 1)
      wait_process(worker)
      assert_int_set(read_result(result), 60)
    end
  end
end

class HonkerTest < Minitest::Test
  def setup
    ext = find_extension
    skip "honker extension not built — run `cargo build -p honker-extension --release`" unless ext
    @ext = ext
    @tmpdir = Dir.mktmpdir("honker-ruby-")
    @db_path = File.join(@tmpdir, "t.db")
  end

  def teardown
    FileUtils.remove_entry(@tmpdir) if @tmpdir && File.directory?(@tmpdir)
  end

  def test_enqueue_claim_ack
    db = Honker::Database.new(@db_path, extension_path: @ext)
    q = db.queue("emails")

    id = q.enqueue({ to: "alice@example.com" })
    assert id > 0, "expected positive id"

    job = q.claim_one("worker-1")
    refute_nil job
    assert_equal id, job.id
    assert_equal "alice@example.com", job.payload["to"]
    assert job.ack, "expected ack=true for fresh claim"

    assert_nil q.claim_one("worker-1"), "queue should be empty after ack"
    db.close
  end

  def test_retry_to_dead
    db = Honker::Database.new(@db_path, extension_path: @ext)
    q = db.queue("retries", max_attempts: 2)
    q.enqueue({ i: 1 })

    job = q.claim_one("w")
    refute_nil job
    assert job.retry(delay_s: 0, error: "first")

    job2 = q.claim_one("w")
    refute_nil job2
    assert_equal 2, job2.attempts

    assert job2.retry(delay_s: 0, error: "second")
    assert_nil q.claim_one("w"), "second retry should send to dead"

    row = db.db.get_first_row("SELECT COUNT(*) FROM _honker_dead WHERE queue='retries'")
    assert_equal 1, row[0]
    db.close
  end

  def test_notify
    db = Honker::Database.new(@db_path, extension_path: @ext)
    id = db.notify("orders", { id: 42 })
    assert id > 0

    row = db.db.get_first_row(
      "SELECT channel, payload FROM _honker_notifications WHERE id = ?",
      [id],
    )
    assert_equal "orders", row[0]
    assert_equal({ "id" => 42 }, JSON.parse(row[1]))
    db.close
  end
end
