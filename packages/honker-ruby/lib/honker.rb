# frozen_string_literal: true
#
# Ruby binding for Honker — a SQLite-native task runtime.
#
# Usage:
#
#   require "honker"
#
#   db = Honker::Database.new("app.db", extension_path: "./libhonker.dylib")
#   q  = db.queue("emails")
#   q.enqueue({to: "alice@example.com"})
#
#   job = q.claim_one("worker-1")
#   send_email(job.payload) if job
#   job&.ack
#
# Thin wrapper around the Honker SQLite loadable extension — each method
# is one SQL call via the `sqlite3` gem. No extra process, no Redis.

require "json"
require "fiddle"
require "rbconfig"
require "sqlite3"

require_relative "honker/version"
require_relative "honker/transaction"
require_relative "honker/stream"
require_relative "honker/scheduler"
require_relative "honker/lock"
require_relative "honker/railtie" if defined?(::Rails::Railtie)

module Honker
  # Honker's error class, raised by ExtensionResolver and CoreWatcher.
  class Error < StandardError; end

  # Resolves the path to the Honker SQLite loadable extension. Platform
  # gems ship it bundled in lib/honker/; an explicit path and the
  # HONKER_EXTENSION_PATH override take precedence.
  class ExtensionResolver
    def initialize(env: ENV.fetch("HONKER_EXTENSION_PATH", nil), bundled: nil)
      @env = env
      @bundled = bundled || File.expand_path("honker/#{extension_filename}", __dir__)
    end

    # Returns the extension path: an explicit `extension_path`, else
    # HONKER_EXTENSION_PATH, else the bundled extension. Raises
    # Honker::Error when HONKER_EXTENSION_PATH or the bundled extension
    # is missing.
    def resolve(extension_path = nil)
      return extension_path unless extension_path.nil?
      return env_extension unless env.nil? || env.empty?

      path = bundled
      return path if File.file?(path)

      raise Error, "Honker SQLite extension not found at #{path}; " \
                   "set HONKER_EXTENSION_PATH or pass extension_path:"
    end

    private

    attr_reader :env, :bundled

    def env_extension
      return env if File.file?(env)

      raise Error, "HONKER_EXTENSION_PATH does not exist: #{env}"
    end

    def extension_filename
      case RbConfig::CONFIG.fetch("host_os")
      when /mswin|mingw|cygwin/ then "honker_ext.dll"
      when /darwin/ then "libhonker_ext.dylib"
      else "libhonker_ext.so"
      end
    end
  end

  # Resolve the bundled (or overridden) extension path without naming
  # ExtensionResolver — useful for `database.yml` ERB and tooling.
  def self.extension_path(override = nil)
    ExtensionResolver.new.resolve(override)
  end

  # Load the Honker extension onto a raw SQLite3::Database. Encapsulates
  # the enable_load_extension(true)/load/enable_load_extension(false)
  # sequence so the toggle-off can't be forgotten.
  def self.load_extension(sqlite_conn, extension_path: nil)
    resolved = ExtensionResolver.new.resolve(extension_path)
    sqlite_conn.enable_load_extension(true)
    sqlite_conn.load_extension(resolved)
  ensure
    sqlite_conn.enable_load_extension(false)
  end

  # Run honker_bootstrap() on the connection. Idempotent. Separated from
  # load_extension so production users can opt to bootstrap from a
  # migration instead of every connect.
  def self.bootstrap(sqlite_conn)
    sqlite_conn.execute("SELECT honker_bootstrap()")
  end

  # Convenience: load the extension then bootstrap. The one-liner most
  # ORM integrations reach for.
  def self.setup(sqlite_conn, extension_path: nil, bootstrap: true)
    load_extension(sqlite_conn, extension_path: extension_path)
    self.bootstrap(sqlite_conn) if bootstrap
  end

  # Returns a Proc suitable for Sequel/Rom/Hanami `after_connect:`.
  def self.sequel_after_connect(extension_path: nil, bootstrap: true)
    proc { |conn| setup(conn, extension_path: extension_path, bootstrap: bootstrap) }
  end

  class CoreWatcher
    def initialize(db_path, extension_path, backend, watcher_poll_interval_ms)
      @lib = Fiddle.dlopen(extension_path)
      @open = Fiddle::Function.new(
        @lib["honker_watcher_open_v2"],
        [Fiddle::TYPE_VOIDP, Fiddle::TYPE_VOIDP, Fiddle::TYPE_LONG_LONG, Fiddle::TYPE_VOIDP, Fiddle::TYPE_SIZE_T],
        Fiddle::TYPE_VOIDP,
      )
      @wait = Fiddle::Function.new(
        @lib["honker_watcher_wait"],
        [Fiddle::TYPE_VOIDP, Fiddle::TYPE_LONG_LONG],
        Fiddle::TYPE_INT,
      )
      @close = Fiddle::Function.new(
        @lib["honker_watcher_close"],
        [Fiddle::TYPE_VOIDP],
        Fiddle::TYPE_VOID,
      )
      err = "\0" * 1024
      @handle = @open.call(
        db_path.to_s,
        backend.to_s,
        watcher_poll_interval_ms || 1,
        err,
        err.bytesize,
      )
      return unless @handle.to_i.zero?

      raise ArgumentError, err.delete_suffix("\0").split("\0", 2).first
    end

    def wait(timeout_s)
      code = @wait.call(@handle, (timeout_s * 1000).ceil)
      return true if code == 1
      return false if code == 0

      raise Error, "honker update watcher closed or died"
    end

    def close
      return if @handle.nil? || @handle.to_i.zero?

      @close.call(@handle)
      @handle = nil
    end
  end

  # Fans one native watcher out to any number of Ruby waiters. The native
  # watcher owns a single receiver, so callers must not race each other on
  # CoreWatcher#wait directly.
  class UpdateHub
    WAIT_SLICE_S = 0.1

    def initialize(watcher)
      @watcher = watcher
      @mutex = Mutex.new
      @changed = ConditionVariable.new
      @generation = 0
      @seen_by_thread = {}
      @stopping = false
      @closed = false
      @disposed = false
      @error = nil
      @thread = Thread.new { run }
      @thread.name = "honker-update-hub" if @thread.respond_to?(:name=)
    end

    def snapshot
      @mutex.synchronize { @generation }
    end

    def closed?
      @mutex.synchronize { @closed }
    end

    def signal
      @mutex.synchronize do
        return if @closed

        @generation += 1
        @changed.broadcast
      end
    end

    # Wake blocked waiters without claiming that SQLite changed. Listener
    # cancellation uses this path so closing one listener cannot create a
    # spurious Database#wait_for_update result for unrelated callers.
    def wake
      @mutex.synchronize { @changed.broadcast unless @closed }
    end

    def wait_after(generation, timeout_s = nil, &cancelled)
      deadline = timeout_s.nil? ? nil : monotonic_now + [timeout_s.to_f, 0.0].max
      @mutex.synchronize do
        loop do
          return false if cancelled&.call
          raise @error if @error
          return false if @closed
          return true if @generation > generation

          if deadline
            remaining = deadline - monotonic_now
            return false if remaining <= 0

            @changed.wait(@mutex, remaining)
          else
            @changed.wait(@mutex)
          end
        end
      end
    end

    def wait(timeout_s)
      deadline = monotonic_now + [timeout_s.to_f, 0.0].max
      key = Thread.current
      @mutex.synchronize do
        @seen_by_thread.delete_if { |thread, _generation| !thread.alive? }
        loop do
          raise @error if @error
          return false if @closed
          if @generation > @seen_by_thread.fetch(key, 0)
            @seen_by_thread[key] = @generation
            return true
          end

          remaining = deadline - monotonic_now
          return false if remaining <= 0

          @changed.wait(@mutex, remaining)
        end
      end
    end

    def close
      @mutex.synchronize do
        return if @disposed

        if @stopping
          @changed.wait(@mutex) until @disposed
          return
        end

        @stopping = true
      end
      begin
        @thread.join
        @watcher.close
      ensure
        @mutex.synchronize do
          @disposed = true
          @closed = true
          @changed.broadcast
        end
      end
    end

    private

    def run
      loop do
        break if @mutex.synchronize { @stopping }

        signal if @watcher.wait(WAIT_SLICE_S)
      end
    rescue StandardError => e
      @mutex.synchronize do
        @error = e
        @closed = true
        @changed.broadcast
      end
    ensure
      @mutex.synchronize do
        @closed = true unless @stopping
        @changed.broadcast
      end
    end

    def monotonic_now
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end
  end

  # A live pub/sub notification. Payloads produced by Database#notify are
  # JSON-decoded; raw non-JSON SQL payloads are returned unchanged.
  Notification = Struct.new(:id, :channel, :payload, :created_at)

  class Listener
    include Enumerable

    def initialize(db, channel, fallback_poll_s: 15.0)
      raise ArgumentError, "channel must not be empty" if channel.to_s.empty?
      if !fallback_poll_s.nil? && !fallback_poll_s.to_f.positive?
        raise ArgumentError, "fallback_poll_s must be positive or nil"
      end

      @db = db
      @channel = channel.to_s
      @fallback_poll_s = fallback_poll_s&.to_f
      @buffer = []
      @state_mutex = Mutex.new
      @state_changed = ConditionVariable.new
      @active_calls = 0
      @active_threads = Hash.new(0)
      @closed = false
      @read_db = SQLite3::Database.new(@db.path)
      @read_db.busy_timeout = 5000
      @read_db.execute("PRAGMA query_only = ON")
      @last_seen = @read_db.get_first_value(
        "SELECT COALESCE(MAX(id), 0) FROM _honker_notifications WHERE channel = ?",
        [@channel],
      ).to_i
    rescue StandardError
      @read_db&.close
      raise
    end

    attr_reader :channel

    def next(timeout_s: nil)
      read_db = begin_call
      return nil unless read_db

      deadline = timeout_s.nil? ? nil : monotonic_now + [timeout_s.to_f, 0.0].max
      loop do
        return nil if closed?
        return @buffer.shift unless @buffer.empty?

        generation = @db.update_snapshot
        refill(read_db)
        next unless @buffer.empty?

        remaining = deadline && deadline - monotonic_now
        return nil if remaining && remaining <= 0

        wait_s = [remaining, @fallback_poll_s].compact.min
        @db.wait_for_update_after(generation, wait_s) { closed? }
        if @db.updates_closed?
          close
          return nil
        end
      end
    rescue StandardError
      close
      raise
    ensure
      end_call if read_db
    end

    def each
      return enum_for(:each) unless block_given?

      while (notification = self.next)
        yield notification
      end
    end

    def close
      should_wake = @state_mutex.synchronize do
        next false if @closed

        @closed = true
        close_read_db_if_idle
        true
      end

      if should_wake
        @db.wake_update_waiters
        @db.unregister_listener(self)
      end

      @state_mutex.synchronize do
        unless @active_threads.key?(Thread.current)
          @state_changed.wait(@state_mutex) until @active_calls.zero?
          close_read_db_if_idle
        end
      end
      nil
    end

    def closed?
      @state_mutex.synchronize { @closed }
    end

    private

    def refill(read_db)
      rows = read_db.execute(
        "SELECT id, channel, payload, created_at " \
        "FROM _honker_notifications " \
        "WHERE channel = ? AND id > ? ORDER BY id LIMIT 1000",
        [@channel, @last_seen],
      )
      rows.each do |id, channel, payload, created_at|
        @last_seen = id.to_i
        @buffer << Notification.new(id.to_i, channel, decode_payload(payload), created_at.to_i)
      end
    end

    def begin_call
      @state_mutex.synchronize do
        return nil if @closed

        @active_calls += 1
        @active_threads[Thread.current] += 1
        @read_db
      end
    end

    def end_call
      @state_mutex.synchronize do
        @active_calls -= 1
        @active_threads[Thread.current] -= 1
        @active_threads.delete(Thread.current) if @active_threads[Thread.current].zero?
        close_read_db_if_idle
        @state_changed.broadcast if @active_calls.zero?
      end
    end

    def close_read_db_if_idle
      return unless @closed && @active_calls.zero? && @read_db

      @read_db.close
      @read_db = nil
    end

    def decode_payload(payload)
      JSON.parse(payload)
    rescue JSON::ParserError, TypeError
      payload
    end

    def monotonic_now
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end
  end

  DEFAULT_PRAGMAS = <<~SQL
    PRAGMA journal_mode = WAL;
    PRAGMA synchronous = NORMAL;
    PRAGMA busy_timeout = 5000;
    PRAGMA mmap_size = 0;
    PRAGMA foreign_keys = ON;
    PRAGMA cache_size = -32000;
    PRAGMA temp_store = MEMORY;
    PRAGMA wal_autocheckpoint = 10000;
  SQL

  # Database is a Honker handle over a SQLite file with the Honker
  # extension loaded. The constructor bootstraps the schema; safe to
  # open the same path from multiple processes.
  class Database
    attr_reader :db, :path

    def initialize(path, extension_path: nil, watcher_backend: nil,
                   watcher_poll_interval_ms: nil,
                   extension_resolver: ExtensionResolver.new)
      unless watcher_backend.nil? || watcher_backend.is_a?(String)
        raise ArgumentError, "unknown watcher backend"
      end
      unless watcher_poll_interval_ms.nil? || watcher_poll_interval_ms.to_i.positive?
        raise ArgumentError, "watcher_poll_interval_ms must be positive"
      end

      resolved_extension = extension_resolver.resolve(extension_path)
      @path = path
      @db = SQLite3::Database.new(path)
      @db.busy_timeout = 5000
      @db.execute("PRAGMA mmap_size = 0")
      @db.enable_load_extension(true)
      @db.load_extension(resolved_extension)
      @db.enable_load_extension(false)
      @db.execute_batch(DEFAULT_PRAGMAS)
      @db.execute("SELECT honker_bootstrap()")
      watcher = CoreWatcher.new(path, resolved_extension, watcher_backend, watcher_poll_interval_ms)
      @updates = UpdateHub.new(watcher)
      @listeners_mutex = Mutex.new
      @listeners = {}
      @closed = false
    end

    def close
      listeners = @listeners_mutex.synchronize do
        return if @closed

        @closed = true
        registered = @listeners.keys
        @listeners.clear
        registered
      end
      listeners.each(&:close)
      @updates&.close
      @db&.close
    end

    def mark_updated
      @updates.signal
    end

    def update_snapshot
      @updates.snapshot
    end

    def wait_for_update(timeout_s)
      @updates.wait(timeout_s)
    end

    def wait_for_update_after(generation, timeout_s, &cancelled)
      @updates.wait_after(generation, timeout_s, &cancelled)
    end

    def wake_update_waiters
      @updates.wake
    end

    def updates_closed?
      @updates.closed?
    end

    def closed?
      @listeners_mutex.synchronize { @closed }
    end

    def listen(channel, fallback_poll_s: 15.0)
      raise Error, "database is closed" if closed?

      listener = Listener.new(self, channel, fallback_poll_s: fallback_poll_s)
      unless register_listener(listener)
        listener.close
        raise Error, "database is closed"
      end
      return listener unless block_given?

      begin
        listener.each { |notification| yield notification }
      ensure
        listener.close
      end
    end

    # Internal listener lifecycle hooks. They are public only because the
    # Listener is a separate object rather than a nested implementation detail.
    def register_listener(listener)
      @listeners_mutex.synchronize do
        return false if @closed

        @listeners[listener] = true
        true
      end
    end

    def unregister_listener(listener)
      @listeners_mutex.synchronize { @listeners.delete(listener) }
      nil
    end

    # Returns a Queue handle for a named queue.
    #
    #   visibility_timeout_s: 300   # claim expiry before reclaim
    #   max_attempts:         3     # retries before moving to dead
    def queue(name, visibility_timeout_s: 300, max_attempts: 3)
      Queue.new(
        self,
        name,
        visibility_timeout_s: visibility_timeout_s,
        max_attempts: max_attempts,
      )
    end

    # Transactional side-effect delivery built on a reserved queue.
    def outbox(name, delivery, visibility_timeout_s: 60, max_attempts: 5, base_backoff_s: 5)
      Outbox.new(
        self,
        name,
        delivery,
        visibility_timeout_s: visibility_timeout_s,
        max_attempts: max_attempts,
        base_backoff_s: base_backoff_s,
      )
    end

    # Returns a Stream handle for an append-only ordered log.
    def stream(name)
      Stream.new(self, name)
    end

    # Returns the time-trigger Scheduler facade. Cheap — no allocation
    # beyond the wrapper object.
    def scheduler
      Scheduler.new(self)
    end

    # Fire a pg_notify-style pub/sub signal. Returns the notification id.
    def notify(channel, payload)
      row = @db.get_first_row("SELECT notify(?, ?)", [channel, JSON.dump(payload)])
      row[0]
    end

    # Fire a notification inside an open transaction. The signal lands
    # only when the transaction commits.
    def notify_tx(tx, channel, payload)
      row = tx.query_row(
        "SELECT notify(?, ?)",
        [channel, JSON.dump(payload)],
      )
      row[0]
    end

    # Run a block inside a SQLite transaction. The block receives a
    # Honker::Transaction; returning normally commits, raising rolls
    # back, and `tx.rollback!` rolls back without surfacing an error.
    #
    #   db.transaction do |tx|
    #     tx.execute("INSERT INTO orders ...")
    #     q.enqueue_tx(tx, {order_id: 1})
    #   end
    def transaction
      tx = Transaction.new(@db)
      begin
        @db.transaction do
          yield tx
        end
      rescue Transaction::Rollback
        # Caller used tx.rollback! to abort. The block exited with an
        # exception so the sqlite3 gem already rolled back — just
        # swallow the sentinel.
        nil
      end
    end

    # Try to acquire an advisory lock. Returns a `Lock` handle on
    # success, `nil` if another owner holds it.
    def try_lock(name, owner:, ttl_s:)
      acquired = @db.get_first_row(
        "SELECT honker_lock_acquire(?, ?, ?)",
        [name, owner, ttl_s],
      )[0]
      return nil unless acquired == 1

      Lock.new(self, name, owner)
    end

    # Fixed-window rate limiter. Returns true if this call fits within
    # `limit` requests per `per` seconds.
    def try_rate_limit(name, limit:, per:)
      @db.get_first_row(
        "SELECT honker_rate_limit_try(?, ?, ?)",
        [name, limit, per],
      )[0] == 1
    end

    # Sweep old rate-limit window rows. Returns count deleted.
    def sweep_rate_limits(older_than_s:)
      @db.get_first_row(
        "SELECT honker_rate_limit_sweep(?)",
        [older_than_s],
      )[0]
    end

    # Persist a job result for later retrieval via `get_result`.
    # `value` is stored verbatim — JSON-encode it yourself if you want
    # to round-trip structured data.
    def save_result(job_id, value, ttl_s:)
      @db.get_first_row(
        "SELECT honker_result_save(?, ?, ?)",
        [job_id, value, ttl_s],
      )
      nil
    end

    # Fetch a stored result, or nil if absent or expired.
    def get_result(job_id)
      @db.get_first_row(
        "SELECT honker_result_get(?)",
        [job_id],
      )[0]
    end

    # Drop expired result rows. Returns count swept.
    def sweep_results
      @db.get_first_row("SELECT honker_result_sweep()")[0]
    end

    # Delete notifications older than `older_than_s` seconds. Returns
    # the number of rows deleted.
    def prune_notifications(older_than_s:)
      @db.execute(
        "DELETE FROM _honker_notifications WHERE created_at < unixepoch() - ?",
        [older_than_s],
      )
      @db.changes
    end
  end

  class Queue
    attr_reader :name, :max_attempts

    def initialize(db, name, visibility_timeout_s:, max_attempts:)
      @db = db
      @name = name
      @visibility_timeout_s = visibility_timeout_s
      @max_attempts = max_attempts
    end

    # Enqueue a job. Returns the inserted row id.
    #
    #   q.enqueue({to: "alice"}, delay: 60, priority: 10, expires: 3600)
    def enqueue(payload, delay: nil, run_at: nil, priority: 0, expires: nil)
      row = @db.db.get_first_row(
        "SELECT honker_enqueue(?, ?, ?, ?, ?, ?, ?)",
        [@name, JSON.dump(payload), run_at, delay, priority, @max_attempts, expires],
      )
      row[0]
    end

    # Enqueue inside an open transaction. Atomic with whatever else ran
    # on the same tx.
    def enqueue_tx(tx, payload, delay: nil, run_at: nil, priority: 0, expires: nil)
      row = tx.query_row(
        "SELECT honker_enqueue(?, ?, ?, ?, ?, ?, ?)",
        [@name, JSON.dump(payload), run_at, delay, priority, @max_attempts, expires],
      )
      row[0]
    end

    # Claim up to n jobs atomically. Returns an array of Job.
    def claim_batch(worker_id, n)
      rows_json = @db.db.get_first_row(
        "SELECT honker_claim_batch(?, ?, ?, ?)",
        [@name, worker_id, n, @visibility_timeout_s],
      )[0]
      JSON.parse(rows_json).map { |r| Job.new(self, r) }
    end

    # Claim a single job or nil if the queue is empty.
    def claim_one(worker_id)
      claim_batch(worker_id, 1).first
    end

    # Ack multiple jobs in one transaction. Returns the number acked.
    def ack_batch(ids, worker_id)
      @db.db.get_first_row(
        "SELECT honker_ack_batch(?, ?)",
        [JSON.dump(ids), worker_id],
      )[0]
    end

    # Sweep this queue's expired claims back to pending. Returns the
    # number of rows reclaimed.
    def sweep_expired
      @db.db.get_first_row(
        "SELECT honker_sweep_expired(?)",
        [@name],
      )[0]
    end

    # Internal: invoked by Job#ack.
    def _ack(job_id, worker_id)
      @db.db.get_first_row("SELECT honker_ack(?, ?)", [job_id, worker_id])[0] == 1
    end

    # Internal: invoked by Job#retry.
    def _retry(job_id, worker_id, delay_s, err_msg)
      @db.db.get_first_row(
        "SELECT honker_retry(?, ?, ?, ?)",
        [job_id, worker_id, delay_s, err_msg],
      )[0] == 1
    end

    # Internal: invoked by Job#fail.
    def _fail(job_id, worker_id, err_msg)
      @db.db.get_first_row(
        "SELECT honker_fail(?, ?, ?)",
        [job_id, worker_id, err_msg],
      )[0] == 1
    end

    # Internal: invoked by Job#heartbeat.
    def _heartbeat(job_id, worker_id, extend_s)
      @db.db.get_first_row(
        "SELECT honker_heartbeat(?, ?, ?)",
        [job_id, worker_id, extend_s],
      )[0] == 1
    end

    # Delete a pending or processing job by id. Returns true iff a row
    # was removed. Idempotent on missing.
    #
    # IMPORTANT: cancel does NOT interrupt a worker currently running
    # the handler. It invalidates the worker's claim — its next
    # ack/heartbeat returns false. If you need the handler to actually
    # halt, build that signal in your app.
    def cancel(job_id)
      n = @db.db.get_first_row("SELECT honker_cancel(?)", [job_id])[0]
      @db.mark_updated if n.positive?
      n.positive?
    end

    # Read a single job row by id. Returns a JobSnapshot, or nil if the
    # job has been ack'd, dead'd, or never existed.
    #
    # The lookup is by id alone. Ids are globally unique but not scoped
    # to this queue, so a foreign id returns that queue's row (#134).
    def get_job(job_id)
      raw = @db.db.get_first_row("SELECT honker_get_job(?)", [job_id])[0]
      return nil if raw.nil? || raw.empty?

      JobSnapshot.from_row(JSON.parse(raw))
    end
  end

  class Outbox
    attr_reader :name, :queue, :max_attempts, :base_backoff_s

    def initialize(db, name, delivery, visibility_timeout_s:, max_attempts:, base_backoff_s:)
      raise ArgumentError, "delivery must respond to #call" unless delivery.respond_to?(:call)

      @name = name
      @delivery = delivery
      @max_attempts = max_attempts
      @base_backoff_s = base_backoff_s
      @queue = db.queue(
        "_outbox:#{name}",
        visibility_timeout_s: visibility_timeout_s,
        max_attempts: max_attempts,
      )
    end

    def enqueue(payload, tx: nil, delay: nil, run_at: nil, priority: 0, expires: nil)
      if tx
        @queue.enqueue_tx(tx, payload, delay: delay, run_at: run_at, priority: priority, expires: expires)
      else
        @queue.enqueue(payload, delay: delay, run_at: run_at, priority: priority, expires: expires)
      end
    end

    def run_once(worker_id)
      job = @queue.claim_one(worker_id)
      return false unless job

      begin
        if @delivery.arity == 1
          @delivery.call(job.payload)
        else
          @delivery.call(job.payload, job)
        end
        raise "outbox ack failed for job #{job.id}" unless job.ack
      rescue StandardError => e
        delay_s = retry_delay(job.attempts)
        raise "outbox retry failed for job #{job.id}" unless job.retry(delay_s: delay_s, error: "#{e}\n#{e.backtrace&.join("\n")}")
      end
      true
    end

    def run_worker(worker_id, stop: nil, idle_sleep_s: 0.1)
      until stop&.call
        processed = run_once(worker_id)
        sleep(idle_sleep_s) unless processed
      end
    end

    private

    def retry_delay(attempts)
      return 0 if @base_backoff_s <= 0

      (@base_backoff_s * (2**[attempts - 1, 0].max)).ceil
    end
  end

  # A read-only view of a live job row, as returned by Queue#get_job.
  # Data only — no ack/retry/fail/heartbeat, because the reader does
  # not hold the claim.
  #
  # Fields match the row: `state` is "pending" or "processing";
  # `worker_id` and `claim_expires_at` are nil until a worker claims
  # the job; `expires_at` is nil unless the job was enqueued with
  # `expires:`. All times are unix epoch seconds.
  #
  # NOTE: `payload` is the raw JSON *text* stored in the row, not a
  # decoded value — unlike Job#payload, which is decoded. Call
  # JSON.parse on it. That difference is inherited from the SQL ABI
  # and is deliberately left alone here; the bindings do not yet agree
  # on one snapshot payload encoding.
  #
  # get_job used to return a Hash of this row. Reader access
  # (snapshot["state"]) still works, and JSON.dump still emits the
  # same JSON object, but Hash-only methods do not: #fetch, #key? and
  # #keys raise NoMethodError, an unknown name raises NameError
  # instead of returning nil, and #to_h has Symbol keys, not String.
  JobSnapshot = Struct.new(
    :id, :queue, :payload, :state, :priority, :run_at, :worker_id,
    :claim_expires_at, :attempts, :max_attempts, :created_at, :expires_at
  ) do
    # Build a snapshot from a decoded honker_get_job() row.
    def self.from_row(row)
      new(
        row["id"],
        row["queue"],
        row["payload"],
        row["state"],
        row["priority"],
        row["run_at"],
        row["worker_id"],
        row["claim_expires_at"],
        row["attempts"],
        row["max_attempts"],
        row["created_at"],
        row["expires_at"],
      )
    end

    # Job exposes the same value as #queue_name; accept both here.
    alias_method :queue_name, :queue

    # Serialize as the row's JSON object, the way the Hash this used
    # to be did. Without this a Struct serializes to its #inspect
    # string, which silently turns a logged snapshot into garbage.
    def to_json(*args)
      to_h.transform_keys(&:to_s).to_json(*args)
    end
  end

  # A claimed unit of work. `payload` is the decoded JSON value (Hash,
  # Array, etc.). Honker never inspects the payload — the shape is a
  # contract between the producer and the consumer, so every app
  # writing to a queue has to agree on it.
  #
  # The rest of the readers are the job's row as it stood at claim
  # time: `state` ("processing"), `priority`, `run_at`, `worker_id`,
  # `claim_expires_at`, `attempts` (already incremented by this
  # claim), `max_attempts`, `created_at`, and `expires_at` (nil unless
  # enqueued with `expires:`). Times are unix epoch seconds.
  class Job
    attr_reader :id, :queue_name, :payload, :state, :priority, :run_at,
                :worker_id, :claim_expires_at, :attempts, :max_attempts,
                :created_at, :expires_at

    # JobSnapshot names this field #queue; accept both here too, so
    # code written against one works on the other.
    alias_method :queue, :queue_name

    def initialize(queue, row)
      @queue = queue
      @id = row["id"]
      @queue_name = row["queue"]
      @payload = JSON.parse(row["payload"]) unless row["payload"].nil?
      @state = row["state"]
      @priority = row["priority"]
      @run_at = row["run_at"]
      @worker_id = row["worker_id"]
      @claim_expires_at = row["claim_expires_at"]
      @attempts = row["attempts"]
      @max_attempts = row["max_attempts"]
      @created_at = row["created_at"]
      @expires_at = row["expires_at"]
    end

    # DELETEs the row if the claim is still valid. Returns true/false.
    def ack
      @queue._ack(@id, @worker_id)
    end

    # Returns the job to pending with a delay, or moves it to dead
    # after max_attempts. Returns true iff the claim was valid.
    def retry(delay_s: 60, error: "")
      @queue._retry(@id, @worker_id, delay_s, error)
    end

    # Unconditionally moves the claim to dead.
    def fail(error: "")
      @queue._fail(@id, @worker_id, error)
    end

    # Extend the claim's visibility timeout.
    def heartbeat(extend_s:)
      @queue._heartbeat(@id, @worker_id, extend_s)
    end
  end
end
