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
require "sqlite3"

require_relative "honker/version"
require_relative "honker/transaction"
require_relative "honker/stream"
require_relative "honker/scheduler"
require_relative "honker/lock"

module Honker
  class CoreWatcher
    def initialize(db_path, extension_path, backend)
      @lib = Fiddle.dlopen(extension_path)
      @open = Fiddle::Function.new(
        @lib["honker_watcher_open"],
        [Fiddle::TYPE_VOIDP, Fiddle::TYPE_VOIDP, Fiddle::TYPE_VOIDP, Fiddle::TYPE_SIZE_T],
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
      @handle = @open.call(db_path.to_s, backend.to_s, err, err.bytesize)
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
    attr_reader :db

    def initialize(path, extension_path:, watcher_backend: nil)
      unless watcher_backend.nil? || watcher_backend.is_a?(String)
        raise ArgumentError, "unknown watcher backend"
      end

      @db = SQLite3::Database.new(path)
      @local_update_seq = 0
      @db.busy_timeout = 5000
      @db.execute("PRAGMA mmap_size = 0")
      @db.enable_load_extension(true)
      @db.load_extension(extension_path)
      @db.enable_load_extension(false)
      @db.execute_batch(DEFAULT_PRAGMAS)
      @db.execute("SELECT honker_bootstrap()")
      @watcher = CoreWatcher.new(path, extension_path, watcher_backend)
    end

    def close
      @watcher&.close
      @db&.close
    end

    def mark_updated
      @local_update_seq += 1
    end

    def update_snapshot
      @local_update_seq
    end

    def wait_for_update(timeout_s)
      @watcher.wait(timeout_s)
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

  # A claimed unit of work. `payload` is the decoded JSON value (Hash,
  # Array, etc.). `id`, `worker_id`, and `attempts` are metadata from
  # the claim result.
  class Job
    attr_reader :id, :queue_name, :payload, :worker_id, :attempts

    def initialize(queue, row)
      @queue = queue
      @id = row["id"]
      @queue_name = row["queue"]
      @payload = JSON.parse(row["payload"]) unless row["payload"].nil?
      @worker_id = row["worker_id"]
      @attempts = row["attempts"]
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
