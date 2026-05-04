defmodule Honker.Scheduler do
  @moduledoc """
  Time-trigger task registry. Thin wrapper over `honker_scheduler_*`
  SQL functions, plus a blocking `run/3` loop with leader election via
  the `honker-scheduler` advisory lock.

      :ok = Honker.Scheduler.add(db,
        name: "hourly-health",
        queue: "health",
        schedule: "0 * * * *",
        payload: %{}
      )

      stop = :atomics.new(1, [])
      :ok = :atomics.put(stop, 1, 0)
      Task.async(fn ->
        Honker.Scheduler.run(db, "worker-1", fn -> :atomics.get(stop, 1) == 1 end)
      end)
  """

  alias Honker.{Database, Lock}

  @lock_name "honker-scheduler"
  @lock_ttl_s 60
  @heartbeat_ms 20_000
  @standby_poll_ms 5_000

  @doc """
  Register a recurring scheduled task. Idempotent by `:name`.

  Options (all required except `:priority` and `:expires_s`):

    * `:name`       — unique task name
    * `:queue`      — queue the payload is enqueued onto each fire
    * `:schedule`   — canonical schedule expression:
                      5-field cron, 6-field cron, or `@every <n><unit>`
    * `:cron`       — backward-compatible alias for `:schedule`
    * `:payload`    — any JSON-encodable term
    * `:priority`   — integer (default 0)
    * `:expires_s`  — seconds; the fired job expires this many seconds
                      after its scheduled fire time. Default `nil`.
  """
  def add(%Database{conn: conn}, opts) do
    name = Keyword.fetch!(opts, :name)
    queue = Keyword.fetch!(opts, :queue)
    schedule = Keyword.get(opts, :schedule) || Keyword.get(opts, :cron)
    if is_nil(schedule), do: raise(ArgumentError, "missing required option :schedule")
    payload = Keyword.fetch!(opts, :payload)
    priority = Keyword.get(opts, :priority, 0)
    expires_s = Keyword.get(opts, :expires_s)

    payload_json = Jason.encode!(payload)

    case Honker.query_first(
           conn,
           "SELECT honker_scheduler_register(?1, ?2, ?3, ?4, ?5, ?6)",
           [name, queue, schedule, payload_json, priority, expires_s]
         ) do
      {:ok, [_]} ->
        Honker.mark_updated(conn)
        :ok

      {:ok, nil} ->
        Honker.mark_updated(conn)
        :ok

      other -> other
    end
  end

  @doc "Unregister a task by name. Returns `{:ok, count}`."
  def remove(%Database{conn: conn}, name) do
    case Honker.query_first(
           conn,
           "SELECT honker_scheduler_unregister(?1)",
           [name]
         ) do
      {:ok, [count]} ->
        Honker.mark_updated(conn)
        {:ok, count}

      other -> other
    end
  end

  @doc """
  Fire any tasks whose boundaries are due. Returns
  `{:ok, [%{name, queue, fire_at, job_id}, ...]}` — a list of maps with
  string keys as decoded from the JSON returned by the extension.
  """
  def tick(%Database{conn: conn}) do
    now = System.system_time(:second)

    case Honker.query_first(
           conn,
           "SELECT honker_scheduler_tick(?1)",
           [now]
         ) do
      {:ok, [rows_json]} -> {:ok, Jason.decode!(rows_json)}
      other -> other
    end
  end

  @doc "Soonest unix timestamp across all tasks, or 0 if none."
  def soonest(%Database{conn: conn}) do
    case Honker.query_first(conn, "SELECT honker_scheduler_soonest()", []) do
      {:ok, [ts]} -> {:ok, ts || 0}
      {:ok, nil} -> {:ok, 0}
      other -> other
    end
  end

  # ---- Phase Mantle: lifecycle methods ----

  @doc "Pause a registered schedule. Returns `{:ok, true}` if a row was paused."
  def pause(%Database{conn: conn}, name) do
    case Honker.query_first(conn, "SELECT honker_scheduler_pause(?1)", [name]) do
      {:ok, [n]} ->
        if n > 0, do: Honker.mark_updated(conn)
        {:ok, n > 0}

      other ->
        other
    end
  end

  @doc "Resume a paused schedule. Returns `{:ok, true}` if a row was resumed."
  def resume(%Database{conn: conn}, name) do
    case Honker.query_first(conn, "SELECT honker_scheduler_resume(?1)", [name]) do
      {:ok, [n]} ->
        if n > 0, do: Honker.mark_updated(conn)
        {:ok, n > 0}

      other ->
        other
    end
  end

  @doc """
  Return every registered schedule with current state. Each entry is a
  map with: name, queue, cron_expr, payload, priority, expires_s,
  next_fire_at, enabled.
  """
  def list(%Database{conn: conn}) do
    case Honker.query_first(conn, "SELECT honker_scheduler_list()", []) do
      {:ok, [raw]} when is_binary(raw) and byte_size(raw) > 0 ->
        {:ok, Jason.decode!(raw)}

      {:ok, _} ->
        {:ok, []}

      other ->
        other
    end
  end

  @doc """
  Mutate fields in place. `opts` is a keyword list with any of:
  `cron:` / `schedule:`, `payload:`, `priority:`, `expires_s:`. Omit a
  key to leave its field alone. `payload: nil` writes JSON null.
  Cron change recomputes `next_fire_at` from now. Returns
  `{:ok, true}` iff a row was updated.
  """
  def update(%Database{conn: conn}, name, opts \\ []) do
    cron_arg =
      cond do
        Keyword.has_key?(opts, :schedule) -> Keyword.get(opts, :schedule)
        Keyword.has_key?(opts, :cron) -> Keyword.get(opts, :cron)
        true -> nil
      end

    payload_arg =
      if Keyword.has_key?(opts, :payload),
        do: Jason.encode!(Keyword.get(opts, :payload)),
        else: nil

    priority_arg = if Keyword.has_key?(opts, :priority), do: Keyword.get(opts, :priority), else: nil
    touch_expires = if Keyword.has_key?(opts, :expires_s), do: 1, else: 0
    expires_arg = if Keyword.has_key?(opts, :expires_s), do: Keyword.get(opts, :expires_s), else: nil

    if cron_arg == nil and payload_arg == nil and priority_arg == nil and touch_expires == 0 do
      # Empty update is a no-op.
      {:ok, false}
    else
      case Honker.query_first(
             conn,
             "SELECT honker_scheduler_update(?1, ?2, ?3, ?4, ?5, ?6)",
             [name, cron_arg, payload_arg, priority_arg, expires_arg, touch_expires]
           ) do
        {:ok, [n]} ->
          if n > 0, do: Honker.mark_updated(conn)
          {:ok, n > 0}

        other ->
          other
      end
    end
  end

  @doc """
  Run the scheduler loop with leader election. Blocks until `stop_fun`
  returns `true`. Only the process holding the `honker-scheduler` lock
  fires ticks; standbys poll for the lock to expire.

  Lock TTL is 60 seconds; we refresh every 20 seconds. If a refresh
  returns `false` (TTL expired, someone else took it), we exit the
  leader loop *before* firing another tick so we never double-fire
  with the new leader.

  On any tick error we release the lock before returning the error so
  a standby can take over immediately.
  """
  def run(%Database{} = db, owner, stop_fun) when is_function(stop_fun, 0) do
    cond do
      stop_fun.() ->
        :ok

      true ->
        case Lock.try_acquire(db, @lock_name, owner, @lock_ttl_s) do
          {:ok, nil} ->
            wait_update_or_timeout(db, @standby_poll_ms, stop_fun)
            run(db, owner, stop_fun)

          {:ok, %Lock{} = lock} ->
            case leader_loop(db, lock, stop_fun, System.monotonic_time(:millisecond)) do
              :ok ->
                _ = Lock.release(lock, db)
                run(db, owner, stop_fun)

              {:stop, :lost_lock} ->
                # Lock refresh failed — someone else is leader now.
                # Go back to standby without releasing (it isn't ours).
                run(db, owner, stop_fun)

              {:stop, :stopped} ->
                _ = Lock.release(lock, db)
                :ok

              {:error, _} = err ->
                _ = Lock.release(lock, db)
                err
            end

          {:error, _} = err ->
            err
        end
    end
  end

  defp leader_loop(%Database{} = db, %Lock{} = lock, stop_fun, last_heartbeat_ms) do
    cond do
      stop_fun.() ->
        {:stop, :stopped}

      true ->
        case tick(db) do
          {:ok, _fires} ->
            now_ms = System.monotonic_time(:millisecond)

            case maybe_heartbeat(db, lock, last_heartbeat_ms, now_ms) do
              {:ok, :kept, new_last_ms} ->
                wait_ms = next_wait_ms(db, now_ms, new_last_ms)
                wait_update_or_timeout(db, wait_ms, stop_fun)
                leader_loop(db, lock, stop_fun, new_last_ms)

              {:ok, :lost} ->
                {:stop, :lost_lock}

              {:error, _} = err ->
                err
            end

          {:error, _} = err ->
            err
        end
    end
  end

  defp maybe_heartbeat(db, lock, last_ms, now_ms) do
    if now_ms - last_ms >= @heartbeat_ms do
      case Lock.heartbeat(lock, db, @lock_ttl_s) do
        {:ok, true} -> {:ok, :kept, now_ms}
        {:ok, false} -> {:ok, :lost}
        err -> err
      end
    else
      {:ok, :kept, last_ms}
    end
  end

  defp next_wait_ms(db, now_ms, last_heartbeat_ms) do
    heartbeat_wait = max(0, @heartbeat_ms - (now_ms - last_heartbeat_ms))

    case soonest(db) do
      {:ok, ts} when is_integer(ts) and ts > 0 ->
        fire_wait = max(0, ts * 1000 - System.system_time(:millisecond))
        min(heartbeat_wait, fire_wait)

      _ ->
        heartbeat_wait
    end
  end

  defp wait_update_or_timeout(_db, ms, _stop_fun) when ms <= 0, do: :ok

  defp wait_update_or_timeout(db, ms, stop_fun) do
    deadline = System.monotonic_time(:millisecond) + ms
    do_wait_update_or_timeout(db, deadline, stop_fun)
  end

  defp do_wait_update_or_timeout(db, deadline, stop_fun) do
    now = System.monotonic_time(:millisecond)

    cond do
      stop_fun.() ->
        :ok

      now >= deadline ->
        :ok

      true ->
        slice = min(100, deadline - now)

        case Honker.wait_for_update(db, slice) do
          :changed -> :ok
          _ -> do_wait_update_or_timeout(db, deadline, stop_fun)
        end
    end
  end
end
