defmodule Honker.UpdateHub do
  @moduledoc false

  use GenServer

  alias Exqlite.Sqlite3

  @wait_slice_ms 100

  def start(path, extension_path, backend, poll_interval_ms) do
    GenServer.start(__MODULE__, {path, extension_path, backend, poll_interval_ms})
  end

  def close(pid) when is_pid(pid) do
    if Process.alive?(pid) do
      GenServer.call(pid, :close, 5_000)
    else
      :ok
    end
  catch
    :exit, _ -> :ok
  end

  def wait(pid, timeout_ms) when is_pid(pid) do
    timeout_ms = max(0, timeout_ms)
    GenServer.call(pid, {:wait, timeout_ms}, timeout_ms + 1_000)
  end

  def subscribe(pid, subscriber \\ self()) do
    GenServer.call(pid, {:subscribe, subscriber})
  end

  def unsubscribe(pid, subscriber \\ self()) do
    if Process.alive?(pid), do: GenServer.call(pid, {:unsubscribe, subscriber}), else: :ok
  catch
    :exit, _ -> :ok
  end

  @impl true
  def init({path, extension_path, backend, poll_interval_ms}) do
    Process.flag(:trap_exit, true)

    case open_watcher(path, extension_path, backend, poll_interval_ms) do
      {:ok, conn, watcher_id} ->
        hub = self()
        worker = spawn_link(fn -> wait_loop(hub, conn, watcher_id) end)

        {:ok,
         %{
           conn: conn,
           watcher_id: watcher_id,
           worker: worker,
           generation: 0,
           seen: %{},
           waiter_monitors: %{},
           waiters: %{},
           subscribers: %{},
           error: nil,
           closed: false
         }}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def handle_call({:subscribe, subscriber}, _from, state) do
    cond do
      state.error ->
        {:reply, {:error, state.error}, state}

      Map.has_key?(state.subscribers, subscriber) ->
        {:reply, :ok, state}

      true ->
        monitor = Process.monitor(subscriber)
        {:reply, :ok, put_in(state.subscribers[subscriber], monitor)}
    end
  end

  def handle_call({:unsubscribe, subscriber}, _from, state) do
    {:reply, :ok, drop_subscriber(state, subscriber)}
  end

  def handle_call({:wait, timeout_ms}, {pid, _tag} = from, state) do
    state = ensure_waiter_monitor(state, pid)

    cond do
      state.error ->
        {:reply, {:error, state.error}, state}

      state.generation > Map.get(state.seen, pid, 0) ->
        {:reply, :changed, put_in(state.seen[pid], state.generation)}

      timeout_ms == 0 ->
        {:reply, :timeout, state}

      true ->
        ref = make_ref()
        timer = Process.send_after(self(), {:wait_timeout, ref}, timeout_ms)
        waiter = %{from: from, pid: pid, timer: timer}
        {:noreply, put_in(state.waiters[ref], waiter)}
    end
  end

  def handle_call(:close, _from, state) do
    state = shutdown(state, :closed)
    {:stop, :normal, :ok, state}
  end

  @impl true
  def handle_info(:watcher_changed, state) do
    generation = state.generation + 1

    Enum.each(state.subscribers, fn {pid, _monitor} ->
      send(pid, {:honker_update, self(), generation})
    end)

    seen =
      Enum.reduce(state.waiters, state.seen, fn {_ref, waiter}, acc ->
        Process.cancel_timer(waiter.timer)
        GenServer.reply(waiter.from, :changed)
        Map.put(acc, waiter.pid, generation)
      end)

    {:noreply, %{state | generation: generation, seen: seen, waiters: %{}}}
  end

  def handle_info({:watcher_error, reason}, state) do
    Enum.each(state.waiters, fn {_ref, waiter} ->
      Process.cancel_timer(waiter.timer)
      GenServer.reply(waiter.from, {:error, reason})
    end)

    Enum.each(state.subscribers, fn {pid, _monitor} ->
      send(pid, {:honker_update_hub_error, self(), reason})
    end)

    {:noreply, %{state | error: reason, waiters: %{}}}
  end

  def handle_info({:wait_timeout, ref}, state) do
    case Map.pop(state.waiters, ref) do
      {nil, _waiters} ->
        {:noreply, state}

      {waiter, waiters} ->
        GenServer.reply(waiter.from, :timeout)
        {:noreply, %{state | waiters: waiters}}
    end
  end

  def handle_info({:DOWN, monitor, :process, pid, _reason}, state) do
    cond do
      state.subscribers[pid] == monitor ->
        {:noreply, drop_subscriber(state, pid)}

      state.waiter_monitors[pid] == monitor ->
        {:noreply, drop_waiter_process(state, pid)}

      true ->
        {:noreply, state}
    end
  end

  def handle_info({:EXIT, worker, reason}, %{worker: worker} = state) do
    if state.closed or reason == :normal do
      {:noreply, state}
    else
      handle_info({:watcher_error, "honker update watcher exited: #{inspect(reason)}"}, state)
    end
  end

  @impl true
  def terminate(_reason, state) do
    unless state.closed do
      _ = shutdown(state, :closed)
    end

    :ok
  end

  defp open_watcher(path, extension_path, backend, poll_interval_ms) do
    case Sqlite3.open(path) do
      {:ok, conn} ->
        result =
          with :ok <- Sqlite3.execute(conn, "PRAGMA busy_timeout = 5000;"),
               :ok <- Sqlite3.enable_load_extension(conn, true),
               :ok <- Honker.run_bare(conn, "SELECT load_extension(?1)", [extension_path]),
               :ok <- Sqlite3.enable_load_extension(conn, false),
               {:ok, [watcher_id]} <-
                 Honker.query_first(conn, "SELECT honker_update_watcher_open(?1, ?2, ?3)", [
                   path,
                   backend,
                   poll_interval_ms
                 ]) do
            {:ok, conn, watcher_id}
          end

        case result do
          {:ok, _, _} = ok ->
            ok

          error ->
            _ = Sqlite3.enable_load_extension(conn, false)
            _ = Sqlite3.close(conn)
            error
        end

      error ->
        error
    end
  end

  defp wait_loop(hub, conn, watcher_id) do
    receive do
      {:stop, caller} ->
        send(caller, {:watcher_stopped, self()})
    after
      0 ->
        case Honker.query_first(conn, "SELECT honker_update_watcher_wait(?1, ?2)", [
               watcher_id,
               @wait_slice_ms
             ]) do
          {:ok, [1]} ->
            send(hub, :watcher_changed)
            wait_loop(hub, conn, watcher_id)

          {:ok, [0]} ->
            wait_loop(hub, conn, watcher_id)

          {:ok, [-1]} ->
            send(hub, {:watcher_error, "honker update watcher closed or died"})

          error ->
            send(hub, {:watcher_error, inspect(error)})
        end
    end
  end

  defp shutdown(%{closed: true} = state, _reason), do: state

  defp shutdown(state, reason) do
    Enum.each(state.waiters, fn {_ref, waiter} ->
      Process.cancel_timer(waiter.timer)
      GenServer.reply(waiter.from, {:error, reason})
    end)

    Enum.each(state.subscribers, fn {pid, _monitor} ->
      send(pid, {:honker_update_hub_error, self(), reason})
    end)

    Enum.each(state.waiter_monitors, fn {_pid, monitor} ->
      Process.demonitor(monitor, [:flush])
    end)

    send(state.worker, {:stop, self()})

    receive do
      {:watcher_stopped, worker} when worker == state.worker -> :ok
    after
      1_000 -> Process.exit(state.worker, :kill)
    end

    _ =
      Honker.query_first(state.conn, "SELECT honker_update_watcher_close(?1)", [state.watcher_id])

    _ = Sqlite3.close(state.conn)
    %{state | closed: true, waiters: %{}, subscribers: %{}, waiter_monitors: %{}, seen: %{}}
  end

  defp drop_subscriber(state, pid) do
    case Map.pop(state.subscribers, pid) do
      {nil, _} ->
        state

      {monitor, subscribers} ->
        Process.demonitor(monitor, [:flush])
        %{state | subscribers: subscribers}
    end
  end

  defp ensure_waiter_monitor(state, pid) do
    if Map.has_key?(state.waiter_monitors, pid) do
      state
    else
      put_in(state.waiter_monitors[pid], Process.monitor(pid))
    end
  end

  defp drop_waiter_process(state, pid) do
    waiters =
      Enum.reduce(state.waiters, %{}, fn {ref, waiter}, acc ->
        if waiter.pid == pid do
          Process.cancel_timer(waiter.timer)
          acc
        else
          Map.put(acc, ref, waiter)
        end
      end)

    %{
      state
      | seen: Map.delete(state.seen, pid),
        waiter_monitors: Map.delete(state.waiter_monitors, pid),
        waiters: waiters
    }
  end
end
