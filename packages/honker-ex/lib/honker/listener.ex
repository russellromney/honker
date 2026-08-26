defmodule Honker.Notification do
  @moduledoc "A live notification delivered by `Honker.listen/3`."
  defstruct [:id, :channel, :payload, :created_at]
end

defmodule Honker.Subscription do
  @moduledoc "An opaque live-notification subscription."
  defstruct [:pid, :ref, :channel]
end

defmodule Honker.Listener do
  @moduledoc false

  use GenServer

  alias Exqlite.Sqlite3
  alias Honker.{Database, Notification, Subscription, UpdateHub}

  def start(%Database{} = db, channel, owner, opts) do
    ref = make_ref()

    case GenServer.start(__MODULE__, {db, to_string(channel), owner, ref, opts}) do
      {:ok, pid} -> {:ok, %Subscription{pid: pid, ref: ref, channel: to_string(channel)}}
      error -> error
    end
  end

  def close(%Subscription{pid: pid}) do
    if Process.alive?(pid), do: GenServer.stop(pid, :normal), else: :ok
  catch
    :exit, _ -> :ok
  end

  @impl true
  def init({db, channel, owner, ref, opts}) do
    fallback_poll_ms = Keyword.get(opts, :fallback_poll_ms, 15_000)

    cond do
      channel == "" ->
        {:stop, "channel must not be empty"}

      !is_nil(fallback_poll_ms) and (!is_integer(fallback_poll_ms) or fallback_poll_ms <= 0) ->
        {:stop, "fallback_poll_ms must be a positive integer or nil"}

      true ->
        with {:ok, read_conn} <- open_read_connection(db.path) do
          result =
            with :ok <- UpdateHub.subscribe(db.update_hub, self()),
                 {:ok, [last_seen]} <-
                   Honker.query_first(
                     read_conn,
                     "SELECT COALESCE(MAX(id), 0) FROM _honker_notifications WHERE channel = ?1",
                     [channel]
                   ) do
              owner_monitor = Process.monitor(owner)
              timer = schedule_fallback(fallback_poll_ms)
              send(self(), :poll)

              {:ok,
               %{
                 db: db,
                 read_conn: read_conn,
                 channel: channel,
                 owner: owner,
                 owner_monitor: owner_monitor,
                 ref: ref,
                 last_seen: last_seen || 0,
                 fallback_poll_ms: fallback_poll_ms,
                 fallback_timer: timer
               }}
            end

          case result do
            {:ok, _state} = ok ->
              ok

            error ->
              _ = Sqlite3.close(read_conn)
              {:stop, error}
          end
        else
          error -> {:stop, error}
        end
    end
  end

  @impl true
  def handle_info(:poll, state), do: poll(state)

  def handle_info({:honker_update, hub, _generation}, %{db: %{update_hub: hub}} = state) do
    poll(state)
  end

  def handle_info(:fallback_poll, state) do
    state = %{state | fallback_timer: schedule_fallback(state.fallback_poll_ms)}
    poll(state)
  end

  def handle_info({:honker_update_hub_error, hub, reason}, %{db: %{update_hub: hub}} = state) do
    if reason != :closed do
      send(state.owner, {:honker_listener_error, state.ref, reason})
    end

    {:stop, :normal, state}
  end

  def handle_info(
        {:DOWN, monitor, :process, owner, _reason},
        %{owner_monitor: monitor, owner: owner} = state
      ) do
    {:stop, :normal, state}
  end

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    if state.fallback_timer, do: Process.cancel_timer(state.fallback_timer)
    _ = UpdateHub.unsubscribe(state.db.update_hub, self())
    _ = Sqlite3.close(state.read_conn)
    :ok
  end

  defp poll(state) do
    sql = """
    SELECT id, channel, payload, created_at
    FROM _honker_notifications
    WHERE channel = ?1 AND id > ?2
    ORDER BY id
    LIMIT 1000
    """

    case Honker.query_all(state.read_conn, sql, [state.channel, state.last_seen]) do
      {:ok, rows} ->
        last_seen =
          Enum.reduce(rows, state.last_seen, fn [id, channel, payload, created_at], _last ->
            notification = %Notification{
              id: id,
              channel: channel,
              payload: decode_payload(payload),
              created_at: created_at
            }

            send(state.owner, {:honker_notification, state.ref, notification})
            id
          end)

        if length(rows) == 1_000, do: send(self(), :poll)
        {:noreply, %{state | last_seen: last_seen}}

      error ->
        send(state.owner, {:honker_listener_error, state.ref, error})
        {:stop, :normal, state}
    end
  end

  defp decode_payload(payload) when is_binary(payload) do
    case Jason.decode(payload) do
      {:ok, decoded} -> decoded
      {:error, _} -> payload
    end
  end

  defp decode_payload(payload), do: payload

  defp schedule_fallback(nil), do: nil
  defp schedule_fallback(ms), do: Process.send_after(self(), :fallback_poll, ms)

  defp open_read_connection(path) do
    with {:ok, conn} <- Sqlite3.open(path) do
      case Sqlite3.execute(conn, "PRAGMA busy_timeout = 5000; PRAGMA query_only = ON;") do
        :ok ->
          {:ok, conn}

        error ->
          _ = Sqlite3.close(conn)
          error
      end
    end
  end
end
