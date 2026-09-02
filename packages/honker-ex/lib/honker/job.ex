defmodule Honker.Job do
  @moduledoc """
  A claimed unit of work. Use the lifecycle helpers
  (`ack/2`, `retry/4`, `fail/3`, `heartbeat/3`) to close it out.

  The struct itself is a plain data carrier — all state lives in the
  database; Elixir just holds a copy of the row as it stood at claim
  time. To read a job you did *not* claim, use `Honker.Queue.get_job/2`,
  which returns the read-only `Honker.JobSnapshot`.

  Fields, as the row stood at claim time:

    * `:id`               — row id
    * `:queue`            — queue this job came from
    * `:payload`          — the decoded JSON value
    * `:state`            — `"processing"`
    * `:priority`         — higher runs first within the queue
    * `:run_at`           — unix seconds; when it became claimable
    * `:worker_id`        — the claiming worker
    * `:claim_expires_at` — unix seconds; heartbeat before this
    * `:attempts`         — already incremented by this claim
    * `:max_attempts`     — dead-letters once `:attempts` reaches this
    * `:created_at`       — unix seconds
    * `:expires_at`       — unix seconds, or nil when enqueued without
      `:expires`

  Honker never inspects a payload. Its shape is a contract between the
  app that enqueues and the app that claims — including across
  languages, since another binding may write to the same queue.
  """

  defstruct [
    :id,
    :queue,
    :payload,
    :state,
    :priority,
    :run_at,
    :worker_id,
    :claim_expires_at,
    :attempts,
    :max_attempts,
    :created_at,
    :expires_at
  ]

  @type t :: %__MODULE__{
          id: integer(),
          queue: String.t(),
          payload: term(),
          state: String.t(),
          priority: integer(),
          run_at: integer(),
          worker_id: String.t(),
          claim_expires_at: integer(),
          attempts: integer(),
          max_attempts: integer(),
          created_at: integer(),
          expires_at: integer() | nil
        }

  alias Honker.Database

  @doc "DELETE the row iff the claim hasn't expired. Returns `{:ok, bool}`."
  def ack(%Database{conn: conn}, %__MODULE__{id: id, worker_id: wid}) do
    do_bool(conn, "SELECT honker_ack(?1, ?2)", [id, wid])
  end

  @doc """
  Put the job back with `delay_s`, or move to `_honker_dead` after
  max_attempts. Returns `{:ok, bool}` — true iff the claim was still
  valid (either path qualifies).
  """
  def retry(%Database{conn: conn}, %__MODULE__{id: id, worker_id: wid}, delay_s, error) do
    do_bool(conn, "SELECT honker_retry(?1, ?2, ?3, ?4)", [id, wid, delay_s, error])
  end

  @doc "Unconditionally move the claim to `_honker_dead`."
  def fail(%Database{conn: conn}, %__MODULE__{id: id, worker_id: wid}, error) do
    do_bool(conn, "SELECT honker_fail(?1, ?2, ?3)", [id, wid, error])
  end

  @doc "Extend the visibility timeout by `extend_s` seconds."
  def heartbeat(%Database{conn: conn}, %__MODULE__{id: id, worker_id: wid}, extend_s) do
    do_bool(conn, "SELECT honker_heartbeat(?1, ?2, ?3)", [id, wid, extend_s])
  end

  defp do_bool(conn, sql, params) do
    case Honker.query_first(conn, sql, params) do
      {:ok, [1]} -> {:ok, true}
      {:ok, [0]} -> {:ok, false}
      {:ok, nil} -> {:ok, false}
      other -> other
    end
  end
end
