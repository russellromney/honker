defmodule Honker.JobSnapshot do
  @moduledoc """
  A read-only view of a live job row, returned by
  `Honker.Queue.get_job/2`.

  Data only — no `ack`/`retry`/`fail`/`heartbeat`, because the reader
  does not hold the claim. The fields are the same twelve a claimed
  `Honker.Job` carries:

    * `:id`               — row id
    * `:queue`            — queue that owns the row
    * `:payload`          — the raw JSON **text** stored in the row
    * `:state`            — `"pending"` or `"processing"`
    * `:priority`         — higher runs first within the queue
    * `:run_at`           — unix seconds; when it becomes claimable
    * `:worker_id`        — nil until a worker claims the job
    * `:claim_expires_at` — nil until a worker claims the job
    * `:attempts`         — claims made so far
    * `:max_attempts`     — dead-letters once `:attempts` reaches this
    * `:created_at`       — unix seconds
    * `:expires_at`       — unix seconds, or nil when enqueued without
      `:expires`

  NOTE: `:payload` is the raw JSON text, not a decoded value — unlike
  `Honker.Job.payload`, which `Honker.Queue.claim_batch/4` decodes.
  Call `Jason.decode!/1` on it. That difference is inherited from the
  SQL ABI and is left alone here; the bindings do not yet agree on one
  snapshot payload encoding.
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
          payload: String.t(),
          state: String.t(),
          priority: integer(),
          run_at: integer(),
          worker_id: String.t() | nil,
          claim_expires_at: integer() | nil,
          attempts: integer(),
          max_attempts: integer(),
          created_at: integer(),
          expires_at: integer() | nil
        }

  @doc """
  Build a snapshot from a decoded `honker_get_job()` row.

  `Honker.Queue.get_job/2` calls this for you. Reach for it directly
  when you run the SQL yourself — the Ecto path in `Honker.Extension`,
  where Honker is loaded onto a connection you own:

      %{rows: [[raw]]} =
        Ecto.Adapters.SQL.query!(Repo, "SELECT honker_get_job(?)", [job_id])

      snapshot = raw |> Jason.decode!() |> Honker.JobSnapshot.from_row()

  `honker_get_job()` returns the empty string on a miss, so check for
  that before decoding.
  """
  @spec from_row(map()) :: t()
  def from_row(row) when is_map(row) do
    %__MODULE__{
      id: row["id"],
      queue: row["queue"],
      payload: row["payload"],
      state: row["state"],
      priority: row["priority"],
      run_at: row["run_at"],
      worker_id: row["worker_id"],
      claim_expires_at: row["claim_expires_at"],
      attempts: row["attempts"],
      max_attempts: row["max_attempts"],
      created_at: row["created_at"],
      expires_at: row["expires_at"]
    }
  end
end
