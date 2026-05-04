defmodule Honker.Outbox do
  @moduledoc """
  Transactional side-effect delivery built on a reserved Honker queue.

  Outbox rows are just queue jobs under `_outbox:<name>`, so
  `enqueue/4` and `enqueue_tx/5` are atomic with the same database
  transactions as normal queue operations.
  """

  alias Honker.{Database, Job, Transaction}

  @enforce_keys [:db, :name, :queue, :delivery, :max_attempts, :base_backoff_s]
  defstruct [:db, :name, :queue, :delivery, :max_attempts, :base_backoff_s]

  def new(%Database{} = db, name, delivery, opts \\ []) when is_function(delivery, 1) do
    vis = Keyword.get(opts, :visibility_timeout_s, 60)
    max = Keyword.get(opts, :max_attempts, 5)
    base = Keyword.get(opts, :base_backoff_s, 5)

    db =
      Honker.configure_queue(db, "_outbox:#{name}", visibility_timeout_s: vis, max_attempts: max)

    %__MODULE__{
      db: db,
      name: name,
      queue: "_outbox:#{name}",
      delivery: delivery,
      max_attempts: max,
      base_backoff_s: base
    }
  end

  def enqueue(%__MODULE__{db: db, queue: queue}, payload, opts \\ []) do
    Honker.Queue.enqueue(db, queue, payload, opts)
  end

  def enqueue_tx(
        %__MODULE__{queue: queue, max_attempts: max},
        %Transaction{} = tx,
        payload,
        opts \\ [],
        queue_opts \\ []
      ) do
    queue_opts = Keyword.put_new(queue_opts, :max_attempts, max)
    Honker.Queue.enqueue_tx(tx, queue, payload, opts, queue_opts)
  end

  @doc """
  Claims and delivers one outbox job. Returns `{:ok, true}` when a job
  was processed and `{:ok, false}` when the queue was empty.
  """
  def run_once(%__MODULE__{} = outbox, worker_id) do
    case Honker.Queue.claim_one(outbox.db, outbox.queue, worker_id) do
      {:ok, nil} ->
        {:ok, false}

      {:ok, job} ->
        deliver(outbox, job)

      other ->
        other
    end
  end

  defp deliver(%__MODULE__{} = outbox, %Job{} = job) do
    try do
      case outbox.delivery.(job.payload) do
        :ok -> ack(outbox, job)
        {:ok, _} -> ack(outbox, job)
        {:error, reason} -> retry(outbox, job, reason)
        other -> if other, do: ack(outbox, job), else: retry(outbox, job, :delivery_failed)
      end
    rescue
      e -> retry(outbox, job, Exception.format(:error, e, __STACKTRACE__))
    end
  end

  defp ack(%__MODULE__{db: db}, job) do
    case Job.ack(db, job) do
      {:ok, true} -> {:ok, true}
      {:ok, false} -> {:error, {:ack_failed, job.id}}
      other -> other
    end
  end

  defp retry(%__MODULE__{db: db} = outbox, job, reason) do
    case Job.retry(db, job, retry_delay(outbox, job.attempts), to_string(reason)) do
      {:ok, true} -> {:ok, true}
      {:ok, false} -> {:error, {:retry_failed, job.id}}
      other -> other
    end
  end

  defp retry_delay(%__MODULE__{base_backoff_s: base}, attempts) do
    cond do
      base <= 0 -> 0
      attempts <= 1 -> base
      true -> ceil(base * :math.pow(2, attempts - 1))
    end
  end
end
