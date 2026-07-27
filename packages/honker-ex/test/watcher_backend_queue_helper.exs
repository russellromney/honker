defmodule HonkerWatcherBackendQueueHelper do
  @idle_deadline_ms 20_000
  @watch_wait_ms 2_000

  def main(["worker", path, ext, backend, worker_id, ready_path, result_path]) do
    backend = if backend == "", do: nil, else: backend
    {:ok, db} = Honker.open(path, extension_path: ext, watcher_backend: backend)
    File.write!(ready_path, "ready")
    values = drain(db, worker_id, [], idle_deadline())
    Honker.close(db)
    File.write!(result_path, Enum.map_join(values, "\n", &to_string/1))
  end

  def main(["writer", path, ext, first, count]) do
    first = String.to_integer(first)
    count = String.to_integer(count)
    {:ok, db} = Honker.open(path, extension_path: ext)

    for i <- first..(first + count - 1) do
      {:ok, _} = Honker.Queue.enqueue(db, "shared", %{"i" => i})
    end

    Honker.close(db)
  end

  defp drain(db, worker_id, acc, deadline_ms) do
    case Honker.Queue.claim_one(db, "shared", worker_id) do
      {:ok, nil} ->
        case Honker.wait_for_update(db, @watch_wait_ms) do
          :changed ->
            drain(db, worker_id, acc, idle_deadline())

          :timeout ->
            if System.monotonic_time(:millisecond) >= deadline_ms do
              Enum.reverse(acc)
            else
              drain(db, worker_id, acc, deadline_ms)
            end

          {:error, reason} -> raise "watcher failed: #{inspect(reason)}"
        end

      {:ok, job} ->
        {:ok, true} = Honker.Job.ack(db, job)

        if job.payload["stop"] do
          Enum.reverse(acc)
        else
          drain(db, worker_id, [job.payload["i"] | acc], idle_deadline())
        end

      other ->
        raise "claim failed: #{inspect(other)}"
    end
  end

  defp idle_deadline do
    System.monotonic_time(:millisecond) + @idle_deadline_ms
  end
end

System.argv()
|> case do
  ["--" | args] -> args
  args -> args
end
|> HonkerWatcherBackendQueueHelper.main()
