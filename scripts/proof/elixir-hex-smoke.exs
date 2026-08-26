extension_path =
  System.get_env("HONKER_EXTENSION_PATH") ||
    raise "HONKER_EXTENSION_PATH is required"

dir =
  Path.join(
    System.tmp_dir!(),
    "honker-elixir-package-proof-#{System.unique_integer([:positive])}"
  )

File.mkdir_p!(dir)
db_path = Path.join(dir, "app.db")

try do
  {:ok, db} = Honker.open(db_path, extension_path: extension_path)
  {:ok, subscription} = Honker.listen(db, "release-proof", fallback_poll_ms: nil)
  {:ok, _id} = Honker.notify(db, "release-proof", %{"installed_hex" => true})

  receive do
    {:honker_notification, ref, notification} when ref == subscription.ref ->
      unless notification.payload == %{"installed_hex" => true} do
        raise "notification mismatch: #{inspect(notification.payload)}"
      end
  after
    2_000 -> raise "listener timed out"
  end

  monitor = Process.monitor(subscription.pid)
  :ok = Honker.close(db)

  receive do
    {:DOWN, ^monitor, :process, _pid, _reason} -> :ok
  after
    1_000 -> raise "database close did not stop listener"
  end
after
  File.rm_rf!(dir)
end

IO.puts("elixir Hex package smoke ok")
