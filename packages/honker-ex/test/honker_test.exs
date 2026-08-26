defmodule HonkerWatcherBackendOptionTest do
  use ExUnit.Case, async: true

  @candidates [
    "target/debug/libhonker_ext.dylib",
    "target/debug/libhonker_ext.so",
    "target/debug/libhonker_extension.dylib",
    "target/debug/libhonker_extension.so",
    "target/release/libhonker_ext.dylib",
    "target/release/libhonker_ext.so",
    "target/release/libhonker_extension.dylib",
    "target/release/libhonker_extension.so"
  ]

  @repo_root Path.expand("../../..", __DIR__)

  defp find_extension do
    Enum.find_value(@candidates, fn rel ->
      p = Path.join(@repo_root, rel)
      if File.exists?(p), do: p, else: nil
    end)
  end

  test "watcher backends detect commits" do
    ext = find_extension() || flunk("honker extension not built")

    for backend <- [
          nil,
          "",
          "poll",
          "polling",
          "kernel",
          "kernel-watcher",
          "shm",
          "shm-fast-path"
        ] do
      dir =
        Path.join(System.tmp_dir!(), "honker-ex-watchers-#{System.unique_integer([:positive])}")

      File.mkdir_p!(dir)
      path = Path.join(dir, "t.db")

      case Honker.open(path, extension_path: ext, watcher_backend: backend) do
        {:ok, db} ->
          {:ok, writer} = Honker.open(path, extension_path: ext)
          {:ok, _} = Honker.notify(writer, "backend", %{backend: backend})
          assert :changed = Honker.wait_for_update(db, 2_000)
          Honker.close(writer)
          Honker.close(db)

        {:error, message} ->
          unless to_string(message) =~ "requires the" or
                   to_string(message) =~ "-shm unavailable" or
                   to_string(message) =~ "unsupported SQLite layout" do
            flunk("backend #{inspect(backend)} failed unexpectedly: #{inspect(message)}")
          end
      end
    end
  end

  test "custom watcher poll interval detects commits" do
    ext = find_extension() || flunk("honker extension not built")

    dir =
      Path.join(
        System.tmp_dir!(),
        "honker-ex-watch-interval-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(dir)
    path = Path.join(dir, "t.db")

    {:ok, db} = Honker.open(path, extension_path: ext, watcher_poll_interval_ms: 25)
    {:ok, writer} = Honker.open(path, extension_path: ext)
    {:ok, _} = Honker.notify(writer, "interval", %{ok: true})
    assert :changed = Honker.wait_for_update(db, 2_000)
    Honker.close(writer)
    Honker.close(db)
  end

  test "accepts polling watcher backend aliases before opening sqlite" do
    for backend <- [nil, "", "poll", "polling"] do
      assert {:error, reason} =
               Honker.open("/tmp/honker-ex-missing.db",
                 extension_path: "/missing/libhonker_ext.so",
                 watcher_backend: backend
               )

      refute to_string(reason) =~ "watcher backend"
    end
  end

  test "rejects unknown watcher backend before opening sqlite" do
    ext = find_extension() || flunk("honker extension not built")

    for backend <- ["bogus", "KERNEL", " polling ", :polling] do
      assert {:error, message} =
               Honker.open("/tmp/honker-ex-missing.db",
                 extension_path: ext,
                 watcher_backend: backend
               )

      assert message =~ "unknown watcher backend"
    end
  end
end

defmodule HonkerWatcherBackendQueueTest do
  use ExUnit.Case, async: false

  @candidates [
    "target/debug/libhonker_ext.dylib",
    "target/debug/libhonker_ext.so",
    "target/debug/libhonker_extension.dylib",
    "target/debug/libhonker_extension.so",
    "target/release/libhonker_ext.dylib",
    "target/release/libhonker_ext.so",
    "target/release/libhonker_extension.dylib",
    "target/release/libhonker_extension.so"
  ]

  @repo_root Path.expand("../../..", __DIR__)
  @backends [nil, "kernel", "shm"]

  defp find_extension do
    Enum.find_value(@candidates, fn rel ->
      p = Path.join(@repo_root, rel)
      if File.exists?(p), do: p, else: nil
    end)
  end

  defp unavailable?(message) do
    message = to_string(message)

    message =~ "requires the" or
      message =~ "-shm unavailable" or
      message =~ "unsupported SQLite layout"
  end

  defp bootstrap(path, ext, backend) do
    case Honker.open(path, extension_path: ext, watcher_backend: backend) do
      {:ok, db} ->
        Honker.Queue.enqueue(db, "shared", %{"bootstrap" => true})
        {:ok, job} = Honker.Queue.claim_one(db, "shared", "bootstrap")
        {:ok, true} = Honker.Job.ack(db, job)
        Honker.close(db)
        :ok

      {:error, message} ->
        if unavailable?(message),
          do: :skip,
          else: flunk("backend #{inspect(backend)} failed: #{inspect(message)}")
    end
  end

  defp maybe_pin_shm(path, ext, "shm") do
    {:ok, db} = Honker.open(path, extension_path: ext)
    db
  end

  defp maybe_pin_shm(_path, _ext, _backend), do: nil

  defp close_pin(nil), do: :ok
  defp close_pin(db), do: Honker.close(db)

  defp tmp_db do
    dir =
      Path.join(
        System.tmp_dir!(),
        "honker-ex-queue-watchers-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(dir)
    {dir, Path.join(dir, "q.db")}
  end

  defp package_root, do: Path.expand("..", __DIR__)

  defp run_helper(args) do
    code_paths =
      package_root()
      |> Path.join("_build/test/lib/*/ebin")
      |> Path.wildcard()
      |> Enum.flat_map(&["-pa", &1])

    {output, status} =
      System.cmd("elixir", code_paths ++ ["test/watcher_backend_queue_helper.exs", "--" | args],
        cd: package_root(),
        env: [{"MIX_ENV", "test"}],
        stderr_to_stdout: true
      )

    if status != 0 do
      flunk("queue helper failed with #{status}:\n#{output}")
    end
  end

  defp spawn_worker(path, ext, backend, worker_id, dir) do
    ready_path = Path.join(dir, "#{worker_id}.ready")
    result_path = Path.join(dir, "#{worker_id}.result")

    task =
      Task.async(fn ->
        run_helper(["worker", path, ext, backend || "", worker_id, ready_path, result_path])

        result_path
        |> File.read!()
        |> String.split("\n", trim: true)
        |> Enum.map(&String.to_integer/1)
      end)

    {task, ready_path}
  end

  defp enqueue_range(path, ext, first, count) do
    {:ok, db} = Honker.open(path, extension_path: ext)

    for i <- first..(first + count - 1) do
      {:ok, _} = Honker.Queue.enqueue(db, "shared", %{"i" => i})
    end

    Honker.close(db)
  end

  defp enqueue_stops(path, ext, count) do
    {:ok, db} = Honker.open(path, extension_path: ext)

    for _ <- 1..count do
      {:ok, _} = Honker.Queue.enqueue(db, "shared", %{"stop" => true}, priority: -1)
    end

    Honker.close(db)
  end

  defp spawn_writer(path, ext, first, count) do
    Task.async(fn -> run_helper(["writer", path, ext, to_string(first), to_string(count)]) end)
  end

  defp wait_ready(path) do
    if Enum.any?(1..200, fn _ ->
         if File.exists?(path) do
           true
         else
           Process.sleep(25)
           false
         end
       end) do
      :ok
    else
      flunk("worker did not become ready: #{path}")
    end
  end

  defp assert_int_set(values, count) do
    assert Enum.sort(values) == Enum.to_list(0..(count - 1))
  end

  test "queue watcher backends drain 1 writer / 1 worker without fallback polling" do
    ext = find_extension() || flunk("honker extension not built")

    for backend <- @backends do
      {dir, path} = tmp_db()

      try do
        if bootstrap(path, ext, backend) != :skip do
          pin = maybe_pin_shm(path, ext, backend)

          {worker, ready} = spawn_worker(path, ext, backend, "w1", dir)
          wait_ready(ready)
          enqueue_range(path, ext, 0, 25)
          enqueue_stops(path, ext, 1)
          assert_int_set(Task.await(worker, 30_000), 25)
          close_pin(pin)
        end
      after
        File.rm_rf!(dir)
      end
    end
  end

  test "queue watcher backends drain 1 writer / many workers without double-claims" do
    ext = find_extension() || flunk("honker extension not built")

    for backend <- @backends do
      {dir, path} = tmp_db()

      try do
        if bootstrap(path, ext, backend) != :skip do
          pin = maybe_pin_shm(path, ext, backend)

          workers = for i <- 0..2, do: spawn_worker(path, ext, backend, "w#{i}", dir)
          for {_, ready} <- workers, do: wait_ready(ready)
          enqueue_range(path, ext, 0, 60)
          enqueue_stops(path, ext, length(workers))
          results = workers |> Enum.flat_map(fn {worker, _} -> Task.await(worker, 30_000) end)
          assert_int_set(results, 60)
          assert Enum.uniq(results) |> length() == 60
          close_pin(pin)
        end
      after
        File.rm_rf!(dir)
      end
    end
  end

  test "queue watcher backends drain many writers / 1 worker without fallback polling" do
    ext = find_extension() || flunk("honker extension not built")

    for backend <- @backends do
      {dir, path} = tmp_db()

      try do
        if bootstrap(path, ext, backend) != :skip do
          pin = maybe_pin_shm(path, ext, backend)

          {worker, ready} = spawn_worker(path, ext, backend, "solo", dir)
          wait_ready(ready)

          writers =
            for offset <- [0, 20, 40],
                do: spawn_writer(path, ext, offset, 20)

          Enum.each(writers, &Task.await(&1, 30_000))
          enqueue_stops(path, ext, 1)
          assert_int_set(Task.await(worker, 30_000), 60)
          close_pin(pin)
        end
      after
        File.rm_rf!(dir)
      end
    end
  end

  test "listener backends observe a notification from another process" do
    ext = find_extension() || flunk("honker extension not built")

    for backend <- @backends do
      {dir, path} = tmp_db()

      try do
        if bootstrap(path, ext, backend) != :skip do
          pin = maybe_pin_shm(path, ext, backend)
          {:ok, db} = Honker.open(path, extension_path: ext, watcher_backend: backend)
          {:ok, subscription} = Honker.listen(db, "cross-process", fallback_poll_ms: nil)
          ref = subscription.ref

          run_helper(["notify", path, ext, "cross-process", ~s({"source":"child"})])

          assert_receive {:honker_notification, ^ref, notification}, 2_000
          assert notification.payload == %{"source" => "child"}
          :ok = Honker.unlisten(subscription)
          Honker.close(db)
          close_pin(pin)
        end
      after
        File.rm_rf!(dir)
      end
    end
  end
end

defmodule HonkerTest do
  use ExUnit.Case, async: false

  @candidates [
    "target/debug/libhonker_ext.dylib",
    "target/debug/libhonker_ext.so",
    "target/debug/libhonker_extension.dylib",
    "target/debug/libhonker_extension.so",
    "target/release/libhonker_ext.dylib",
    "target/release/libhonker_ext.so",
    "target/release/libhonker_extension.dylib",
    "target/release/libhonker_extension.so"
  ]

  @repo_root Path.expand("../../..", __DIR__)

  defp find_extension do
    Enum.find_value(@candidates, fn rel ->
      p = Path.join(@repo_root, rel)
      if File.exists?(p), do: p, else: nil
    end)
  end

  setup do
    case find_extension() do
      nil ->
        {:skip, "honker extension not built — run cargo build -p honker-extension --release"}

      ext ->
        dir = Path.join(System.tmp_dir!(), "honker-ex-#{System.unique_integer([:positive])}")
        File.mkdir_p!(dir)
        db_path = Path.join(dir, "t.db")
        {:ok, db} = Honker.open(db_path, extension_path: ext)

        on_exit(fn ->
          Honker.close(db)
          File.rm_rf!(dir)
        end)

        {:ok, %{db: db, ext: ext, db_path: db_path}}
    end
  end

  test "enqueue / claim / ack round-trips", ctx do
    db = ctx.db
    {:ok, id} = Honker.Queue.enqueue(db, "emails", %{"to" => "alice@example.com"})
    assert id > 0

    {:ok, job} = Honker.Queue.claim_one(db, "emails", "worker-1")
    assert job.id == id
    assert job.attempts == 1
    assert job.payload["to"] == "alice@example.com"

    assert {:ok, true} = Honker.Job.ack(db, job)
    assert {:ok, nil} = Honker.Queue.claim_one(db, "emails", "worker-1")
  end

  test "retry-to-dead after max_attempts", ctx do
    db = ctx.db
    db = Honker.configure_queue(db, "retries", max_attempts: 2)

    {:ok, _} = Honker.Queue.enqueue(db, "retries", %{"i" => 1})

    {:ok, job1} = Honker.Queue.claim_one(db, "retries", "w")
    assert {:ok, true} = Honker.Job.retry(db, job1, 0, "first")

    {:ok, job2} = Honker.Queue.claim_one(db, "retries", "w")
    assert job2.attempts == 2
    assert {:ok, true} = Honker.Job.retry(db, job2, 0, "second")

    assert {:ok, nil} = Honker.Queue.claim_one(db, "retries", "w")

    {:ok, [count]} =
      Honker.query_first(
        db.conn,
        "SELECT COUNT(*) FROM _honker_dead WHERE queue='retries'",
        []
      )

    assert count == 1
  end

  test "notify inserts into _honker_notifications", ctx do
    db = ctx.db
    {:ok, id} = Honker.notify(db, "orders", %{"id" => 42})
    assert id > 0

    {:ok, [channel, payload]} =
      Honker.query_first(
        db.conn,
        "SELECT channel, payload FROM _honker_notifications WHERE id = ?1",
        [id]
      )

    assert channel == "orders"
    assert Jason.decode!(payload) == %{"id" => 42}
  end

  test "listen is live-only, channel-filtered, and ordered", ctx do
    db = ctx.db
    {:ok, _} = Honker.notify(db, "orders", %{"id" => "historical"})
    {:ok, subscription} = Honker.listen(db, "orders", fallback_poll_ms: nil)
    ref = subscription.ref

    refute_receive {:honker_notification, ^ref, _}, 50
    {:ok, _} = Honker.notify(db, "other", %{"id" => "wrong-channel"})
    {:ok, _} = Honker.notify(db, "orders", %{"id" => 1})
    {:ok, _} = Honker.notify(db, "orders", %{"id" => 2})

    assert_receive {:honker_notification, ^ref, first}, 2_000
    assert_receive {:honker_notification, ^ref, second}, 2_000
    assert first.payload == %{"id" => 1}
    assert second.payload == %{"id" => 2}
    assert first.channel == "orders"
    assert second.id > first.id
    assert first.created_at > 0
    refute_receive {:honker_notification, ^ref, _}, 50

    assert :ok = Honker.unlisten(subscription)
  end

  test "multiple listeners and wait_for_update all observe one commit", ctx do
    db = ctx.db
    {:ok, first} = Honker.listen(db, "orders", fallback_poll_ms: nil)
    {:ok, second} = Honker.listen(db, "orders", fallback_poll_ms: nil)
    first_ref = first.ref
    second_ref = second.ref

    parent = self()
    waiter = spawn(fn -> send(parent, {:wait_result, Honker.wait_for_update(db, 2_000)}) end)

    {:ok, _} = Honker.notify(db, "orders", %{"id" => 42})

    assert_receive {:honker_notification, ^first_ref, first_notification}, 2_000
    assert_receive {:honker_notification, ^second_ref, second_notification}, 2_000
    assert_receive {:wait_result, :changed}, 2_000
    assert first_notification.id == second_notification.id
    assert first_notification.payload == %{"id" => 42}

    assert :ok = Honker.unlisten(first)
    assert :ok = Honker.unlisten(second)
    refute Process.alive?(waiter)
  end

  test "listener observes commit but not rollback", ctx do
    db = ctx.db
    {:ok, subscription} = Honker.listen(db, "orders", fallback_poll_ms: nil)
    ref = subscription.ref

    {:ok, tx} = Honker.Transaction.begin(db)
    {:ok, _} = Honker.notify_tx(tx, "orders", %{"id" => "rolled-back"})
    :ok = Honker.Transaction.rollback(tx)
    refute_receive {:honker_notification, ^ref, _}, 50

    {:ok, tx2} = Honker.Transaction.begin(db)
    {:ok, _} = Honker.notify_tx(tx2, "orders", %{"id" => "committed"})
    :ok = Honker.Transaction.commit(tx2)

    assert_receive {:honker_notification, ^ref, notification}, 2_000
    assert notification.payload == %{"id" => "committed"}
    assert :ok = Honker.unlisten(subscription)
  end

  test "listener fallback polling does not expose uncommitted rows", ctx do
    db = ctx.db
    {:ok, subscription} = Honker.listen(db, "orders", fallback_poll_ms: 10)
    ref = subscription.ref

    {:ok, tx} = Honker.Transaction.begin(db)
    {:ok, _} = Honker.notify_tx(tx, "orders", %{"id" => "uncommitted"})
    refute_receive {:honker_notification, ^ref, _}, 75
    :ok = Honker.Transaction.rollback(tx)
    refute_receive {:honker_notification, ^ref, _}, 50

    assert :ok = Honker.unlisten(subscription)
  end

  test "listener stops when its subscribing process exits", ctx do
    parent = self()

    owner =
      spawn(fn ->
        {:ok, subscription} = Honker.listen(ctx.db, "orders", fallback_poll_ms: nil)
        send(parent, {:subscription, subscription})
        Process.sleep(:infinity)
      end)

    assert_receive {:subscription, subscription}, 1_000
    listener_monitor = Process.monitor(subscription.pid)
    Process.exit(owner, :kill)
    assert_receive {:DOWN, ^listener_monitor, :process, _pid, _reason}, 1_000
  end

  test "closing a database stops its listeners without reporting an error", ctx do
    {:ok, subscription} = Honker.listen(ctx.db, "orders", fallback_poll_ms: nil)
    ref = subscription.ref
    listener_monitor = Process.monitor(subscription.pid)

    :ok = Honker.close(ctx.db)

    assert_receive {:DOWN, ^listener_monitor, :process, _pid, _reason}, 1_000
    refute_receive {:honker_listener_error, ^ref, _}, 50
  end

  test "unlisten is idempotent", ctx do
    {:ok, subscription} = Honker.listen(ctx.db, "orders")
    assert :ok = Honker.unlisten(subscription)
    assert :ok = Honker.unlisten(subscription)
  end
end
