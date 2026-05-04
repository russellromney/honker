defmodule PhaseMantleTest do
  use ExUnit.Case, async: false

  @candidates [
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
        dir = Path.join(System.tmp_dir!(), "honker-mantle-#{System.unique_integer([:positive])}")
        File.mkdir_p!(dir)
        db_path = Path.join(dir, "t.db")
        on_exit(fn -> File.rm_rf!(dir) end)
        {:ok, %{ext: ext, db_path: db_path}}
    end
  end

  test "schedule list round-trips fields", ctx do
    {:ok, db} = Honker.open(ctx.db_path, extension_path: ctx.ext)
    :ok =
      Honker.Scheduler.add(db,
        name: "recap",
        queue: "emails",
        schedule: "0 9 * * 1",
        payload: %{"team" => "premier-league"},
        priority: 3
      )

    :ok =
      Honker.Scheduler.add(db,
        name: "sync",
        queue: "syncs",
        schedule: "@every 1h",
        payload: nil
      )

    {:ok, rows} = Honker.Scheduler.list(db)
    assert length(rows) == 2
    recap = Enum.find(rows, &(&1["name"] == "recap"))
    assert recap["queue"] == "emails"
    assert recap["priority"] == 3
    assert recap["enabled"] == true
    assert Jason.decode!(recap["payload"])["team"] == "premier-league"
  end

  test "pause / resume idempotent", ctx do
    {:ok, db} = Honker.open(ctx.db_path, extension_path: ctx.ext)
    :ok =
      Honker.Scheduler.add(db,
        name: "a",
        queue: "q",
        schedule: "0 9 * * *",
        payload: nil
      )

    assert {:ok, true} = Honker.Scheduler.pause(db, "a")
    assert {:ok, false} = Honker.Scheduler.pause(db, "a")
    assert {:ok, false} = Honker.Scheduler.pause(db, "missing")

    {:ok, rows} = Honker.Scheduler.list(db)
    assert Enum.find(rows, &(&1["name"] == "a"))["enabled"] == false

    assert {:ok, true} = Honker.Scheduler.resume(db, "a")
    assert {:ok, false} = Honker.Scheduler.resume(db, "a")
    {:ok, rows} = Honker.Scheduler.list(db)
    assert Enum.find(rows, &(&1["name"] == "a"))["enabled"] == true
  end

  test "update mutates fields, no-op on empty, recomputes next_fire_at on cron change", ctx do
    {:ok, db} = Honker.open(ctx.db_path, extension_path: ctx.ext)
    :ok =
      Honker.Scheduler.add(db,
        name: "t",
        queue: "q",
        schedule: "0 9 * * *",
        payload: %{"v" => 1}
      )

    assert {:ok, true} = Honker.Scheduler.update(db, "t", payload: %{"v" => 99}, priority: 5)
    {:ok, rows} = Honker.Scheduler.list(db)
    row = Enum.find(rows, &(&1["name"] == "t"))
    assert Jason.decode!(row["payload"])["v"] == 99
    assert row["priority"] == 5

    before = row["next_fire_at"]
    assert {:ok, true} = Honker.Scheduler.update(db, "t", schedule: "*/5 * * * *")
    {:ok, rows} = Honker.Scheduler.list(db)
    row = Enum.find(rows, &(&1["name"] == "t"))
    assert row["cron_expr"] == "*/5 * * * *"
    refute row["next_fire_at"] == before

    assert {:ok, false} = Honker.Scheduler.update(db, "t", [])
    assert {:ok, false} = Honker.Scheduler.update(db, "missing", payload: %{})
  end

  test "queue cancel + get_job", ctx do
    {:ok, db} = Honker.open(ctx.db_path, extension_path: ctx.ext)
    {:ok, id} = Honker.Queue.enqueue(db, "emails", %{"to" => "alice@example.com"})

    {:ok, row} = Honker.Queue.get_job(db, id)
    assert row["queue"] == "emails"
    assert row["state"] == "pending"
    assert row["id"] == id

    assert {:ok, true} = Honker.Queue.cancel(db, id)
    assert {:ok, false} = Honker.Queue.cancel(db, id)
    assert {:ok, nil} = Honker.Queue.get_job(db, id)
    assert {:ok, nil} = Honker.Queue.claim_one(db, "emails", "worker-1")
  end

  test "cancel of processing invalidates ack", ctx do
    {:ok, db} = Honker.open(ctx.db_path, extension_path: ctx.ext)
    {:ok, id} = Honker.Queue.enqueue(db, "emails", %{"to" => "x"})
    {:ok, job} = Honker.Queue.claim_one(db, "emails", "worker-1")
    assert job.id == id

    assert {:ok, true} = Honker.Queue.cancel(db, id)
    assert {:ok, false} = Honker.Job.ack(db, job)
  end

  test "paused schedule does not emit on tick", ctx do
    {:ok, db} = Honker.open(ctx.db_path, extension_path: ctx.ext)
    :ok =
      Honker.Scheduler.add(db,
        name: "due",
        queue: "emails",
        schedule: "@every 1s",
        payload: %{"x" => 1}
      )

    Process.sleep(1100)
    assert {:ok, true} = Honker.Scheduler.pause(db, "due")

    {:ok, fires} = Honker.Scheduler.tick(db)
    assert fires == [], "paused schedule must not emit; got #{inspect(fires)}"

    assert {:ok, true} = Honker.Scheduler.resume(db, "due")
    {:ok, fires2} = Honker.Scheduler.tick(db)
    assert length(fires2) >= 1, "resumed schedule should emit; got #{inspect(fires2)}"
  end

  test "get_job misses after ack (separate from cancel)", ctx do
    {:ok, db} = Honker.open(ctx.db_path, extension_path: ctx.ext)
    {:ok, id} = Honker.Queue.enqueue(db, "emails", %{"to" => "x"})
    {:ok, job} = Honker.Queue.claim_one(db, "emails", "worker-1")
    assert {:ok, true} = Honker.Job.ack(db, job)
    assert {:ok, nil} = Honker.Queue.get_job(db, id)
  end

  test "update payload null vs omitted distinction", ctx do
    {:ok, db} = Honker.open(ctx.db_path, extension_path: ctx.ext)
    :ok =
      Honker.Scheduler.add(db,
        name: "t",
        queue: "q",
        schedule: "0 9 * * *",
        payload: %{"v" => 1}
      )

    # Omitted payload — leaves alone.
    assert {:ok, true} = Honker.Scheduler.update(db, "t", priority: 7)
    {:ok, rows} = Honker.Scheduler.list(db)
    assert Jason.decode!(Enum.at(rows, 0)["payload"])["v"] == 1

    # payload: nil — explicitly write JSON null.
    assert {:ok, true} = Honker.Scheduler.update(db, "t", payload: nil)
    {:ok, rows} = Honker.Scheduler.list(db)
    assert Jason.decode!(Enum.at(rows, 0)["payload"]) == nil
  end
end
