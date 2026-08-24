# Ecto, as guides/orm/elixir.mdx shows it.

Mix.install([
  {:ecto_sql, "~> 3.12"},
  {:ecto_sqlite3, "~> 0.17"},
  {:jason, "~> 1.4"}
])

db = System.fetch_env!("HONKER_TEST_DB")
ext = Honker.Extension.path!()

defmodule Proof.Repo do
  use Ecto.Repo, otp_app: :proof, adapter: Ecto.Adapters.SQLite3
end

Application.put_env(:proof, Proof.Repo,
  database: db,
  load_extensions: [ext],
  pool_size: 1
)

{:ok, _} = Proof.Repo.start_link()
Ecto.Adapters.SQL.query!(Proof.Repo, "SELECT honker_bootstrap()", [])

catalog_path =
  System.get_env("HONKER_ORM_SURFACE") ||
    Path.expand("../surface.json", Path.dirname(__ENV__.file))

catalog = catalog_path |> File.read!() |> Jason.decode!()

defmodule Surface do
  def as_int(v) when is_integer(v), do: v
  def as_int(v) when is_float(v), do: trunc(v)
  def as_int(v) when is_binary(v), do: String.to_integer(v)
  def as_int(v), do: raise("expected int, got #{inspect(v)}")

  def as_text(nil), do: ""
  def as_text(v) when is_binary(v), do: v
  def as_text(v), do: to_string(v)

  def resolve("$ns:" <> name, prefix, _vars), do: prefix <> "_" <> name

  def resolve("$json:" <> keys, _prefix, vars) do
    keys
    |> String.split(",")
    |> Enum.map(fn k -> as_int(Map.fetch!(vars, k)) end)
    |> Jason.encode!()
  end

  def resolve("$" <> name, _prefix, vars), do: Map.fetch!(vars, name)
  def resolve(token, _prefix, _vars), do: token

  def resolve_text(text, prefix, vars) do
    Enum.reduce(vars, text, fn {k, v}, acc ->
      String.replace(acc, "$" <> k, as_text(v))
    end)
    |> String.replace("$ns:", prefix <> "_")
  end

  def check(%{"kind" => "int_gt", "n" => n}, result, _, _) do
    true = as_int(result) > n
  end

  def check(%{"kind" => "int_eq", "n" => n}, result, _, _) do
    true = as_int(result) == n
  end

  def check(%{"kind" => "int_ge", "n" => n}, result, _, _) do
    true = as_int(result) >= n
  end

  def check(%{"kind" => "int_gt_ref", "ref" => ref}, result, _, vars) do
    true = as_int(result) > as_int(Map.fetch!(vars, ref))
  end

  def check(%{"kind" => "eq_ref", "ref" => ref}, result, _, vars) do
    true = as_int(result) == as_int(Map.fetch!(vars, ref))
  end

  def check(%{"kind" => "json_len", "n" => n}, result, _, _) do
    true = length(Jason.decode!(as_text(result))) == n
  end

  def check(%{"kind" => "json_id_eq_ref", "ref" => ref}, result, _, vars) do
    [job] = Jason.decode!(as_text(result))
    true = as_int(job["id"]) == as_int(Map.fetch!(vars, ref))
  end

  def check(%{"kind" => "contains", "s" => s}, result, prefix, vars) do
    true = String.contains?(as_text(result), resolve_text(s, prefix, vars))
  end

  def check(%{"kind" => "empty_string"}, result, _, _) do
    true = as_text(result) == ""
  end

  def check(%{"kind" => "is_null"}, result, _, _) do
    true = is_nil(result)
  end

  def run(catalog, prefix) do
    Enum.reduce(catalog["steps"], %{}, fn step, vars ->
      args = Enum.map(step["args"], &resolve(&1, prefix, vars))

      %{rows: [[result]]} =
        try do
          Ecto.Adapters.SQL.query!(Proof.Repo, step["sql"], args)
        rescue
          e -> raise "#{step["id"]} failed: #{Exception.message(e)}"
        end

      vars = if step["store"], do: Map.put(vars, step["store"], result), else: vars

      if step["expect"] do
        try do
          check(step["expect"], result, prefix, vars)
        rescue
          e -> raise "#{step["id"]}: #{Exception.message(e)}"
        end
      end

      vars
    end)
  end
end

Surface.run(catalog, "ec")

Ecto.Adapters.SQL.query!(
  Proof.Repo,
  "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER)",
  []
)

{:ok, job_id} =
  Proof.Repo.transaction(fn ->
    Ecto.Adapters.SQL.query!(Proof.Repo, "INSERT INTO orders (id, user_id) VALUES (?, ?)", [42, 1])

    %{rows: [[id]]} =
      Ecto.Adapters.SQL.query!(
        Proof.Repo,
        "SELECT honker_enqueue(?, ?, NULL, NULL, ?, ?, NULL)",
        ["ec_atomic", ~s({"order_id":42}), 0, 3]
      )

    id
  end)

%{rows: [[1]]} = Ecto.Adapters.SQL.query!(Proof.Repo, "SELECT COUNT(*) FROM orders WHERE id = ?", [42])
%{rows: [[job]]} = Ecto.Adapters.SQL.query!(Proof.Repo, "SELECT honker_get_job(?)", [job_id])
true = is_binary(job) and String.contains?(job, "order_id")

rolled =
  case Proof.Repo.transaction(fn ->
         Ecto.Adapters.SQL.query!(Proof.Repo, "INSERT INTO orders (id, user_id) VALUES (?, ?)", [43, 1])

         %{rows: [[id]]} =
           Ecto.Adapters.SQL.query!(
             Proof.Repo,
             "SELECT honker_enqueue(?, ?, NULL, NULL, ?, ?, NULL)",
             ["ec_atomic", ~s({"order_id":43}), 0, 3]
           )

         Proof.Repo.rollback({:rollback, id})
       end) do
    {:error, {:rollback, id}} -> id
  end

%{rows: [[0]]} = Ecto.Adapters.SQL.query!(Proof.Repo, "SELECT COUNT(*) FROM orders WHERE id = ?", [43])
%{rows: [[""]]} = Ecto.Adapters.SQL.query!(Proof.Repo, "SELECT honker_get_job(?)", [rolled])

IO.puts("PASS ecto")
