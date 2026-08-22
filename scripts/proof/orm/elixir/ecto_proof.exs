# Ecto, as guides/orm/elixir.mdx shows it: exqlite's :load_extensions
# on the Repo, then honker_bootstrap once at start.
#
# Run with: elixir ecto_proof.exs   (deps supplied by Mix.install)

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

# Every value passed as a bound parameter, never inlined, so Ecto's own
# parameter handling is what gets exercised.
payload = ~s({"to":"alice@example.com"})

%{rows: [[id]]} =
  Ecto.Adapters.SQL.query!(
    Proof.Repo,
    "SELECT honker_enqueue(?, ?, NULL, NULL, ?, ?, NULL)",
    ["emails", payload, 0, 3]
  )

true = is_integer(id) and id > 0

%{rows: [[claimed_json]]} =
  Ecto.Adapters.SQL.query!(
    Proof.Repo,
    "SELECT honker_claim_batch(?, ?, ?, ?)",
    ["emails", "w1", 8, 300]
  )

claimed = Jason.decode!(claimed_json)
1 = length(claimed)
^id = claimed |> hd() |> Map.fetch!("id")

%{rows: [[acked]]} =
  Ecto.Adapters.SQL.query!(Proof.Repo, "SELECT honker_ack(?, ?)", [id, "w1"])

1 = acked

IO.puts("PASS ecto")
