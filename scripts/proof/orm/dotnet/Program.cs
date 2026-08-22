// Framework-owned Microsoft.Data.Sqlite, as guides/orm/dotnet.mdx
// describes for the EF Core case: Honker SQL running through a
// connection the application owns, loaded via HonkerExtension.Locate().
using System.Text.Json;
using Honker;
using Microsoft.Data.Sqlite;

var dbPath = Environment.GetEnvironmentVariable("HONKER_TEST_DB")
    ?? throw new InvalidOperationException("HONKER_TEST_DB is required");

using var conn = new SqliteConnection($"Data Source={dbPath}");
conn.Open();
conn.EnableExtensions(true);
conn.LoadExtension(HonkerExtension.Locate());

using (var boot = conn.CreateCommand())
{
    boot.CommandText = "SELECT honker_bootstrap()";
    boot.ExecuteScalar();
}

// Bound parameters throughout, so Microsoft.Data.Sqlite's own binding
// is what is exercised rather than inlined literals.
long id;
using (var cmd = conn.CreateCommand())
{
    cmd.CommandText = "SELECT honker_enqueue($q, $p, NULL, NULL, $prio, $max, NULL)";
    cmd.Parameters.AddWithValue("$q", "emails");
    cmd.Parameters.AddWithValue("$p", JsonSerializer.Serialize(new { to = "alice@example.com" }));
    cmd.Parameters.AddWithValue("$prio", 0);
    cmd.Parameters.AddWithValue("$max", 3);
    id = Convert.ToInt64(cmd.ExecuteScalar());
}
if (id <= 0) throw new Exception($"expected a job id, got {id}");

string claimedJson;
using (var cmd = conn.CreateCommand())
{
    cmd.CommandText = "SELECT honker_claim_batch($q, $w, $n, $t)";
    cmd.Parameters.AddWithValue("$q", "emails");
    cmd.Parameters.AddWithValue("$w", "w1");
    cmd.Parameters.AddWithValue("$n", 8);
    cmd.Parameters.AddWithValue("$t", 300);
    claimedJson = (string)cmd.ExecuteScalar()!;
}
using var claimed = JsonDocument.Parse(claimedJson);
if (claimed.RootElement.GetArrayLength() != 1)
    throw new Exception($"expected one claimed job, got {claimedJson}");
if (claimed.RootElement[0].GetProperty("id").GetInt64() != id)
    throw new Exception($"claimed the wrong job: {claimedJson}");

using (var cmd = conn.CreateCommand())
{
    cmd.CommandText = "SELECT honker_ack($id, $w)";
    cmd.Parameters.AddWithValue("$id", id);
    cmd.Parameters.AddWithValue("$w", "w1");
    if (Convert.ToInt64(cmd.ExecuteScalar()) != 1)
        throw new Exception("ack must match the claim");
}

Console.WriteLine("PASS dotnet-microsoft-data-sqlite");
