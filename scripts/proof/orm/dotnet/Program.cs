// Framework-owned Microsoft.Data.Sqlite, as guides/orm/dotnet.mdx describes.
// docs:start example
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

RunSurface(conn, "dn");
ProveAtomicity(conn);
Console.WriteLine("PASS dotnet-microsoft-data-sqlite");

static string CatalogPath()
{
    var env = Environment.GetEnvironmentVariable("HONKER_ORM_SURFACE");
    if (!string.IsNullOrEmpty(env))
    {
        if (!File.Exists(env)) throw new FileNotFoundException(env);
        return env;
    }
    var cwd = Directory.GetCurrentDirectory();
    for (var dir = cwd; dir != null; dir = Directory.GetParent(dir)?.FullName)
    {
        foreach (var rel in new[] { "surface.json", "scripts/proof/orm/surface.json" })
        {
            var p = Path.Combine(dir, rel);
            if (File.Exists(p)) return p;
        }
    }
    throw new FileNotFoundException("surface.json not found; set HONKER_ORM_SURFACE");
}

static object? Resolve(JsonElement token, string prefix, Dictionary<string, object?> vars)
{
    if (token.ValueKind == JsonValueKind.Null) return null;
    if (token.ValueKind == JsonValueKind.Number) return token.GetInt64();
    if (token.ValueKind != JsonValueKind.String) throw new Exception($"bad arg {token}");
    var s = token.GetString()!;
    if (s.StartsWith("$ns:")) return prefix + "_" + s[4..];
    if (s.StartsWith("$json:"))
    {
        var ids = s[6..].Split(',').Select(k => Convert.ToInt64(vars[k])).ToArray();
        return JsonSerializer.Serialize(ids);
    }
    if (s.StartsWith('$')) return vars[s[1..]];
    return s;
}

static string AsText(object? v) => v is null or DBNull ? "" : Convert.ToString(v) ?? "";

static long AsInt(object? v) => Convert.ToInt64(v);

static string ResolveText(string text, string prefix, Dictionary<string, object?> vars)
{
    var outText = text;
    foreach (var (k, v) in vars) outText = outText.Replace("$" + k, AsText(v));
    return outText.Replace("$ns:", prefix + "_");
}

static object? Scalar(SqliteConnection conn, string sql, IReadOnlyList<object?> args)
{
    using var cmd = conn.CreateCommand();
    var named = sql;
    var n = 0;
    var built = "";
    foreach (var ch in named)
    {
        if (ch == '?')
        {
            built += $"$p{n}";
            n++;
        }
        else built += ch;
    }
    cmd.CommandText = built;
    for (var i = 0; i < args.Count; i++)
        cmd.Parameters.AddWithValue($"$p{i}", args[i] ?? DBNull.Value);
    var result = cmd.ExecuteScalar();
    return result is DBNull ? null : result;
}

static void Check(JsonElement expect, object? result, string prefix, Dictionary<string, object?> vars)
{
    var kind = expect.GetProperty("kind").GetString();
    switch (kind)
    {
        case "int_gt":
            if (!(AsInt(result) > expect.GetProperty("n").GetInt64())) throw new Exception($"got {result}");
            break;
        case "int_eq":
            if (AsInt(result) != expect.GetProperty("n").GetInt64()) throw new Exception($"got {result}");
            break;
        case "int_ge":
            if (AsInt(result) < expect.GetProperty("n").GetInt64()) throw new Exception($"got {result}");
            break;
        case "int_gt_ref":
            if (!(AsInt(result) > AsInt(vars[expect.GetProperty("ref").GetString()!]))) throw new Exception($"got {result}");
            break;
        case "eq_ref":
            if (AsInt(result) != AsInt(vars[expect.GetProperty("ref").GetString()!])) throw new Exception($"got {result}");
            break;
        case "json_len":
            if (JsonDocument.Parse(AsText(result)).RootElement.GetArrayLength() != expect.GetProperty("n").GetInt32())
                throw new Exception($"got {result}");
            break;
        case "json_id_eq_ref":
        {
            using var doc = JsonDocument.Parse(AsText(result));
            if (doc.RootElement.GetArrayLength() != 1) throw new Exception($"got {result}");
            if (doc.RootElement[0].GetProperty("id").GetInt64() != AsInt(vars[expect.GetProperty("ref").GetString()!]))
                throw new Exception($"got {result}");
            break;
        }
        case "contains":
        {
            var needle = ResolveText(expect.GetProperty("s").GetString()!, prefix, vars);
            if (!AsText(result).Contains(needle)) throw new Exception($"{needle} not in {result}");
            break;
        }
        case "empty_string":
            if (AsText(result) != "") throw new Exception($"expected empty string, got {result}");
            break;
        case "is_null":
            if (result is not null) throw new Exception($"expected NULL, got {result}");
            break;
        default:
            throw new Exception($"unknown expect kind {kind}");
    }
}

static void RunSurface(SqliteConnection conn, string prefix)
{
    using var doc = JsonDocument.Parse(File.ReadAllText(CatalogPath()));
    var vars = new Dictionary<string, object?>();
    foreach (var step in doc.RootElement.GetProperty("steps").EnumerateArray())
    {
        var id = step.GetProperty("id").GetString()!;
        var sql = step.GetProperty("sql").GetString()!;
        var args = step.GetProperty("args").EnumerateArray().Select(a => Resolve(a, prefix, vars)).ToList();
        object? result;
        try { result = Scalar(conn, sql, args); }
        catch (Exception ex) { throw new Exception($"{id} failed: {ex.Message}", ex); }
        if (step.TryGetProperty("store", out var store) && store.GetString() is { } name)
            vars[name] = result;
        if (step.TryGetProperty("expect", out var expect))
        {
            try { Check(expect, result, prefix, vars); }
            catch (Exception ex) { throw new Exception($"{id}: {ex.Message}", ex); }
        }
    }
}

static void ProveAtomicity(SqliteConnection conn)
{
    using (var cmd = conn.CreateCommand())
    {
        cmd.CommandText = "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER)";
        cmd.ExecuteNonQuery();
    }

    using (var tx = conn.BeginTransaction())
    {
        using (var ins = conn.CreateCommand())
        {
            ins.Transaction = tx;
            ins.CommandText = "INSERT INTO orders (id, user_id) VALUES ($id, $uid)";
            ins.Parameters.AddWithValue("$id", 42);
            ins.Parameters.AddWithValue("$uid", 1);
            ins.ExecuteNonQuery();
        }
        long jobId;
        using (var enq = conn.CreateCommand())
        {
            enq.Transaction = tx;
            enq.CommandText = "SELECT honker_enqueue($q, $p, NULL, NULL, $prio, $max, NULL)";
            enq.Parameters.AddWithValue("$q", "dn_atomic");
            enq.Parameters.AddWithValue("$p", JsonSerializer.Serialize(new { order_id = 42 }));
            enq.Parameters.AddWithValue("$prio", 0);
            enq.Parameters.AddWithValue("$max", 3);
            jobId = Convert.ToInt64(enq.ExecuteScalar());
        }
        tx.Commit();

        using var check = conn.CreateCommand();
        check.CommandText = "SELECT COUNT(*) FROM orders WHERE id = 42";
        if (Convert.ToInt64(check.ExecuteScalar()) != 1) throw new Exception("missing committed order");
        using var job = conn.CreateCommand();
        job.CommandText = "SELECT honker_get_job($id)";
        job.Parameters.AddWithValue("$id", jobId);
        if (!AsText(job.ExecuteScalar()).Contains("order_id")) throw new Exception("missing committed job");
    }

    long rolled;
    using (var tx = conn.BeginTransaction())
    {
        using (var ins = conn.CreateCommand())
        {
            ins.Transaction = tx;
            ins.CommandText = "INSERT INTO orders (id, user_id) VALUES ($id, $uid)";
            ins.Parameters.AddWithValue("$id", 43);
            ins.Parameters.AddWithValue("$uid", 1);
            ins.ExecuteNonQuery();
        }
        using (var enq = conn.CreateCommand())
        {
            enq.Transaction = tx;
            enq.CommandText = "SELECT honker_enqueue($q, $p, NULL, NULL, $prio, $max, NULL)";
            enq.Parameters.AddWithValue("$q", "dn_atomic");
            enq.Parameters.AddWithValue("$p", JsonSerializer.Serialize(new { order_id = 43 }));
            enq.Parameters.AddWithValue("$prio", 0);
            enq.Parameters.AddWithValue("$max", 3);
            rolled = Convert.ToInt64(enq.ExecuteScalar());
        }
        tx.Rollback();
    }

    using (var check = conn.CreateCommand())
    {
        check.CommandText = "SELECT COUNT(*) FROM orders WHERE id = 43";
        if (Convert.ToInt64(check.ExecuteScalar()) != 0) throw new Exception("rollback left an order");
    }
    using (var job = conn.CreateCommand())
    {
        job.CommandText = "SELECT honker_get_job($id)";
        job.Parameters.AddWithValue("$id", rolled);
        if (AsText(job.ExecuteScalar()) != "") throw new Exception("rollback left a job");
    }
}
// docs:end example
