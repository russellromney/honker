// Framework-owned sqlite3, as guides/orm/cpp.mdx shows it.
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <nlohmann/json.hpp>
#include <stdexcept>
#include <sqlite3.h>
#include <string>
#include <unordered_map>
#include <vector>

using json = nlohmann::json;

static void must(int rc, sqlite3* db, const char* what) {
    if (rc != SQLITE_OK && rc != SQLITE_ROW && rc != SQLITE_DONE) {
        std::fprintf(stderr, "%s: %s\n", what, db ? sqlite3_errmsg(db) : "(no db)");
        std::exit(1);
    }
}

static std::string catalog_path() {
    if (const char* env = std::getenv("HONKER_ORM_SURFACE")) {
        return env;
    }
    if (std::ifstream("surface.json")) return "surface.json";
    if (std::ifstream("scripts/proof/orm/surface.json")) return "scripts/proof/orm/surface.json";
    std::fprintf(stderr, "surface.json not found; set HONKER_ORM_SURFACE\n");
    std::exit(1);
}

static std::string as_text(const json& v) {
    if (v.is_null()) return "";
    if (v.is_string()) return v.get<std::string>();
    if (v.is_number_integer()) return std::to_string(v.get<long long>());
    return v.dump();
}

static long long as_int(const json& v) {
    if (v.is_number_integer()) return v.get<long long>();
    if (v.is_number_float()) return static_cast<long long>(v.get<double>());
    if (v.is_string()) return std::stoll(v.get<std::string>());
    std::fprintf(stderr, "expected int, got %s\n", v.dump().c_str());
    std::exit(1);
}

static json resolve(const json& token, const std::string& prefix,
                    const std::unordered_map<std::string, json>& vars) {
    if (!token.is_string()) return token;
    auto s = token.get<std::string>();
    if (s.rfind("$ns:", 0) == 0) return prefix + "_" + s.substr(4);
    if (s.rfind("$json:", 0) == 0) {
        json ids = json::array();
        std::string keys = s.substr(6);
        size_t start = 0;
        while (start <= keys.size()) {
            auto comma = keys.find(',', start);
            auto key = keys.substr(start, comma == std::string::npos ? std::string::npos : comma - start);
            ids.push_back(as_int(vars.at(key)));
            if (comma == std::string::npos) break;
            start = comma + 1;
        }
        return ids.dump();
    }
    if (!s.empty() && s[0] == '$') return vars.at(s.substr(1));
    return s;
}

static std::string resolve_text(std::string text, const std::string& prefix,
                                const std::unordered_map<std::string, json>& vars) {
    for (const auto& [k, v] : vars) {
        auto needle = "$" + k;
        size_t pos = 0;
        auto repl = as_text(v);
        while ((pos = text.find(needle, pos)) != std::string::npos) {
            text.replace(pos, needle.size(), repl);
            pos += repl.size();
        }
    }
    auto ns = std::string("$ns:");
    size_t pos = 0;
    auto repl = prefix + "_";
    while ((pos = text.find(ns, pos)) != std::string::npos) {
        text.replace(pos, ns.size(), repl);
        pos += repl.size();
    }
    return text;
}

static json scalar(sqlite3* db, const std::string& sql, const std::vector<json>& args) {
    sqlite3_stmt* stmt = nullptr;
    must(sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr), db, sql.c_str());
    for (int i = 0; i < static_cast<int>(args.size()); i++) {
        const auto& a = args[i];
        if (a.is_null()) {
            sqlite3_bind_null(stmt, i + 1);
        } else if (a.is_number_integer()) {
            sqlite3_bind_int64(stmt, i + 1, a.get<long long>());
        } else if (a.is_string()) {
            auto s = a.get<std::string>();
            sqlite3_bind_text(stmt, i + 1, s.c_str(), -1, SQLITE_TRANSIENT);
        } else {
            std::fprintf(stderr, "cannot bind %s\n", a.dump().c_str());
            std::exit(1);
        }
    }
    int rc = sqlite3_step(stmt);
    json out = nullptr;
    if (rc == SQLITE_ROW) {
        switch (sqlite3_column_type(stmt, 0)) {
            case SQLITE_INTEGER:
                out = sqlite3_column_int64(stmt, 0);
                break;
            case SQLITE_FLOAT:
                out = static_cast<long long>(sqlite3_column_double(stmt, 0));
                break;
            case SQLITE_NULL:
                out = nullptr;
                break;
            default: {
                const unsigned char* t = sqlite3_column_text(stmt, 0);
                out = t ? reinterpret_cast<const char*>(t) : "";
                break;
            }
        }
    } else if (rc != SQLITE_DONE) {
        std::fprintf(stderr, "step: %s\n", sqlite3_errmsg(db));
        std::exit(1);
    }
    sqlite3_finalize(stmt);
    return out;
}

static void check(const json& expect, const json& result, const std::string& prefix,
                  const std::unordered_map<std::string, json>& vars) {
    auto kind = expect.at("kind").get<std::string>();
    if (kind == "int_gt") {
        if (!(as_int(result) > expect.at("n").get<long long>())) {
            throw std::runtime_error("got " + result.dump());
        }
    } else if (kind == "int_eq") {
        if (as_int(result) != expect.at("n").get<long long>()) {
            throw std::runtime_error("got " + result.dump());
        }
    } else if (kind == "int_ge") {
        if (as_int(result) < expect.at("n").get<long long>()) {
            throw std::runtime_error("got " + result.dump());
        }
    } else if (kind == "int_gt_ref") {
        if (!(as_int(result) > as_int(vars.at(expect.at("ref").get<std::string>())))) {
            throw std::runtime_error("got " + result.dump());
        }
    } else if (kind == "eq_ref") {
        if (as_int(result) != as_int(vars.at(expect.at("ref").get<std::string>()))) {
            throw std::runtime_error("got " + result.dump());
        }
    } else if (kind == "json_len") {
        auto parsed = json::parse(as_text(result));
        if (static_cast<int>(parsed.size()) != expect.at("n").get<int>()) {
            throw std::runtime_error("got " + result.dump());
        }
    } else if (kind == "json_id_eq_ref") {
        auto parsed = json::parse(as_text(result));
        auto refn = as_int(vars.at(expect.at("ref").get<std::string>()));
        if (parsed.size() != 1 || as_int(parsed[0].at("id")) != refn) {
            throw std::runtime_error("got " + result.dump());
        }
    } else if (kind == "contains") {
        auto needle = resolve_text(expect.at("s").get<std::string>(), prefix, vars);
        if (as_text(result).find(needle) == std::string::npos) {
            throw std::runtime_error(needle + " not in " + result.dump());
        }
    } else if (kind == "empty_string") {
        if (as_text(result) != "") {
            throw std::runtime_error("expected empty string, got " + result.dump());
        }
    } else if (kind == "is_null") {
        if (!result.is_null()) {
            throw std::runtime_error("expected NULL, got " + result.dump());
        }
    } else {
        throw std::runtime_error("unknown expect kind " + kind);
    }
}

int main() {
    const char* db_path = std::getenv("HONKER_TEST_DB");
    const char* ext = std::getenv("HONKER_EXTENSION_PATH");
    if (!db_path || !ext) {
        std::fprintf(stderr, "HONKER_TEST_DB and HONKER_EXTENSION_PATH are required\n");
        return 1;
    }

    sqlite3* db = nullptr;
    must(sqlite3_open(db_path, &db), db, "open");
    must(sqlite3_enable_load_extension(db, 1), db, "enable_load_extension");
    char* err = nullptr;
    if (sqlite3_load_extension(db, ext, nullptr, &err) != SQLITE_OK) {
        std::fprintf(stderr, "load_extension: %s\n", err ? err : "(unknown)");
        return 1;
    }
    must(sqlite3_exec(db, "SELECT honker_bootstrap()", nullptr, nullptr, nullptr), db, "bootstrap");

    std::ifstream in(catalog_path());
    json catalog = json::parse(in);
    std::unordered_map<std::string, json> vars;
    for (const auto& step : catalog.at("steps")) {
        auto id = step.at("id").get<std::string>();
        std::vector<json> args;
        for (const auto& a : step.at("args")) {
            args.push_back(resolve(a, "cp", vars));
        }
        json result;
        try {
            result = scalar(db, step.at("sql").get<std::string>(), args);
        } catch (const std::exception& e) {
            std::fprintf(stderr, "%s failed: %s\n", id.c_str(), e.what());
            return 1;
        }
        if (step.contains("store")) {
            vars[step.at("store").get<std::string>()] = result;
        }
        if (step.contains("expect")) {
            try {
                check(step.at("expect"), result, "cp", vars);
            } catch (const std::exception& e) {
                std::fprintf(stderr, "%s: %s\n", id.c_str(), e.what());
                return 1;
            }
        }
    }

    must(sqlite3_exec(db, "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER)",
                      nullptr, nullptr, nullptr),
         db, "create orders");
    must(sqlite3_exec(db, "BEGIN IMMEDIATE", nullptr, nullptr, nullptr), db, "begin");
    must(sqlite3_exec(db, "INSERT INTO orders (id, user_id) VALUES (42, 1)", nullptr, nullptr, nullptr),
         db, "insert");
    sqlite3_stmt* stmt = nullptr;
    must(sqlite3_prepare_v2(db, "SELECT honker_enqueue(?, ?, NULL, NULL, ?, ?, NULL)", -1, &stmt, nullptr),
         db, "enqueue");
    sqlite3_bind_text(stmt, 1, "cp_atomic", -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, R"({"order_id":42})", -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(stmt, 3, 0);
    sqlite3_bind_int64(stmt, 4, 3);
    if (sqlite3_step(stmt) != SQLITE_ROW) {
        std::fprintf(stderr, "enqueue: %s\n", sqlite3_errmsg(db));
        return 1;
    }
    sqlite3_int64 committed = sqlite3_column_int64(stmt, 0);
    sqlite3_finalize(stmt);
    must(sqlite3_exec(db, "COMMIT", nullptr, nullptr, nullptr), db, "commit");

    sqlite3_int64 n = 0;
    must(sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM orders WHERE id = 42", -1, &stmt, nullptr), db, "count");
    sqlite3_step(stmt);
    n = sqlite3_column_int64(stmt, 0);
    sqlite3_finalize(stmt);
    if (n != 1) {
        std::fprintf(stderr, "missing committed order\n");
        return 1;
    }

    must(sqlite3_exec(db, "BEGIN IMMEDIATE", nullptr, nullptr, nullptr), db, "begin rollback");
    must(sqlite3_exec(db, "INSERT INTO orders (id, user_id) VALUES (43, 1)", nullptr, nullptr, nullptr),
         db, "insert rollback");
    must(sqlite3_prepare_v2(db, "SELECT honker_enqueue(?, ?, NULL, NULL, ?, ?, NULL)", -1, &stmt, nullptr),
         db, "enqueue rollback");
    sqlite3_bind_text(stmt, 1, "cp_atomic", -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, R"({"order_id":43})", -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(stmt, 3, 0);
    sqlite3_bind_int64(stmt, 4, 3);
    sqlite3_step(stmt);
    sqlite3_int64 rolled = sqlite3_column_int64(stmt, 0);
    sqlite3_finalize(stmt);
    must(sqlite3_exec(db, "ROLLBACK", nullptr, nullptr, nullptr), db, "rollback");

    must(sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM orders WHERE id = 43", -1, &stmt, nullptr), db, "count rb");
    sqlite3_step(stmt);
    n = sqlite3_column_int64(stmt, 0);
    sqlite3_finalize(stmt);
    if (n != 0) {
        std::fprintf(stderr, "rollback left an order\n");
        return 1;
    }
    must(sqlite3_prepare_v2(db, "SELECT honker_get_job(?)", -1, &stmt, nullptr), db, "get rolled");
    sqlite3_bind_int64(stmt, 1, rolled);
    sqlite3_step(stmt);
    const unsigned char* job = sqlite3_column_text(stmt, 0);
    std::string job_s = job ? reinterpret_cast<const char*>(job) : "";
    sqlite3_finalize(stmt);
    if (!job_s.empty()) {
        std::fprintf(stderr, "rollback left a job\n");
        return 1;
    }
    (void)committed;

    sqlite3_close(db);
    std::printf("PASS cpp-framework-owned-sqlite3\n");
    return 0;
}
