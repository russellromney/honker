// Framework-owned sqlite3, as guides/orm/cpp.mdx shows it: Honker as a
// helper over a sqlite3* the surrounding data layer owns, with the
// business write and the enqueue on the same transaction.
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <sqlite3.h>
#include <string>

static void must(int rc, sqlite3* db, const char* what) {
    if (rc != SQLITE_OK && rc != SQLITE_ROW && rc != SQLITE_DONE) {
        std::fprintf(stderr, "%s: %s\n", what, db ? sqlite3_errmsg(db) : "(no db)");
        std::exit(1);
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
    // No entry point argument: filename derivation is part of the
    // contract the docs rely on.
    if (sqlite3_load_extension(db, ext, nullptr, &err) != SQLITE_OK) {
        std::fprintf(stderr, "load_extension: %s\n", err ? err : "(unknown)");
        return 1;
    }
    must(sqlite3_exec(db, "SELECT honker_bootstrap()", nullptr, nullptr, nullptr), db, "bootstrap");

    must(sqlite3_exec(db, "BEGIN IMMEDIATE", nullptr, nullptr, nullptr), db, "begin");

    sqlite3_stmt* stmt = nullptr;
    // Bound parameters, so the C API's own binding is exercised.
    must(sqlite3_prepare_v2(
             db, "SELECT honker_enqueue(?, ?, NULL, NULL, ?, ?, NULL)", -1, &stmt, nullptr),
         db, "prepare enqueue");
    sqlite3_bind_text(stmt, 1, "emails", -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, R"({"to":"alice@example.com"})", -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(stmt, 3, 0);
    sqlite3_bind_int64(stmt, 4, 3);
    if (sqlite3_step(stmt) != SQLITE_ROW) {
        std::fprintf(stderr, "enqueue: %s\n", sqlite3_errmsg(db));
        return 1;
    }
    sqlite3_int64 id = sqlite3_column_int64(stmt, 0);
    sqlite3_finalize(stmt);
    if (id <= 0) {
        std::fprintf(stderr, "expected a job id, got %lld\n", (long long)id);
        return 1;
    }

    must(sqlite3_exec(db, "COMMIT", nullptr, nullptr, nullptr), db, "commit");

    must(sqlite3_prepare_v2(db, "SELECT honker_claim_batch(?, ?, ?, ?)", -1, &stmt, nullptr),
         db, "prepare claim");
    sqlite3_bind_text(stmt, 1, "emails", -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, "w1", -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(stmt, 3, 8);
    sqlite3_bind_int64(stmt, 4, 300);
    if (sqlite3_step(stmt) != SQLITE_ROW) {
        std::fprintf(stderr, "claim: %s\n", sqlite3_errmsg(db));
        return 1;
    }
    std::string claimed = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
    sqlite3_finalize(stmt);
    if (claimed.find("\"id\":" + std::to_string(id)) == std::string::npos) {
        std::fprintf(stderr, "claimed the wrong job: %s\n", claimed.c_str());
        return 1;
    }

    must(sqlite3_prepare_v2(db, "SELECT honker_ack(?, ?)", -1, &stmt, nullptr), db, "prepare ack");
    sqlite3_bind_int64(stmt, 1, id);
    sqlite3_bind_text(stmt, 2, "w1", -1, SQLITE_TRANSIENT);
    if (sqlite3_step(stmt) != SQLITE_ROW || sqlite3_column_int(stmt, 0) != 1) {
        std::fprintf(stderr, "ack must match the claim\n");
        return 1;
    }
    sqlite3_finalize(stmt);
    sqlite3_close(db);

    std::printf("PASS cpp-framework-owned-sqlite3\n");
    return 0;
}
