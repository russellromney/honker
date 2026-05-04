// Phase Mantle: Scheduler lifecycle (pause/resume/list/update) +
// Queue cancel/get_job.

#include "honker.hpp"

#include <cassert>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <filesystem>
#include <iostream>
#include <nlohmann/json.hpp>
#include <string>
#include <thread>

namespace fs = std::filesystem;
using nlohmann::json;

namespace {

honker::Database open_db(const fs::path& tmp, const char* ext) {
    fs::remove(tmp);
    fs::remove(tmp.string() + "-wal");
    fs::remove(tmp.string() + "-shm");
    return honker::Database{tmp.string(), ext};
}

void test_schedule_list_round_trips_fields(const char* ext) {
    auto db = open_db(fs::temp_directory_path() / "honker-cpp-mantle-list.db", ext);
    auto sched = db.scheduler();
    sched.add("recap", "emails", "0 9 * * 1", R"({"team":"premier-league"})", 3);
    sched.add("sync", "syncs", "@every 1h", "null", 0);

    auto rows = json::parse(sched.list_json());
    assert(rows.size() == 2);
    json recap;
    for (const auto& r : rows) {
        if (r["name"] == "recap") recap = r;
    }
    assert(recap["queue"] == "emails");
    assert(recap["priority"] == 3);
    assert(recap["enabled"] == true);
    auto payload = json::parse(recap["payload"].get<std::string>());
    assert(payload["team"] == "premier-league");
    std::cout << "schedule_list_round_trips_fields: ok\n";
}

void test_pause_resume_idempotent(const char* ext) {
    auto db = open_db(fs::temp_directory_path() / "honker-cpp-mantle-pause.db", ext);
    auto sched = db.scheduler();
    sched.add("a", "q", "0 9 * * *", "null", 0);

    assert(sched.pause("a") == true);
    assert(sched.pause("a") == false);   // idempotent
    assert(sched.pause("missing") == false);

    auto rows = json::parse(sched.list_json());
    assert(rows[0]["enabled"] == false);

    assert(sched.resume("a") == true);
    assert(sched.resume("a") == false);
    rows = json::parse(sched.list_json());
    assert(rows[0]["enabled"] == true);
    std::cout << "pause_resume_idempotent: ok\n";
}

void test_update_mutates_and_noop(const char* ext) {
    auto db = open_db(fs::temp_directory_path() / "honker-cpp-mantle-update.db", ext);
    auto sched = db.scheduler();
    sched.add("t", "q", "0 9 * * *", R"({"v":1})", 0);

    // Mutate payload + priority.
    assert(sched.update("t", std::nullopt, std::string_view{R"({"v":99})"}, std::optional<int64_t>{5}, std::nullopt));
    auto rows = json::parse(sched.list_json());
    auto payload = json::parse(rows[0]["payload"].get<std::string>());
    assert(payload["v"] == 99);
    assert(rows[0]["priority"] == 5);

    // Cron change recomputes next_fire_at.
    int64_t before = rows[0]["next_fire_at"].get<int64_t>();
    assert(sched.update("t", std::string_view{"*/5 * * * *"}, std::nullopt, std::nullopt, std::nullopt));
    rows = json::parse(sched.list_json());
    assert(rows[0]["cron_expr"] == "*/5 * * * *");
    assert(rows[0]["next_fire_at"].get<int64_t>() != before);

    // Empty update is a no-op.
    assert(sched.update("t", std::nullopt, std::nullopt, std::nullopt, std::nullopt) == false);
    // Missing schedule.
    assert(sched.update("missing", std::nullopt, std::string_view{"{}"}, std::nullopt, std::nullopt) == false);
    std::cout << "update_mutates_and_noop: ok\n";
}

void test_cancel_and_get_job(const char* ext) {
    auto db = open_db(fs::temp_directory_path() / "honker-cpp-mantle-cancel.db", ext);
    auto q = db.queue("emails");
    auto id = q.enqueue(R"({"to":"alice@example.com"})");
    assert(id > 0);

    auto raw = q.get_job_json(id);
    assert(!raw.empty());
    auto row = json::parse(raw);
    assert(row["queue"] == "emails");
    assert(row["state"] == "pending");
    assert(row["id"] == id);

    assert(q.cancel(id) == true);
    assert(q.cancel(id) == false); // idempotent
    assert(q.get_job_json(id).empty());
    auto second = q.claim_one("worker-1");
    assert(!second.has_value());
    std::cout << "cancel_and_get_job: ok\n";
}

void test_cancel_processing_invalidates_ack(const char* ext) {
    auto db = open_db(fs::temp_directory_path() / "honker-cpp-mantle-proc.db", ext);
    auto q = db.queue("emails");
    auto id = q.enqueue(R"({"to":"x"})");
    auto job = q.claim_one("worker-1");
    assert(job.has_value());
    assert(job->id() == id);

    assert(q.cancel(id) == true);
    // Worker's ack now returns false (same shape as expired claim).
    assert(job->ack() == false);
    std::cout << "cancel_processing_invalidates_ack: ok\n";
}

void test_paused_schedule_does_not_emit(const char* ext) {
    auto db = open_db(fs::temp_directory_path() / "honker-cpp-mantle-paused-tick.db", ext);
    auto sched = db.scheduler();
    sched.add("due", "emails", "@every 1s", R"({"x":1})", 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    assert(sched.pause("due") == true);

    const auto future = static_cast<int64_t>(std::time(nullptr)) + 5;
    auto fires = sched.tick(future);
    assert(fires.empty() && "paused schedule must not emit");

    assert(sched.resume("due") == true);
    auto fires2 = sched.tick(future);
    assert(!fires2.empty() && "resumed schedule should emit");
    std::cout << "paused_schedule_does_not_emit: ok\n";
}

void test_get_job_misses_after_ack(const char* ext) {
    auto db = open_db(fs::temp_directory_path() / "honker-cpp-mantle-getack.db", ext);
    auto q = db.queue("emails");
    auto id = q.enqueue(R"({"to":"x"})");
    auto job = q.claim_one("worker-1");
    assert(job.has_value());
    assert(job->ack() == true);
    // Row gone after ack — get_job misses just like after cancel.
    assert(q.get_job_json(id).empty());
    std::cout << "get_job_misses_after_ack: ok\n";
}

void test_update_payload_null_vs_omitted(const char* ext) {
    auto db = open_db(fs::temp_directory_path() / "honker-cpp-mantle-payload.db", ext);
    auto sched = db.scheduler();
    sched.add("t", "q", "0 9 * * *", R"({"v":1})", 0);

    // Omitted payload (std::nullopt for the payload arg) — leaves alone.
    assert(sched.update("t", std::nullopt, std::nullopt, std::optional<int64_t>{7}, std::nullopt));
    auto row = json::parse(sched.list_json())[0];
    assert(json::parse(row["payload"].get<std::string>())["v"] == 1);

    // payload: explicit "null" string — write JSON null.
    assert(sched.update("t", std::nullopt, std::string_view{"null"}, std::nullopt, std::nullopt));
    row = json::parse(sched.list_json())[0];
    assert(json::parse(row["payload"].get<std::string>()).is_null());
    std::cout << "update_payload_null_vs_omitted: ok\n";
}

}  // anonymous namespace

int main() {
    const char* ext = std::getenv("HONKER_EXTENSION_PATH");
    if (!ext || !*ext) {
        std::fputs(
            "skip: HONKER_EXTENSION_PATH not set "
            "(export it to ./libhonker_ext.{dylib,so})\n",
            stderr);
        return 0;
    }

    try {
        test_schedule_list_round_trips_fields(ext);
        test_pause_resume_idempotent(ext);
        test_update_mutates_and_noop(ext);
        test_cancel_and_get_job(ext);
        test_cancel_processing_invalidates_ack(ext);
        test_paused_schedule_does_not_emit(ext);
        test_get_job_misses_after_ack(ext);
        test_update_payload_null_vs_omitted(ext);
    } catch (const std::exception& e) {
        std::cerr << "FAIL: " << e.what() << '\n';
        return 1;
    }

    std::cout << "all phase mantle tests passed\n";
    return 0;
}
