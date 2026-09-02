// Issue #136: claimed jobs and read-only snapshots must carry every
// field honker_claim_batch() / honker_get_job() return.
//
// Every value asserted here is chosen to be distinguishable from the
// others. Defaults (priority 0, max_attempts 3, a 300s visibility
// timeout, run_at == created_at) would let a wrong field pass for the
// right one, so the fixture uses odd constants and back-dates run_at
// well away from created_at.

#include "honker.hpp"

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <filesystem>
#include <iostream>
#include <nlohmann/json.hpp>
#include <optional>
#include <string>
#include <vector>

namespace fs = std::filesystem;
using nlohmann::json;

namespace {

// Fixture constants. All distinct, none a default, none derivable
// from another by accident.
constexpr int64_t VISIBILITY_TIMEOUT_S = 137;
constexpr int64_t MAX_ATTEMPTS         = 9;
constexpr int64_t HIGH_PRIORITY        = 42;
constexpr int64_t LOW_PRIORITY         = 7;
constexpr int64_t RUN_AT_BACKDATE_S    = 300;
constexpr int64_t EXPIRES_S            = 9000;
constexpr int64_t DELAY_S              = 600;

int64_t now_unix() { return static_cast<int64_t>(std::time(nullptr)); }

[[noreturn]] void fail(const std::string& msg) {
    std::cerr << "FAIL: " << msg << '\n';
    std::abort();
}

void check_i64(int64_t got, int64_t want, const char* what) {
    if (got != want) {
        fail(std::string{what} + ": got " + std::to_string(got) +
             ", want " + std::to_string(want));
    }
}

void check_ne_i64(int64_t got, int64_t forbidden, const char* what) {
    if (got == forbidden) {
        fail(std::string{what} + ": got " + std::to_string(got) +
             ", which must not equal " + std::to_string(forbidden));
    }
}

void check_str(const std::string& got, const std::string& want, const char* what) {
    if (got != want) {
        fail(std::string{what} + ": got \"" + got + "\", want \"" + want + "\"");
    }
}

void check_range(int64_t got, int64_t lo, int64_t hi, const char* what) {
    if (got < lo || got > hi) {
        fail(std::string{what} + ": got " + std::to_string(got) +
             ", want within [" + std::to_string(lo) + ", " + std::to_string(hi) + "]");
    }
}

honker::Database open_db(const fs::path& tmp, const char* ext) {
    fs::remove(tmp);
    fs::remove(tmp.string() + "-wal");
    fs::remove(tmp.string() + "-shm");
    return honker::Database{tmp.string(), ext};
}

// The C ABI's enqueue takes only delay/priority/max_attempts. The
// fixture needs an absolute run_at and an expiry too, so it goes
// through the SQL function directly. Nothing about the read path
// changes — this is only how the row gets on disk.
int64_t raw_enqueue(sqlite3* db, const char* queue, const char* payload,
                    int64_t run_at, int64_t priority, int64_t max_attempts,
                    std::optional<int64_t> expires) {
    sqlite3_stmt* stmt = nullptr;
    const char* sql = "SELECT honker_enqueue(?1, ?2, ?3, NULL, ?4, ?5, ?6)";
    const int prepared = sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr);
    assert(prepared == SQLITE_OK);
    (void)prepared;
    sqlite3_bind_text(stmt, 1, queue, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, payload, -1, SQLITE_STATIC);
    sqlite3_bind_int64(stmt, 3, run_at);
    sqlite3_bind_int64(stmt, 4, priority);
    sqlite3_bind_int64(stmt, 5, max_attempts);
    if (expires) {
        sqlite3_bind_int64(stmt, 6, *expires);
    } else {
        sqlite3_bind_null(stmt, 6);
    }
    int64_t id = -1;
    if (sqlite3_step(stmt) == SQLITE_ROW) id = sqlite3_column_int64(stmt, 0);
    sqlite3_finalize(stmt);
    if (id <= 0) fail("raw_enqueue did not return a job id");
    return id;
}

void test_claimed_job_carries_every_core_field(const char* ext) {
    auto db = open_db(fs::temp_directory_path() / "honker-cpp-details-claim.db", ext);
    auto q = db.queue("details", VISIBILITY_TIMEOUT_S, MAX_ATTEMPTS);

    const int64_t enqueued_lo = now_unix();
    // Back-dated run_at: claimable immediately, but far enough from
    // created_at that swapping the two fields cannot pass.
    const int64_t high_run_at = enqueued_lo - RUN_AT_BACKDATE_S;
    const int64_t high_id = raw_enqueue(
        db.raw(), "details", R"({"to":"alice@example.com","v":1})",
        high_run_at, HIGH_PRIORITY, MAX_ATTEMPTS, EXPIRES_S);
    const int64_t low_run_at = enqueued_lo - (RUN_AT_BACKDATE_S / 2);
    // No expiry on this one: proves expires_at is read per-row and not
    // filled from a constant.
    const int64_t low_id = raw_enqueue(
        db.raw(), "details", R"({"to":"bob@example.com","v":1})",
        low_run_at, LOW_PRIORITY, MAX_ATTEMPTS, std::nullopt);
    const int64_t enqueued_hi = now_unix();

    const int64_t claim_lo = now_unix();
    auto jobs = q.claim_batch("worker-parity", 2);
    const int64_t claim_hi = now_unix();
    check_i64(static_cast<int64_t>(jobs.size()), 2, "claimed job count");

    // priority DESC decides the order, so the high-priority row is first.
    const auto& high = jobs[0];
    const auto& low  = jobs[1];

    check_i64(high.id(), high_id, "id");
    check_i64(low.id(), low_id, "id of the second row");
    check_str(high.queue(), "details", "queue");
    check_str(high.payload(), R"({"to":"alice@example.com","v":1})",
              "payload stays the raw JSON text as enqueued");
    check_str(json::parse(high.payload())["to"].get<std::string>(),
              "alice@example.com", "decoded payload");
    check_str(high.state(), "processing", "state");

    check_i64(high.priority(), HIGH_PRIORITY, "priority");
    check_i64(low.priority(), LOW_PRIORITY, "priority is per-row, not a constant");

    check_i64(high.run_at(), high_run_at, "run_at");
    check_i64(low.run_at(), low_run_at, "run_at is per-row");
    check_ne_i64(high.run_at(), high.created_at(), "run_at must not be created_at");

    check_str(high.worker_id(), "worker-parity", "worker_id");

    check_range(high.claim_expires_at(), claim_lo + VISIBILITY_TIMEOUT_S,
                claim_hi + VISIBILITY_TIMEOUT_S,
                "claim_expires_at is the claim instant + 137s");

    check_i64(high.attempts(), 1, "attempts after the first claim");
    check_i64(high.max_attempts(), MAX_ATTEMPTS, "max_attempts");
    check_range(high.created_at(), enqueued_lo, enqueued_hi,
                "created_at inside the enqueue window");

    if (!high.expires_at().has_value()) fail("expires_at unset on the high row");
    check_range(*high.expires_at(), enqueued_lo + EXPIRES_S, enqueued_hi + EXPIRES_S,
                "expires_at is the enqueue instant + 9000s");
    if (low.expires_at().has_value()) {
        fail("expires_at should stay null when the enqueue set no expiry");
    }

    std::cout << "claimed_job_carries_every_core_field: ok\n";
}

void test_claimed_job_attempts_count_up_across_retries(const char* ext) {
    auto db = open_db(fs::temp_directory_path() / "honker-cpp-details-attempts.db", ext);
    auto q = db.queue("details", VISIBILITY_TIMEOUT_S, MAX_ATTEMPTS);
    q.enqueue(R"({"n":1})");

    auto first = q.claim_one("w1");
    if (!first.has_value()) fail("first claim came back empty");
    check_i64(first->attempts(), 1, "attempts on the first claim");
    assert(first->retry(0, "boom") == true);

    auto second = q.claim_one("w2");
    if (!second.has_value()) fail("second claim came back empty");
    check_i64(second->attempts(), 2,
              "attempts must advance with the retry, not stick at 1");
    check_str(second->worker_id(), "w2", "worker_id follows the new holder");
    check_i64(second->id(), first->id(), "same row, re-claimed");
    check_i64(second->created_at(), first->created_at(),
              "created_at is stable across claims");

    std::cout << "claimed_job_attempts_count_up_across_retries: ok\n";
}

void test_snapshot_pending_then_processing_then_miss(const char* ext) {
    const auto path = fs::temp_directory_path() / "honker-cpp-details-snapshot.db";
    auto db = open_db(path, ext);
    auto q = db.queue("details", VISIBILITY_TIMEOUT_S, MAX_ATTEMPTS);

    const int64_t enqueued_lo = now_unix();
    const int64_t id = raw_enqueue(
        db.raw(), "details", R"({"to":"carol@example.com"})",
        enqueued_lo, HIGH_PRIORITY, MAX_ATTEMPTS, EXPIRES_S);
    const int64_t enqueued_hi = now_unix();

    auto pending = q.get_job(id);
    if (!pending.has_value()) fail("pending snapshot missed");
    check_i64(pending->id(), id, "id");
    check_str(pending->queue(), "details", "queue");
    check_str(pending->payload(), R"({"to":"carol@example.com"})",
              "snapshot payload stays raw JSON text");
    check_str(pending->state(), "pending", "state before any claim");
    check_i64(pending->priority(), HIGH_PRIORITY, "priority");
    check_i64(pending->run_at(), enqueued_lo, "run_at");
    if (pending->worker_id().has_value()) fail("no holder while pending");
    if (pending->claim_expires_at().has_value()) fail("no claim deadline while pending");
    check_i64(pending->attempts(), 0, "attempts before any claim");
    check_i64(pending->max_attempts(), MAX_ATTEMPTS, "max_attempts");
    check_range(pending->created_at(), enqueued_lo, enqueued_hi,
                "created_at inside the enqueue window");
    if (!pending->expires_at().has_value()) fail("expires_at unset");
    check_range(*pending->expires_at(), enqueued_lo + EXPIRES_S,
                enqueued_hi + EXPIRES_S, "expires_at is the enqueue instant + 9000s");

    // A second reader sees the processing details of someone else's claim.
    const int64_t claim_lo = now_unix();
    auto job = q.claim_one("worker-reader");
    const int64_t claim_hi = now_unix();
    if (!job.has_value()) fail("claim came back empty");

    honker::Database reader{path.string(), ext};
    auto rq = reader.queue("details", VISIBILITY_TIMEOUT_S, MAX_ATTEMPTS);
    auto processing = rq.get_job(id);
    if (!processing.has_value()) fail("processing snapshot missed");
    check_str(processing->state(), "processing", "state while claimed");
    if (!processing->worker_id().has_value()) fail("worker_id unset while processing");
    check_str(*processing->worker_id(), "worker-reader", "worker_id names the holder");
    check_i64(processing->attempts(), 1, "attempts after the claim");
    if (!processing->claim_expires_at().has_value()) {
        fail("claim_expires_at unset while processing");
    }
    check_range(*processing->claim_expires_at(), claim_lo + VISIBILITY_TIMEOUT_S,
                claim_hi + VISIBILITY_TIMEOUT_S,
                "claim_expires_at is the claim instant + 137s");
    // Unchanged by the claim.
    check_i64(processing->priority(), HIGH_PRIORITY, "priority survives");
    check_i64(processing->max_attempts(), MAX_ATTEMPTS, "max_attempts survives");
    check_i64(processing->created_at(), pending->created_at(), "created_at survives");
    check_i64(*processing->expires_at(), *pending->expires_at(), "expires_at survives");

    assert(job->ack() == true);
    if (rq.get_job(id).has_value()) fail("the reader still sees a job after ack");

    std::cout << "snapshot_pending_then_processing_then_miss: ok\n";
}

void test_delayed_job_snapshot_reports_future_run_at(const char* ext) {
    auto db = open_db(fs::temp_directory_path() / "honker-cpp-details-delay.db", ext);
    auto q = db.queue("details", VISIBILITY_TIMEOUT_S, MAX_ATTEMPTS);

    const int64_t lo = now_unix();
    const int64_t id = q.enqueue(R"({"later":true})", DELAY_S);
    const int64_t hi = now_unix();

    auto row = q.get_job(id);
    if (!row.has_value()) fail("delayed snapshot missed");
    check_range(row->run_at(), lo + DELAY_S, hi + DELAY_S,
                "delayed run_at is the enqueue instant + 600s");
    check_i64(row->run_at() - row->created_at(), DELAY_S,
              "run_at must sit exactly 600s after created_at");
    check_str(row->state(), "pending", "a delayed job stays pending");
    if (q.claim_one("w").has_value()) fail("a delayed job is not claimable yet");

    std::cout << "delayed_job_snapshot_reports_future_run_at: ok\n";
}

void test_get_job_json_still_returns_the_raw_blob(const char* ext) {
    // get_job() decodes; get_job_json() keeps handing back the bytes.
    // Both must agree, and both must miss after ack.
    auto db = open_db(fs::temp_directory_path() / "honker-cpp-details-rawjson.db", ext);
    auto q = db.queue("details", VISIBILITY_TIMEOUT_S, MAX_ATTEMPTS);
    const int64_t id = q.enqueue(R"({"raw":true})", 0, LOW_PRIORITY);

    const auto blob = q.get_job_json(id);
    if (blob.empty()) fail("get_job_json missed a live row");
    auto parsed = json::parse(blob);
    auto row = q.get_job(id);
    if (!row.has_value()) fail("get_job missed a live row");
    check_i64(row->id(), parsed["id"].get<int64_t>(), "id agrees with the raw blob");
    check_str(row->payload(), parsed["payload"].get<std::string>(),
              "payload agrees with the raw blob");
    check_i64(row->priority(), LOW_PRIORITY, "priority agrees with the enqueue");

    auto job = q.claim_one("w");
    if (!job.has_value()) fail("claim came back empty");
    assert(job->ack() == true);
    if (!q.get_job_json(id).empty()) fail("get_job_json should miss after ack");
    if (q.get_job(id).has_value()) fail("get_job should miss after ack");

    std::cout << "get_job_json_still_returns_the_raw_blob: ok\n";
}

void exec_raw(sqlite3* db, const char* sql) {
    char* err = nullptr;
    if (sqlite3_exec(db, sql, nullptr, nullptr, &err) != SQLITE_OK) {
        const std::string msg = err ? err : "unknown error";
        sqlite3_free(err);
        fail(std::string{"exec_raw failed for "} + sql + ": " + msg);
    }
}

// The decoders throw on a malformed stream row so a bad row cannot
// become offset 0. That only closes the read half. The write half is
// StreamSubscription's checkpoint: if it drops a failed save on the
// floor, the consumer replays the topic just the same, and nothing —
// not the destructor's catch (...), not save_offset() — ever says so.
void test_stream_subscription_reports_a_failed_checkpoint(const char* ext) {
    const auto path = fs::temp_directory_path() / "honker-cpp-details-checkpoint.db";
    auto db = open_db(path, ext);
    auto stream = db.stream("checkpoints");
    const int64_t first_offset = stream.publish(R"({"n":1})");
    stream.publish(R"({"n":2})");

    // save_every_n is far above the event count, so the only checkpoint
    // in this test is the one it asks for explicitly.
    auto sub = stream.subscribe("consumer-a", 1000, std::chrono::milliseconds(20));
    auto first = sub.next();
    if (!first.has_value()) fail("the subscription produced no event");
    check_i64(first->offset(), first_offset, "first event offset");

    // query_only makes every write on this connection fail the way a
    // read-only volume or a full disk would.
    exec_raw(db.raw(), "PRAGMA query_only = 1");
    bool reported = false;
    try {
        sub.save_offset();
    } catch (const honker::Error&) {
        reported = true;
    }
    exec_raw(db.raw(), "PRAGMA query_only = 0");
    if (!reported) fail("save_offset swallowed a failed checkpoint write");

    // The failure did not fake a checkpoint either.
    check_i64(stream.get_offset("consumer-a"), 0,
              "a failed checkpoint must leave the saved offset untouched");

    // And the position is still pending, so a retry commits it.
    sub.save_offset();
    check_i64(stream.get_offset("consumer-a"), first_offset,
              "the retry saved the offset the failed write did not");

    std::cout << "stream_subscription_reports_a_failed_checkpoint: ok\n";
}

// ---------------------------------------------------------------------
// Decode failures. These are the tests that defend the "fails loudly"
// promise: a malformed blob, an absent or null required field, and a
// wrong-typed field must each raise honker::Error instead of producing
// a plausible-looking default.
//
// They need no extension and no database. They call exactly the
// functions Queue::get_job() and Queue::claim_batch() call —
// detail::parse_row_blob / detail::parse_row_array / from_json — so
// reverting the guard in honker.hpp fails them.
// ---------------------------------------------------------------------

// A complete, well-formed claimed-job row. Every failure case below is
// this object with one thing wrong.
json good_row() {
    return json{
        {"id", 101},
        {"queue", "details"},
        {"payload", R"({"n":1})"},
        {"state", "processing"},
        {"priority", HIGH_PRIORITY},
        {"run_at", 1700000000},
        {"worker_id", "w1"},
        {"claim_expires_at", 1700000137},
        {"attempts", 1},
        {"max_attempts", MAX_ATTEMPTS},
        {"created_at", 1700000300},
        {"expires_at", 1700009000},
    };
}

// Runs fn and demands it throw honker::Error whose message contains
// needle. Returning normally, or throwing anything else, is a failure —
// a caller told to write catch (const honker::Error&) must not have a
// JSON library's exception escape past it.
template <typename Fn>
void expect_honker_error(Fn&& fn, const std::string& needle, const std::string& label) {
    try {
        fn();
    } catch (const honker::Error& e) {
        const std::string msg = e.what();
        if (msg.find(needle) == std::string::npos) {
            fail(label + ": honker::Error said \"" + msg +
                 "\", which does not mention \"" + needle + "\"");
        }
        return;
    } catch (const json::exception& e) {
        fail(label + ": leaked nlohmann::json::exception \"" +
             std::string{e.what()} + "\" instead of honker::Error");
    } catch (const std::exception& e) {
        fail(label + ": threw \"" + std::string{e.what()} +
             "\" instead of honker::Error");
    }
    fail(label + ": returned normally — no error was raised");
}

void test_decode_rejects_unparseable_json() {
    expect_honker_error(
        [] { (void)honker::detail::parse_row_blob(R"({"id": 1,)", "job row"); },
        "not parseable", "get_job blob with truncated JSON");
    expect_honker_error(
        [] { (void)honker::detail::parse_row_blob("", "job row"); },
        "not parseable", "get_job blob that is empty text");
    expect_honker_error(
        [] { (void)honker::detail::parse_row_array("<html>nope</html>", "claim"); },
        "not parseable", "claim blob that is not JSON at all");

    std::cout << "decode_rejects_unparseable_json: ok\n";
}

void test_decode_rejects_a_non_array_claim_result() {
    // A claim that comes back as a bare object or null is a core bug.
    // Returning {} here would read as "nothing to claim" while the
    // claim UPDATE has already burned an attempt on every row.
    expect_honker_error(
        [] { (void)honker::detail::parse_row_array(good_row().dump(), "claim"); },
        "not a JSON array", "claim result that is a single object");
    expect_honker_error(
        [] { (void)honker::detail::parse_row_array("null", "claim"); },
        "not a JSON array", "claim result that is null");
    expect_honker_error(
        [] { (void)honker::detail::parse_row_array("17", "claim"); },
        "not a JSON array", "claim result that is a number");

    std::cout << "decode_rejects_a_non_array_claim_result: ok\n";
}

void test_decode_rejects_a_missing_required_field() {
    // The fixture itself must decode, so a failure below is the removed
    // field and not a broken fixture.
    const auto ok = honker::JobSnapshot::from_json(good_row());
    check_i64(ok.id(), 101, "the fixture row decodes cleanly");
    check_i64(ok.attempts(), 1, "the fixture row decodes cleanly");

    // Every non-nullable field on a snapshot, absent and explicitly
    // null. This loop is what catches a thirteenth field added later
    // with a tolerant lookup.
    const std::vector<std::string> required = {
        "id", "queue", "payload", "state", "priority",
        "run_at", "attempts", "max_attempts", "created_at",
    };
    for (const auto& key : required) {
        auto absent = good_row();
        absent.erase(key);
        expect_honker_error(
            [&] { (void)honker::JobSnapshot::from_json(absent); },
            "missing required field '" + key + "'",
            "snapshot with no " + key);

        auto nulled = good_row();
        nulled[key] = nullptr;
        expect_honker_error(
            [&] { (void)honker::JobSnapshot::from_json(nulled); },
            "missing required field '" + key + "'",
            "snapshot with a null " + key);
    }

    // A claimed row always carries the claim fields, so Job requires
    // the two that JobSnapshot leaves optional.
    for (const auto& key : std::vector<std::string>{"worker_id", "claim_expires_at"}) {
        auto absent = good_row();
        absent.erase(key);
        expect_honker_error(
            [&] { (void)honker::Job::from_json(nullptr, absent); },
            "missing required field '" + key + "'",
            "claimed job with no " + key);
    }

    // A row that is not an object at all.
    expect_honker_error(
        [] { (void)honker::JobSnapshot::from_json(json::array({1, 2})); },
        "not a JSON object", "snapshot from a JSON array");

    std::cout << "decode_rejects_a_missing_required_field: ok\n";
}

void test_decode_reports_a_wrong_typed_field_as_honker_error() {
    // README promises honker::Error, so nlohmann's type_error must not
    // escape. expect_honker_error fails loudly if it does.
    struct Case {
        std::string key;
        json        value;
        std::string want;
    };
    const std::vector<Case> cases = {
        {"id",               json("101"),                  "an integer"},
        {"attempts",         json(true),                   "an integer"},
        {"claim_expires_at", json("1700000137"),           "an integer"},
        {"payload",          json::object({{"n", 1}}),     "a string"},
        {"queue",            json::array({"details"}),     "a string"},
        {"state",            json(3),                      "a string"},
        // Nullable fields decode through the same helper.
        {"expires_at",       json("1700009000"),           "an integer"},
        {"worker_id",        json(7),                      "a string"},
    };
    for (const auto& c : cases) {
        auto bad = good_row();
        bad[c.key] = c.value;
        expect_honker_error(
            [&] { (void)honker::JobSnapshot::from_json(bad); },
            "field '" + c.key + "' is not " + c.want,
            "snapshot with a wrong-typed " + c.key);
    }

    // Same guarantee on the claim path.
    auto bad_claim = good_row();
    bad_claim["worker_id"] = 7;
    expect_honker_error(
        [&] { (void)honker::Job::from_json(nullptr, bad_claim); },
        "field 'worker_id' is not a string",
        "claimed job with a wrong-typed worker_id");

    std::cout << "decode_reports_a_wrong_typed_field_as_honker_error: ok\n";
}

void test_stream_and_scheduler_rows_decode_loudly() {
    // Same anti-pattern as the job decoders, same fix. The offset one
    // is the dangerous shape: a swallowed row used to yield offset 0,
    // and a consumer that saved that offset replayed the whole topic.
    const json event = json{
        {"offset", 42},
        {"topic", "t"},
        {"key", "k"},
        {"payload", R"({"n":1})"},
        {"created_at", 1700000000},
    };

    // Positive: the well-formed row decodes, and a null key stays the
    // empty string rather than becoming an error.
    auto unkeyed = event;
    unkeyed["key"] = nullptr;
    const auto events = honker::detail::parse_stream_events(
        json::array({event, unkeyed}).dump());
    check_i64(static_cast<int64_t>(events.size()), 2, "stream events decoded");
    check_i64(events[0].offset(), 42, "stream event offset");
    check_str(events[0].key(), "k", "stream event key");
    check_str(events[1].key(), "", "a null key decodes to the empty string");

    for (const auto& key : std::vector<std::string>{
             "offset", "topic", "payload", "created_at"}) {
        auto bad = event;
        bad.erase(key);
        expect_honker_error(
            [&] { (void)honker::detail::parse_stream_events(json::array({bad}).dump()); },
            "missing required field '" + key + "'",
            "stream event with no " + key);
    }
    auto typed = event;
    typed["offset"] = "42";
    expect_honker_error(
        [&] { (void)honker::detail::parse_stream_events(json::array({typed}).dump()); },
        "field 'offset' is not an integer",
        "stream event with a string offset");
    expect_honker_error(
        [] { (void)honker::detail::parse_stream_events("{oops"); },
        "not parseable", "stream read blob that is not JSON");

    const json fire = json{
        {"name", "nightly"},
        {"queue", "emails"},
        {"fire_at", 1700000000},
        {"job_id", 7},
    };
    const auto fires = honker::detail::parse_scheduler_fires(json::array({fire}).dump());
    check_i64(static_cast<int64_t>(fires.size()), 1, "scheduler fires decoded");
    check_i64(fires[0].job_id(), 7, "scheduler fire job_id");

    for (const auto& key : std::vector<std::string>{"name", "queue", "fire_at", "job_id"}) {
        auto bad = fire;
        bad.erase(key);
        expect_honker_error(
            [&] { (void)honker::detail::parse_scheduler_fires(json::array({bad}).dump()); },
            "missing required field '" + key + "'",
            "scheduler fire with no " + key);
    }
    expect_honker_error(
        [] { (void)honker::detail::parse_scheduler_fires(R"({"name":"n"})"); },
        "not a JSON array", "scheduler tick result that is a single object");

    std::cout << "stream_and_scheduler_rows_decode_loudly: ok\n";
}

}  // anonymous namespace

int main() {
    // Decode-failure tests first: they need no extension, so they run
    // even where the DB-backed tests below would skip.
    try {
        test_decode_rejects_unparseable_json();
        test_decode_rejects_a_non_array_claim_result();
        test_decode_rejects_a_missing_required_field();
        test_decode_reports_a_wrong_typed_field_as_honker_error();
        test_stream_and_scheduler_rows_decode_loudly();
    } catch (const std::exception& e) {
        std::cerr << "FAIL: " << e.what() << '\n';
        return 1;
    }

    const char* ext = std::getenv("HONKER_EXTENSION_PATH");
    if (!ext || !*ext) {
        std::fputs(
            "skip: HONKER_EXTENSION_PATH not set "
            "(export it to ./libhonker_ext.{dylib,so})\n",
            stderr);
        return 0;
    }

    try {
        test_claimed_job_carries_every_core_field(ext);
        test_claimed_job_attempts_count_up_across_retries(ext);
        test_snapshot_pending_then_processing_then_miss(ext);
        test_delayed_job_snapshot_reports_future_run_at(ext);
        test_get_job_json_still_returns_the_raw_blob(ext);
        test_stream_subscription_reports_a_failed_checkpoint(ext);
    } catch (const std::exception& e) {
        std::cerr << "FAIL: " << e.what() << '\n';
        return 1;
    }

    std::cout << "all job detail tests passed\n";
    return 0;
}
