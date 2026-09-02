// honker.hpp — C++17 RAII wrappers around the Zig-implemented C ABI.
#pragma once

#include <atomic>
#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <functional>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <nlohmann/json.hpp>

#if defined(_WIN32)
#include <windows.h>
#else
#include <dlfcn.h>
#endif

extern "C" {
    #include <sqlite3.h>

    int32_t honker_cpp_open(const char* path, const char* ext_path, sqlite3** out_db);
    void    honker_cpp_close(sqlite3* db);

    int32_t honker_cpp_begin_tx(sqlite3* db);
    int32_t honker_cpp_commit_tx(sqlite3* db);
    int32_t honker_cpp_rollback_tx(sqlite3* db);

    int64_t honker_cpp_enqueue(
        sqlite3* db,
        const char* queue,
        const char* payload_json,
        int64_t delay_sec,
        int64_t priority,
        int64_t max_attempts);

    char*   honker_cpp_claim_one(
        sqlite3* db,
        const char* queue,
        const char* worker_id,
        int64_t visibility_timeout_s);
    char*   honker_cpp_claim_batch(
        sqlite3* db,
        const char* queue,
        const char* worker_id,
        int64_t n,
        int64_t visibility_timeout_s);

    int64_t honker_cpp_ack(sqlite3* db, int64_t job_id, const char* worker_id);
    int64_t honker_cpp_ack_batch(sqlite3* db, const char* ids_json, const char* worker_id);
    int64_t honker_cpp_retry(sqlite3* db, int64_t job_id, const char* worker_id,
                             int64_t delay_sec, const char* error);
    int64_t honker_cpp_fail(sqlite3* db, int64_t job_id, const char* worker_id,
                            const char* error);
    int64_t honker_cpp_heartbeat(sqlite3* db, int64_t job_id, const char* worker_id,
                                  int64_t extend_sec);
    int64_t honker_cpp_sweep_expired(sqlite3* db, const char* queue);

    int64_t honker_cpp_stream_publish(
        sqlite3* db, const char* topic, const char* key, const char* payload_json);
    char*   honker_cpp_stream_read_since(
        sqlite3* db, const char* topic, int64_t offset, int64_t limit);
    int64_t honker_cpp_stream_save_offset(
        sqlite3* db, const char* consumer, const char* topic, int64_t offset);
    int64_t honker_cpp_stream_get_offset(
        sqlite3* db, const char* consumer, const char* topic);

    int64_t honker_cpp_scheduler_register(
        sqlite3* db, const char* name, const char* queue, const char* cron,
        const char* payload_json, int64_t priority, int64_t expires_sec,
        int64_t max_attempts);
    int64_t honker_cpp_scheduler_unregister(sqlite3* db, const char* name);
    char*   honker_cpp_scheduler_tick(sqlite3* db, int64_t now_unix);
    int64_t honker_cpp_scheduler_soonest(sqlite3* db);

    // Phase Mantle
    int64_t honker_cpp_scheduler_pause(sqlite3* db, const char* name);
    int64_t honker_cpp_scheduler_resume(sqlite3* db, const char* name);
    char*   honker_cpp_scheduler_list(sqlite3* db);
    int64_t honker_cpp_scheduler_update(
        sqlite3* db, const char* name, const char* cron, const char* payload_json,
        int64_t priority, int64_t touch_priority,
        int64_t expires_sec, int64_t touch_expires,
        int64_t max_attempts, int64_t touch_max_attempts);
    int64_t honker_cpp_cancel(sqlite3* db, int64_t job_id);
    char*   honker_cpp_get_job(sqlite3* db, int64_t job_id);

    int64_t honker_cpp_lock_acquire(
        sqlite3* db, const char* name, const char* owner, int64_t ttl_sec);
    int64_t honker_cpp_lock_release(
        sqlite3* db, const char* name, const char* owner);
    int64_t honker_cpp_lock_heartbeat(
        sqlite3* db, const char* name, const char* owner, int64_t ttl_sec);

    int64_t honker_cpp_rate_limit_try(
        sqlite3* db, const char* name, int64_t limit, int64_t per_sec);

    int64_t honker_cpp_result_save(
        sqlite3* db, int64_t job_id, const char* value_json, int64_t ttl_sec);
    char*   honker_cpp_result_get(sqlite3* db, int64_t job_id);
    int64_t honker_cpp_result_sweep(sqlite3* db);

    int64_t honker_cpp_notify(
        sqlite3* db, const char* channel, const char* payload_json);

    void    honker_cpp_free(char* ptr);
}

namespace honker {

// =====================================================================
// Error
// =====================================================================

class Error : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

// =====================================================================
// Forward declarations
// =====================================================================

class Database;
class Transaction;
class JobSnapshot;
class Job;
class Queue;
class Outbox;
class StreamEvent;
class Stream;
class StreamSubscription;
class ScheduledFire;
class Scheduler;
class Lock;
class Notification;
class Subscription;

using watcher_open_fn = void* (*)(const char*, const char*, uint64_t, char*, uintptr_t);
using watcher_wait_fn = int (*)(void*, uint64_t);
using watcher_close_fn = void (*)(void*);

inline std::mutex& native_open_mutex() {
    static std::mutex m;
    return m;
}

// =====================================================================
// Database
// =====================================================================

class Database {
public:
    Database(
        std::string_view path,
        std::string_view ext_path,
        std::string_view watcher_backend = "",
        uint64_t watcher_poll_interval_ms = 1) {
        const std::string p{path};
        const std::string e{ext_path};
        std::lock_guard<std::mutex> open_lock(native_open_mutex());
        if (auto rc = honker_cpp_open(p.c_str(), e.c_str(), &db_); rc != 0) {
            throw Error{"honker_cpp_open failed: code " + std::to_string(rc)};
        }
        path_ = p;
        ext_path_ = e;
        watcher_backend_ = std::string{watcher_backend};
        watcher_poll_interval_ms_ = watcher_poll_interval_ms;
        try {
            open_core_watcher();
        } catch (...) {
            // Not a swallow: close() releases the handle we just opened,
            // then the original failure is rethrown to the caller.
            close();
            throw;
        }
    }

    ~Database() { close(); }

    Database(Database&& other) noexcept
        : db_(other.db_), path_(std::move(other.path_)),
          ext_path_(std::move(other.ext_path_)),
          watcher_backend_(std::move(other.watcher_backend_)),
          watcher_poll_interval_ms_(other.watcher_poll_interval_ms_),
          watcher_lib_(other.watcher_lib_),
          core_watcher_(other.core_watcher_),
          watcher_wait_(other.watcher_wait_),
          watcher_close_(other.watcher_close_),
          update_watcher_(std::move(other.update_watcher_)),
          update_watcher_active_(other.update_watcher_active_.load()) {
        other.db_ = nullptr;
        other.watcher_lib_ = nullptr;
        other.core_watcher_ = nullptr;
        other.update_watcher_active_ = false;
    }

    Database& operator=(Database&& other) noexcept {
        if (this != &other) {
            close();
            db_ = other.db_;
            path_ = std::move(other.path_);
            ext_path_ = std::move(other.ext_path_);
            watcher_backend_ = std::move(other.watcher_backend_);
            watcher_poll_interval_ms_ = other.watcher_poll_interval_ms_;
            watcher_lib_ = other.watcher_lib_;
            core_watcher_ = other.core_watcher_;
            watcher_wait_ = other.watcher_wait_;
            watcher_close_ = other.watcher_close_;
            update_watcher_ = std::move(other.update_watcher_);
            update_watcher_active_ = other.update_watcher_active_.load();
            other.db_ = nullptr;
            other.watcher_lib_ = nullptr;
            other.core_watcher_ = nullptr;
            other.update_watcher_active_ = false;
        }
        return *this;
    }

    Database(const Database&) = delete;
    Database& operator=(const Database&) = delete;

    Queue queue(std::string_view name,
                int64_t visibility_timeout_s = 300,
                int64_t max_attempts = 3);

    Outbox outbox(std::string_view name,
                  std::function<void(const nlohmann::json&)> delivery,
                  int64_t visibility_timeout_s = 60,
                  int64_t max_attempts = 5,
                  int64_t base_backoff_s = 5);

    Transaction begin();

    Stream stream(std::string_view name);

    Scheduler scheduler();

    std::optional<Lock> try_lock(std::string_view name,
                                  std::string_view owner,
                                  int64_t ttl_sec = 60);

    bool try_rate_limit(std::string_view name, int64_t limit, int64_t per_sec);

    int64_t save_result(int64_t job_id, std::string_view value_json, int64_t ttl_sec = 0);
    std::optional<std::string> get_result(int64_t job_id);
    int64_t sweep_results();

    int64_t notify(std::string_view channel, std::string_view payload_json);

    Subscription listen(std::string_view channel);

    sqlite3* raw() const noexcept { return db_; }
    const std::string& path() const noexcept { return path_; }

    // Internal: start/stop update watcher thread for stream subscriptions.
    void start_update_watcher();
    void stop_update_watcher();
    bool wait_update(std::chrono::milliseconds timeout);
    void mark_updated();

private:
    void open_core_watcher();

    void close_watcher_lib() {
#if defined(_WIN32)
        if (watcher_lib_) {
            FreeLibrary(static_cast<HMODULE>(watcher_lib_));
            watcher_lib_ = nullptr;
        }
#else
        if (watcher_lib_) {
            dlclose(watcher_lib_);
            watcher_lib_ = nullptr;
        }
#endif
        watcher_wait_ = nullptr;
        watcher_close_ = nullptr;
    }

    void close() {
        if (db_) {
            stop_update_watcher();
            if (core_watcher_ && watcher_close_) {
                watcher_close_(core_watcher_);
                core_watcher_ = nullptr;
            }
            close_watcher_lib();
            honker_cpp_close(db_);
            db_ = nullptr;
        }
    }

    sqlite3* db_ = nullptr;
    std::string path_;
    std::string ext_path_;
    std::string watcher_backend_;
    uint64_t watcher_poll_interval_ms_ = 1;
    void* watcher_lib_ = nullptr;
    void* core_watcher_ = nullptr;
    watcher_wait_fn watcher_wait_ = nullptr;
    watcher_close_fn watcher_close_ = nullptr;
    std::thread update_watcher_;
    std::atomic<bool> update_watcher_active_{false};
    std::mutex update_mtx_;
    std::condition_variable update_cv_;
    bool update_changed_ = false;
};

// =====================================================================
// Transaction
// =====================================================================

class Transaction {
public:
    explicit     Transaction(sqlite3* db) : db_(db) {
        if (auto rc = honker_cpp_begin_tx(db_); rc != 0) {
            throw Error{"begin_tx failed: code " + std::to_string(rc)};
        }
    }

    ~Transaction() {
        if (!done_) {
            honker_cpp_rollback_tx(db_);
        }
    }

    void commit() {
        if (done_) return;
        if (auto rc = honker_cpp_commit_tx(db_); rc != 0) {
            throw Error{"commit failed: code " + std::to_string(rc)};
        }
        done_ = true;
    }

    void rollback() {
        if (done_) return;
        if (auto rc = honker_cpp_rollback_tx(db_); rc != 0) {
            throw Error{"rollback failed: code " + std::to_string(rc)};
        }
        done_ = true;
    }

    sqlite3* raw() const noexcept { return db_; }

    Transaction(const Transaction&) = delete;
    Transaction& operator=(const Transaction&) = delete;
    Transaction(Transaction&&) = delete;
    Transaction& operator=(Transaction&&) = delete;

private:
    sqlite3* db_;
    bool done_ = false;
};

// =====================================================================
// Job field decoding
// =====================================================================
//
// honker_claim_batch() and honker_get_job() return the same twelve
// snake_case keys. These helpers read them without inventing values:
// a required key that is absent or null is a bug in the core we are
// talking to, so it throws rather than defaulting to 0 or "".

namespace detail {

/// Look up a required key on one decoded row. `what` names the row
/// kind so the message says which decoder failed ("job row",
/// "stream event", "scheduler fire").
inline const nlohmann::json& row_field(
    const nlohmann::json& j, const char* key, const char* what) {
    if (!j.is_object()) {
        throw Error{std::string{what} + " is not a JSON object"};
    }
    const auto it = j.find(key);
    if (it == j.end() || it->is_null()) {
        throw Error{std::string{what} + " is missing required field '" + key + "'"};
    }
    return *it;
}

/// nlohmann throws its own nlohmann::json::type_error when a present
/// value has the wrong type. Callers are told to catch honker::Error,
/// so translate here rather than leaking the JSON library's exception
/// type out of honker's API.
template <typename T>
T row_get(const nlohmann::json& v, const char* key, const char* what, const char* want) {
    try {
        return v.get<T>();
    } catch (const nlohmann::json::exception& e) {
        throw Error{std::string{what} + " field '" + key + "' is not " + want +
                    ": " + e.what()};
    }
}

inline int64_t row_i64(const nlohmann::json& j, const char* key, const char* what) {
    return row_get<int64_t>(row_field(j, key, what), key, what, "an integer");
}

inline std::string row_str(const nlohmann::json& j, const char* key, const char* what) {
    return row_get<std::string>(row_field(j, key, what), key, what, "a string");
}

/// A genuinely nullable key: absent or null means std::nullopt, never
/// 0 or "". The row still has to be an object — otherwise every
/// nullable field on a garbage row would silently read as null, which
/// is the tolerant lookup this file exists to avoid.
inline std::optional<int64_t> row_opt_i64(
    const nlohmann::json& j, const char* key, const char* what) {
    if (!j.is_object()) {
        throw Error{std::string{what} + " is not a JSON object"};
    }
    const auto it = j.find(key);
    if (it == j.end() || it->is_null()) return std::nullopt;
    return row_get<int64_t>(*it, key, what, "an integer");
}

inline std::optional<std::string> row_opt_str(
    const nlohmann::json& j, const char* key, const char* what) {
    if (!j.is_object()) {
        throw Error{std::string{what} + " is not a JSON object"};
    }
    const auto it = j.find(key);
    if (it == j.end() || it->is_null()) return std::nullopt;
    return row_get<std::string>(*it, key, what, "a string");
}

constexpr const char* kJobRow = "job row";

inline int64_t job_i64(const nlohmann::json& j, const char* key) {
    return row_i64(j, key, kJobRow);
}

inline std::string job_str(const nlohmann::json& j, const char* key) {
    return row_str(j, key, kJobRow);
}

inline std::optional<int64_t> job_opt_i64(const nlohmann::json& j, const char* key) {
    return row_opt_i64(j, key, kJobRow);
}

inline std::optional<std::string> job_opt_str(const nlohmann::json& j, const char* key) {
    return row_opt_str(j, key, kJobRow);
}

/// Parse one honker JSON blob. A malformed blob is a bug in the core
/// we are talking to, not an empty result — surface it instead of
/// quietly handing back nothing.
inline nlohmann::json parse_row_blob(const std::string& json, const char* what) {
    try {
        return nlohmann::json::parse(json);
    } catch (const nlohmann::json::exception& e) {
        throw Error{std::string{what} + " JSON is not parseable: " + e.what()};
    }
}

inline nlohmann::json parse_row_array(const std::string& json, const char* what) {
    auto arr = parse_row_blob(json, what);
    if (!arr.is_array()) {
        throw Error{std::string{what} + " result is not a JSON array"};
    }
    return arr;
}

}  // namespace detail

// =====================================================================
// JobSnapshot
// =====================================================================

/// A read-only row from Queue::get_job(). Data only — a snapshot
/// holds no claim, so there is no ack/retry/fail/heartbeat here.
///
/// worker_id() and claim_expires_at() are optional because a pending
/// row has neither. payload() is the raw JSON text exactly as stored;
/// parse it with your preferred JSON library.
class JobSnapshot {
public:
    int64_t     id()         const noexcept { return id_; }
    std::string queue()      const noexcept { return queue_; }
    std::string payload()    const noexcept { return payload_; }
    /// "pending" or "processing".
    std::string state()      const noexcept { return state_; }
    int64_t     priority()   const noexcept { return priority_; }
    int64_t     run_at()     const noexcept { return run_at_; }
    std::optional<std::string> worker_id() const noexcept { return worker_id_; }
    std::optional<int64_t> claim_expires_at() const noexcept { return claim_expires_at_; }
    int64_t     attempts()     const noexcept { return attempts_; }
    int64_t     max_attempts() const noexcept { return max_attempts_; }
    int64_t     created_at()   const noexcept { return created_at_; }
    std::optional<int64_t> expires_at() const noexcept { return expires_at_; }

    /// Decode one honker_get_job() object. Throws honker::Error when a
    /// required field is missing.
    static JobSnapshot from_json(const nlohmann::json& j) {
        JobSnapshot s;
        s.id_               = detail::job_i64(j, "id");
        s.queue_            = detail::job_str(j, "queue");
        s.payload_          = detail::job_str(j, "payload");
        s.state_            = detail::job_str(j, "state");
        s.priority_         = detail::job_i64(j, "priority");
        s.run_at_           = detail::job_i64(j, "run_at");
        s.worker_id_        = detail::job_opt_str(j, "worker_id");
        s.claim_expires_at_ = detail::job_opt_i64(j, "claim_expires_at");
        s.attempts_         = detail::job_i64(j, "attempts");
        s.max_attempts_     = detail::job_i64(j, "max_attempts");
        s.created_at_       = detail::job_i64(j, "created_at");
        s.expires_at_       = detail::job_opt_i64(j, "expires_at");
        return s;
    }

private:
    JobSnapshot() = default;

    int64_t     id_ = 0;
    std::string queue_;
    std::string payload_;
    std::string state_;
    int64_t     priority_ = 0;
    int64_t     run_at_ = 0;
    std::optional<std::string> worker_id_;
    std::optional<int64_t> claim_expires_at_;
    int64_t     attempts_ = 0;
    int64_t     max_attempts_ = 0;
    int64_t     created_at_ = 0;
    std::optional<int64_t> expires_at_;
};

// =====================================================================
// Job
// =====================================================================

/// A claimed unit of work. Carries the same twelve fields as
/// JobSnapshot plus the claim methods. state() is always "processing",
/// and worker_id() / claim_expires_at() are never null because holding
/// a claim is what makes this a Job.
///
/// payload() is the raw JSON text exactly as stored.
class Job {
public:
    int64_t     id()        const noexcept { return id_; }
    std::string queue()     const noexcept { return queue_; }
    std::string payload()   const noexcept { return payload_; }
    /// Always "processing" for a claimed job.
    std::string state()     const noexcept { return state_; }
    int64_t     priority()  const noexcept { return priority_; }
    int64_t     run_at()    const noexcept { return run_at_; }
    std::string worker_id() const noexcept { return worker_id_; }
    int64_t     claim_expires_at() const noexcept { return claim_expires_at_; }
    int64_t     attempts()     const noexcept { return attempts_; }
    int64_t     max_attempts() const noexcept { return max_attempts_; }
    int64_t     created_at()   const noexcept { return created_at_; }
    std::optional<int64_t> expires_at() const noexcept { return expires_at_; }

    bool ack() {
        const auto n = honker_cpp_ack(db_, id_, worker_id_.c_str());
        if (n < 0) throw Error{"ack failed: SQL error"};
        return n > 0;
    }

    bool retry(int64_t delay_sec, std::string_view error) {
        const std::string e{error};
        const auto n = honker_cpp_retry(db_, id_, worker_id_.c_str(), delay_sec, e.c_str());
        if (n < 0) throw Error{"retry failed: SQL error"};
        return n > 0;
    }

    bool fail(std::string_view error) {
        const std::string e{error};
        const auto n = honker_cpp_fail(db_, id_, worker_id_.c_str(), e.c_str());
        if (n < 0) throw Error{"fail failed: SQL error"};
        return n > 0;
    }

    bool heartbeat(int64_t extend_sec) {
        const auto n = honker_cpp_heartbeat(db_, id_, worker_id_.c_str(), extend_sec);
        if (n < 0) throw Error{"heartbeat failed: SQL error"};
        return n > 0;
    }

    /// Decode one honker_claim_batch() element. Throws honker::Error
    /// when a required field is missing — including worker_id and
    /// claim_expires_at, which a claimed row always carries.
    static Job from_json(sqlite3* db, const nlohmann::json& j) {
        Job job;
        job.db_               = db;
        job.id_               = detail::job_i64(j, "id");
        job.queue_            = detail::job_str(j, "queue");
        job.payload_          = detail::job_str(j, "payload");
        job.state_            = detail::job_str(j, "state");
        job.priority_         = detail::job_i64(j, "priority");
        job.run_at_           = detail::job_i64(j, "run_at");
        job.worker_id_        = detail::job_str(j, "worker_id");
        job.claim_expires_at_ = detail::job_i64(j, "claim_expires_at");
        job.attempts_         = detail::job_i64(j, "attempts");
        job.max_attempts_     = detail::job_i64(j, "max_attempts");
        job.created_at_       = detail::job_i64(j, "created_at");
        job.expires_at_       = detail::job_opt_i64(j, "expires_at");
        return job;
    }

private:
    Job() = default;

    sqlite3*    db_ = nullptr;
    int64_t     id_ = 0;
    std::string queue_;
    std::string payload_;
    std::string state_;
    int64_t     priority_ = 0;
    int64_t     run_at_ = 0;
    std::string worker_id_;
    int64_t     claim_expires_at_ = 0;
    int64_t     attempts_ = 0;
    int64_t     max_attempts_ = 0;
    int64_t     created_at_ = 0;
    std::optional<int64_t> expires_at_;
};

// =====================================================================
// Queue
// =====================================================================

class Queue {
public:
    int64_t enqueue(std::string_view payload_json,
                    int64_t delay_sec = 0,
                    int64_t priority = 0) {
        const std::string p{payload_json};
        const auto id = honker_cpp_enqueue(
            db_, name_.c_str(), p.c_str(),
            delay_sec, priority, max_attempts_);
        if (id < 0) throw Error{"enqueue failed: code " + std::to_string(id)};
        return id;
    }

    int64_t enqueue_tx(Transaction& tx, std::string_view payload_json,
                       int64_t delay_sec = 0,
                       int64_t priority = 0) {
        const std::string p{payload_json};
        const auto id = honker_cpp_enqueue(
            tx.raw(), name_.c_str(), p.c_str(),
            delay_sec, priority, max_attempts_);
        if (id < 0) throw Error{"enqueue_tx failed: code " + std::to_string(id)};
        return id;
    }

    std::optional<Job> claim_one(std::string_view worker_id) {
        const std::string w{worker_id};
        char* rows = honker_cpp_claim_one(
            db_, name_.c_str(), w.c_str(), visibility_timeout_s_);
        if (!rows) return std::nullopt;
        std::string json{rows};
        honker_cpp_free(rows);
        auto jobs = parse_claim(json);
        if (jobs.empty()) return std::nullopt;
        return std::move(jobs.front());
    }

    std::vector<Job> claim_batch(std::string_view worker_id, int64_t n) {
        const std::string w{worker_id};
        char* rows = honker_cpp_claim_batch(
            db_, name_.c_str(), w.c_str(), n, visibility_timeout_s_);
        if (!rows) return {};
        std::string json{rows};
        honker_cpp_free(rows);
        return parse_claim(json);
    }

    int64_t ack_batch(const std::vector<int64_t>& ids, std::string_view worker_id) {
        nlohmann::json j = ids;
        const std::string ids_json = j.dump();
        const std::string w{worker_id};
        const auto n = honker_cpp_ack_batch(db_, ids_json.c_str(), w.c_str());
        if (n < 0) throw Error{"ack_batch failed: SQL error"};
        return n;
    }

    int64_t sweep_expired() {
        const auto n = honker_cpp_sweep_expired(db_, name_.c_str());
        if (n < 0) throw Error{"sweep_expired failed: SQL error"};
        return n;
    }

    int64_t save_result(int64_t job_id, std::string_view value_json, int64_t ttl_sec = 0) {
        const std::string v{value_json};
        const auto n = honker_cpp_result_save(db_, job_id, v.c_str(), ttl_sec);
        if (n < 0) throw Error{"save_result failed: SQL error"};
        return n;
    }

    /// Delete a pending or processing job by id. Returns true iff a row
    /// was removed. Idempotent on missing.
    ///
    /// IMPORTANT: cancel does NOT interrupt a worker currently running
    /// the handler. It invalidates the worker's claim — its next
    /// ack/heartbeat returns false. If you need the handler to actually
    /// halt, build that signal in your app.
    bool cancel(int64_t job_id) {
        const auto rc = honker_cpp_cancel(db_, job_id);
        if (rc < 0) throw Error{"cancel failed: SQL error"};
        return rc > 0;
    }

    /// Read a single job row by id. Returns the raw JSON-string blob
    /// or an empty string on miss. Use get_job() for the decoded
    /// JobSnapshot; this stays for callers that want the bytes.
    std::string get_job_json(int64_t job_id) {
        char* raw = honker_cpp_get_job(db_, job_id);
        if (!raw) return {};
        std::string out{raw};
        honker_cpp_free(raw);
        return out;
    }

    /// Read a single job row by id as a decoded JobSnapshot. Returns
    /// nullopt on miss (ack'd, dead'd, or never existed).
    ///
    /// NOT queue-scoped: job ids are globally unique and this lookup
    /// hits any queue's row. Scoping is tracked in #134.
    std::optional<JobSnapshot> get_job(int64_t job_id) {
        const std::string json = get_job_json(job_id);
        if (json.empty()) return std::nullopt;
        return JobSnapshot::from_json(detail::parse_row_blob(json, "job row"));
    }

    Queue(sqlite3* db, std::string name, int64_t vis, int64_t max)
        : db_(db), name_(std::move(name)),
          visibility_timeout_s_(vis), max_attempts_(max) {}

private:
    std::vector<Job> parse_claim(const std::string& json) {
        auto arr = detail::parse_row_array(json, "claim");
        std::vector<Job> out;
        out.reserve(arr.size());
        for (const auto& j : arr) {
            out.push_back(Job::from_json(db_, j));
        }
        return out;
    }

    sqlite3*    db_;
    std::string name_;
    int64_t     visibility_timeout_s_;
    int64_t     max_attempts_;
};

// =====================================================================
// Outbox
// =====================================================================

class Outbox {
public:
    int64_t enqueue(std::string_view payload_json,
                    int64_t delay_sec = 0,
                    int64_t priority = 0) {
        return queue_.enqueue(payload_json, delay_sec, priority);
    }

    int64_t enqueue_tx(Transaction& tx, std::string_view payload_json,
                       int64_t delay_sec = 0,
                       int64_t priority = 0) {
        return queue_.enqueue_tx(tx, payload_json, delay_sec, priority);
    }

    bool run_once(std::string_view worker_id) {
        auto job = queue_.claim_one(worker_id);
        if (!job.has_value()) return false;
        try {
            delivery_(nlohmann::json::parse(job->payload()));
            if (!job->ack()) throw Error{"outbox ack failed"};
        } catch (const std::exception& e) {
            if (!job->retry(retry_delay(job->attempts()), e.what())) {
                throw Error{"outbox retry failed"};
            }
        }
        return true;
    }

    const std::string& name() const noexcept { return name_; }
    Queue& queue() noexcept { return queue_; }
    const Queue& queue() const noexcept { return queue_; }

    Outbox(Queue queue, std::string name,
           std::function<void(const nlohmann::json&)> delivery,
           int64_t base_backoff_s)
        : queue_(std::move(queue)), name_(std::move(name)),
          delivery_(std::move(delivery)), base_backoff_s_(base_backoff_s) {
        if (!delivery_) throw Error{"outbox delivery is empty"};
    }

private:
    int64_t retry_delay(int64_t attempts) const {
        if (base_backoff_s_ <= 0) return 0;
        int64_t delay = base_backoff_s_;
        for (int64_t i = 1; i < attempts && delay < (1LL << 60); ++i) {
            delay *= 2;
        }
        return delay;
    }

    Queue queue_;
    std::string name_;
    std::function<void(const nlohmann::json&)> delivery_;
    int64_t base_backoff_s_;
};

// =====================================================================
// StreamEvent
// =====================================================================

class StreamEvent {
public:
    int64_t     offset()    const noexcept { return offset_; }
    std::string topic()     const noexcept { return topic_; }
    std::string key()       const noexcept { return key_; }
    std::string payload()   const noexcept { return payload_; }
    int64_t     created_at() const noexcept { return created_at_; }

    StreamEvent(int64_t offset, std::string topic, std::string key,
                std::string payload, int64_t created_at)
        : offset_(offset), topic_(std::move(topic)), key_(std::move(key)),
          payload_(std::move(payload)), created_at_(created_at) {}

private:
    int64_t     offset_;
    std::string topic_;
    std::string key_;
    std::string payload_;
    int64_t     created_at_;
};

namespace detail {

/// Decode one honker_stream_read_since() blob. Shared by Stream::read*
/// and StreamSubscription::read_batch so both fail the same way.
///
/// A malformed row used to be swallowed, which turned a bad row into
/// offset 0 — and a consumer that saved that offset replayed the whole
/// topic. Throw instead. `key` stays optional because the core writes
/// NULL for an unkeyed event.
inline std::vector<StreamEvent> parse_stream_events(const std::string& json) {
    auto arr = parse_row_array(json, "stream read");
    std::vector<StreamEvent> out;
    out.reserve(arr.size());
    for (const auto& j : arr) {
        out.emplace_back(
            row_i64(j, "offset", "stream event"),
            row_str(j, "topic", "stream event"),
            row_opt_str(j, "key", "stream event").value_or(std::string{}),
            row_str(j, "payload", "stream event"),
            row_i64(j, "created_at", "stream event"));
    }
    return out;
}

}  // namespace detail

// =====================================================================
// Stream
// =====================================================================

class Stream {
public:
    int64_t publish(std::string_view payload_json, std::optional<std::string_view> key = std::nullopt) {
        const std::string p{payload_json};
        const std::string k = key ? std::string{*key} : "";
        const auto off = honker_cpp_stream_publish(
            db_, name_.c_str(), key ? k.c_str() : nullptr, p.c_str());
        if (off < 0) throw Error{"stream_publish failed: SQL error"};
        return off;
    }

    int64_t publish_tx(Transaction& tx, std::string_view payload_json,
                       std::optional<std::string_view> key = std::nullopt) {
        const std::string p{payload_json};
        const std::string k = key ? std::string{*key} : "";
        const auto off = honker_cpp_stream_publish(
            tx.raw(), name_.c_str(), key ? k.c_str() : nullptr, p.c_str());
        if (off < 0) throw Error{"stream_publish_tx failed: SQL error"};
        return off;
    }

    std::vector<StreamEvent> read_since(int64_t offset, int64_t limit = 1000) {
        char* rows = honker_cpp_stream_read_since(db_, name_.c_str(), offset, limit);
        if (!rows) return {};
        return parse_events(rows);
    }

    std::vector<StreamEvent> read_from_consumer(std::string_view consumer, int64_t limit = 1000) {
        const auto offset = get_offset(consumer);
        return read_since(offset, limit);
    }

    bool save_offset(std::string_view consumer, int64_t offset) {
        const std::string c{consumer};
        const auto n = honker_cpp_stream_save_offset(db_, c.c_str(), name_.c_str(), offset);
        if (n < 0) throw Error{"save_offset failed: SQL error"};
        return n > 0;
    }

    bool save_offset_tx(Transaction& tx, std::string_view consumer, int64_t offset) {
        const std::string c{consumer};
        const auto n = honker_cpp_stream_save_offset(tx.raw(), c.c_str(), name_.c_str(), offset);
        if (n < 0) throw Error{"save_offset_tx failed: SQL error"};
        return n > 0;
    }

    int64_t get_offset(std::string_view consumer) {
        const std::string c{consumer};
        return honker_cpp_stream_get_offset(db_, c.c_str(), name_.c_str());
    }

    StreamSubscription subscribe(std::string_view consumer,
                                  int64_t save_every_n = 1000,
                                  std::chrono::milliseconds poll_interval = std::chrono::milliseconds(100));

    Stream(Database* owner, sqlite3* db, std::string name)
        : owner_(owner), db_(db), name_(std::move(name)) {}

private:
    std::vector<StreamEvent> parse_events(char* rows) {
        std::string json{rows};
        honker_cpp_free(rows);
        return detail::parse_stream_events(json);
    }

    Database*   owner_;
    sqlite3*    db_;
    std::string name_;
};

// =====================================================================
// StreamSubscription
// =====================================================================

class StreamSubscription {
public:
    StreamSubscription(Database* owner, sqlite3* db, std::string topic, std::string consumer,
                       int64_t save_every_n, std::chrono::milliseconds poll_interval)
        : owner_(owner), db_(db), topic_(std::move(topic)), consumer_(std::move(consumer)),
          save_every_n_(save_every_n), poll_interval_(poll_interval) {
        owner_->start_update_watcher();
        last_offset_ = honker_cpp_stream_get_offset(db_, consumer_.c_str(), topic_.c_str());
    }

    ~StreamSubscription() {
        if (pending_ > 0) {
            // A destructor may not throw. The lost checkpoint is
            // survivable — the consumer replays from the last saved
            // offset. Call save_offset() before the subscription goes
            // out of scope if you need the failure reported.
            try { flush_offset(); } catch (...) {}
        }
    }

    /// Blocks until the next event. Throws honker::Error if the
    /// every-save_every_n checkpoint write fails. The event is not
    /// consumed in that case: the in-memory position rolls back so a
    /// later next() hands out the same event rather than skipping it.
    std::optional<StreamEvent> next() {
        while (true) {
            if (idx_ < buffer_.size()) {
                const int64_t prev_offset = last_offset_;
                last_offset_ = buffer_[idx_].offset();
                ++pending_;
                if (pending_ >= save_every_n_) {
                    try {
                        flush_offset();
                    } catch (...) {
                        --pending_;
                        last_offset_ = prev_offset;
                        throw;
                    }
                }
                auto ev = std::move(buffer_[idx_]);
                ++idx_;
                return ev;
            }
            buffer_ = read_batch();
            idx_ = 0;
            if (buffer_.empty()) {
                owner_->wait_update(poll_interval_);
            }
        }
    }

    /// Persist the consumer's position now. Throws honker::Error if the
    /// checkpoint write fails — a silently dropped checkpoint replays
    /// the topic, so call this before the subscription goes out of
    /// scope when you need the failure reported; the destructor cannot
    /// throw and has to swallow it.
    void save_offset() {
        flush_offset();
    }

    int64_t offset() const noexcept { return last_offset_; }

private:
    std::vector<StreamEvent> read_batch() {
        char* rows = honker_cpp_stream_read_since(db_, topic_.c_str(), last_offset_, 100);
        if (!rows) return {};
        std::string json{rows};
        honker_cpp_free(rows);
        return detail::parse_stream_events(json);
    }

    /// Persist the checkpoint. A failed write must not clear pending_:
    /// the offset is still unsaved, so the destructor gets another try
    /// and offset() keeps reporting the uncommitted position.
    void flush_offset() {
        if (pending_ == 0) return;
        const auto n = honker_cpp_stream_save_offset(
            db_, consumer_.c_str(), topic_.c_str(), last_offset_);
        if (n < 0) throw Error{"save_offset failed: SQL error"};
        pending_ = 0;
    }

    Database*   owner_;
    sqlite3*    db_;
    std::string topic_;
    std::string consumer_;
    int64_t     save_every_n_;
    std::chrono::milliseconds poll_interval_;
    int64_t     last_offset_ = 0;
    int64_t     pending_ = 0;
    std::vector<StreamEvent> buffer_;
    std::size_t idx_ = 0;
};

// =====================================================================
// ScheduledFire
// =====================================================================

class ScheduledFire {
public:
    std::string name()      const noexcept { return name_; }
    std::string queue()     const noexcept { return queue_; }
    int64_t     fire_at()   const noexcept { return fire_at_; }
    int64_t     job_id()    const noexcept { return job_id_; }

    ScheduledFire(std::string name, std::string queue, int64_t fire_at, int64_t job_id)
        : name_(std::move(name)), queue_(std::move(queue)),
          fire_at_(fire_at), job_id_(job_id) {}

private:
    std::string name_;
    std::string queue_;
    int64_t     fire_at_;
    int64_t     job_id_;
};

namespace detail {

/// Decode one honker_scheduler_tick() blob. Every field is required: a
/// fire with name "" or job_id 0 is not a plausible default, it is a
/// row we failed to read.
inline std::vector<ScheduledFire> parse_scheduler_fires(const std::string& json) {
    auto arr = parse_row_array(json, "scheduler tick");
    std::vector<ScheduledFire> out;
    out.reserve(arr.size());
    for (const auto& j : arr) {
        out.emplace_back(
            row_str(j, "name", "scheduler fire"),
            row_str(j, "queue", "scheduler fire"),
            row_i64(j, "fire_at", "scheduler fire"),
            row_i64(j, "job_id", "scheduler fire"));
    }
    return out;
}

}  // namespace detail

// =====================================================================
// Scheduler
// =====================================================================

class Scheduler {
public:
    void add(std::string_view name, std::string_view queue, std::string_view schedule_expr,
             std::string_view payload_json, int64_t priority = 0,
             std::optional<int64_t> expires_sec = std::nullopt,
             int64_t max_attempts = 3) {
        const std::string n{name};
        const std::string q{queue};
        const std::string c{schedule_expr};
        const std::string p{payload_json};
        const auto rc = honker_cpp_scheduler_register(
            db_->raw(), n.c_str(), q.c_str(), c.c_str(), p.c_str(), priority,
            expires_sec.value_or(0), max_attempts);
        if (rc < 0) throw Error{"scheduler_register failed: SQL error"};
        db_->mark_updated();
    }

    int64_t remove(std::string_view name) {
        const std::string n{name};
        const auto rc = honker_cpp_scheduler_unregister(db_->raw(), n.c_str());
        if (rc < 0) throw Error{"scheduler_unregister failed: SQL error"};
        db_->mark_updated();
        return rc;
    }

    std::vector<ScheduledFire> tick(int64_t now_unix) {
        char* rows = honker_cpp_scheduler_tick(db_->raw(), now_unix);
        if (!rows) return {};
        return parse_fires(rows);
    }

    int64_t soonest() {
        return honker_cpp_scheduler_soonest(db_->raw());
    }

    // ---- Phase Mantle: lifecycle methods ----

    /// Pause a registered schedule. Returns true iff a row was paused.
    bool pause(std::string_view name) {
        const std::string n{name};
        const auto rc = honker_cpp_scheduler_pause(db_->raw(), n.c_str());
        if (rc < 0) throw Error{"scheduler_pause failed: SQL error"};
        if (rc > 0) db_->mark_updated();
        return rc > 0;
    }

    /// Resume a paused schedule. Returns true iff a row was resumed.
    bool resume(std::string_view name) {
        const std::string n{name};
        const auto rc = honker_cpp_scheduler_resume(db_->raw(), n.c_str());
        if (rc < 0) throw Error{"scheduler_resume failed: SQL error"};
        if (rc > 0) db_->mark_updated();
        return rc > 0;
    }

    /// Return every registered schedule as a JSON-string blob. Caller
    /// parses with their preferred JSON library — keeping this
    /// dependency-free in the header.
    std::string list_json() {
        char* raw = honker_cpp_scheduler_list(db_->raw());
        if (!raw) return "[]";
        std::string out{raw};
        honker_cpp_free(raw);
        return out;
    }

    /// Mutate one or more fields of an existing schedule. Pass nullopt
    /// for any field you want left alone; pass a value (or nullopt
    /// inside the inner option for expires) to clear vs set. Returns
    /// true iff a row was updated.
    ///
    /// IMPORTANT: cancel does NOT interrupt a worker mid-handler — see
    /// Queue::cancel.
    bool update(
        std::string_view name,
        std::optional<std::string_view> cron = std::nullopt,
        std::optional<std::string_view> payload_json = std::nullopt,
        std::optional<int64_t> priority = std::nullopt,
        std::optional<std::optional<int64_t>> expires_sec = std::nullopt,
        std::optional<int64_t> max_attempts = std::nullopt
    ) {
        // Empty update is a no-op.
        if (!cron && !payload_json && !priority && !expires_sec && !max_attempts) return false;

        const std::string n{name};
        std::string c, p;
        const char* cron_z = nullptr;
        const char* payload_z = nullptr;
        if (cron) { c = std::string{*cron}; cron_z = c.c_str(); }
        if (payload_json) { p = std::string{*payload_json}; payload_z = p.c_str(); }
        const int64_t touch_priority = priority.has_value() ? 1 : 0;
        const int64_t touch_expires = expires_sec.has_value() ? 1 : 0;
        const int64_t expires_arg =
            (expires_sec.has_value() && expires_sec->has_value()) ? **expires_sec : -1;
        const int64_t touch_max_attempts = max_attempts.has_value() ? 1 : 0;

        const auto rc = honker_cpp_scheduler_update(
            db_->raw(), n.c_str(), cron_z, payload_z,
            priority.value_or(0), touch_priority,
            expires_arg, touch_expires,
            max_attempts.value_or(0), touch_max_attempts
        );
        if (rc < 0) throw Error{"scheduler_update failed: SQL error"};
        if (rc > 0) db_->mark_updated();
        return rc > 0;
    }

    void run(std::atomic<bool>& stop_token, std::string_view owner) {
        constexpr int64_t LOCK_TTL = 60;
        constexpr auto HEARTBEAT = std::chrono::seconds(20);
        const std::string o{owner};
        db_->start_update_watcher();

        while (!stop_token.load()) {
            auto acquired = honker_cpp_lock_acquire(
                db_->raw(), "honker-scheduler", o.c_str(), LOCK_TTL);
            if (acquired <= 0) {
                db_->wait_update(std::chrono::seconds(5));
                continue;
            }

            while (!stop_token.load()) {
                auto hb = honker_cpp_lock_heartbeat(
                    db_->raw(), "honker-scheduler", o.c_str(), LOCK_TTL);
                if (hb <= 0) {
                    // lost leadership before firing
                    break;
                }
                auto last_hb = std::chrono::steady_clock::now();

                auto now = std::chrono::system_clock::now().time_since_epoch();
                auto now_sec = std::chrono::duration_cast<std::chrono::seconds>(now).count();
                auto fires = tick(now_sec);
                (void)fires; // caller may process them if they override run()

                auto now_clock = std::chrono::steady_clock::now();
                auto wait = HEARTBEAT - (now_clock - last_hb);
                auto next_fire = soonest();
                if (next_fire > 0) {
                    auto fire_tp = std::chrono::system_clock::time_point{
                        std::chrono::seconds(next_fire)
                    };
                    auto until_fire = fire_tp - std::chrono::system_clock::now();
                    if (until_fire < std::chrono::seconds(0)) {
                        wait = std::chrono::seconds(0);
                    } else {
                        auto until_fire_ms = std::chrono::duration_cast<std::chrono::milliseconds>(until_fire);
                        auto wait_ms = std::chrono::duration_cast<std::chrono::milliseconds>(wait);
                        if (until_fire_ms < wait_ms) wait = until_fire_ms;
                    }
                }
                db_->wait_update(std::chrono::duration_cast<std::chrono::milliseconds>(wait));
            }

            honker_cpp_lock_release(db_->raw(), "honker-scheduler", o.c_str());
        }
    }

    Scheduler(Database* db) : db_(db) {}

private:
    std::vector<ScheduledFire> parse_fires(char* rows) {
        std::string json{rows};
        honker_cpp_free(rows);
        return detail::parse_scheduler_fires(json);
    }

    Database* db_;
};

// =====================================================================
// Lock
// =====================================================================

class Lock {
public:
    std::string name()  const noexcept { return name_; }
    std::string owner() const noexcept { return owner_; }

    bool release() {
        if (released_) return true;
        const auto n = honker_cpp_lock_release(db_, name_.c_str(), owner_.c_str());
        if (n < 0) throw Error{"lock_release failed: SQL error"};
        released_ = true;
        return n > 0;
    }

    bool heartbeat(int64_t ttl_sec) {
        if (released_) return false;
        const auto n = honker_cpp_lock_heartbeat(db_, name_.c_str(), owner_.c_str(), ttl_sec);
        if (n < 0) throw Error{"lock_heartbeat failed: SQL error"};
        return n > 0;
    }

    ~Lock() {
        if (!released_) {
            // A destructor may not throw. An unreleased lock still
            // expires on its TTL. Call release() explicitly if you need
            // the failure reported.
            try { release(); } catch (...) {}
        }
    }

    Lock(const Lock&) = delete;
    Lock& operator=(const Lock&) = delete;
    Lock(Lock&& other) noexcept
        : db_(other.db_), name_(std::move(other.name_)),
          owner_(std::move(other.owner_)), released_(other.released_) {
        other.released_ = true;
    }
    Lock& operator=(Lock&& other) noexcept {
        if (this != &other) {
            // noexcept move-assign: same reasoning as the destructor.
            if (!released_) { try { release(); } catch (...) {} }
            db_ = other.db_;
            name_ = std::move(other.name_);
            owner_ = std::move(other.owner_);
            released_ = other.released_;
            other.released_ = true;
        }
        return *this;
    }

private:
    friend class Database;
    Lock(sqlite3* db, std::string name, std::string owner)
        : db_(db), name_(std::move(name)), owner_(std::move(owner)) {}

    sqlite3*    db_;
    std::string name_;
    std::string owner_;
    bool        released_ = false;
};

// =====================================================================
// Notification
// =====================================================================

class Notification {
public:
    int64_t     id()       const noexcept { return id_; }
    std::string channel()  const noexcept { return channel_; }
    std::string payload()  const noexcept { return payload_; }

    Notification(int64_t id, std::string channel, std::string payload)
        : id_(id), channel_(std::move(channel)), payload_(std::move(payload)) {}

private:
    int64_t     id_;
    std::string channel_;
    std::string payload_;
};

// =====================================================================
// Subscription (listen)
// =====================================================================

class Subscription {
public:
    std::optional<Notification> recv(std::chrono::milliseconds timeout = std::chrono::milliseconds(5000)) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            char* rows = nullptr;
            {
                const std::string c{channel_};
                const std::string sql =
                    "SELECT id, channel, payload FROM _honker_notifications "
                    "WHERE channel = ? AND id > ? ORDER BY id LIMIT 1";
                sqlite3_stmt* stmt = nullptr;
                if (sqlite3_prepare_v2(db_->raw(), sql.c_str(), -1, &stmt, nullptr) == SQLITE_OK) {
                    sqlite3_bind_text(stmt, 1, c.c_str(), -1, SQLITE_STATIC);
                    sqlite3_bind_int64(stmt, 2, last_id_);
                if (sqlite3_step(stmt) == SQLITE_ROW) {
                    int64_t id = sqlite3_column_int64(stmt, 0);
                    std::string ch = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));
                    std::string pl = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 2));
                    last_id_ = id;
                    sqlite3_finalize(stmt);
                    return Notification{id, std::move(ch), std::move(pl)};
                }
                sqlite3_finalize(stmt);
                }
            }
            auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
                deadline - std::chrono::steady_clock::now());
            db_->wait_update(std::min(std::chrono::milliseconds(100), remaining));
        }
        return std::nullopt;
    }

private:
    friend class Database;
    Subscription(Database* db, std::string channel)
        : db_(db), channel_(std::move(channel)) {
        db_->start_update_watcher();
        // Attach at current max id so we only see new notifications.
        const std::string c{channel_};
        const std::string sql = "SELECT COALESCE(MAX(id), 0) FROM _honker_notifications WHERE channel = ?";
        sqlite3_stmt* stmt = nullptr;
        if (sqlite3_prepare_v2(db_->raw(), sql.c_str(), -1, &stmt, nullptr) == SQLITE_OK) {
            sqlite3_bind_text(stmt, 1, c.c_str(), -1, SQLITE_STATIC);
            if (sqlite3_step(stmt) == SQLITE_ROW) {
                last_id_ = sqlite3_column_int64(stmt, 0);
            }
            sqlite3_finalize(stmt);
        }
    }

    Database*   db_;
    std::string channel_;
    int64_t     last_id_ = 0;
};

// =====================================================================
// Database inline implementations
// =====================================================================

inline Queue Database::queue(std::string_view name,
                             int64_t visibility_timeout_s,
                             int64_t max_attempts) {
    return Queue{db_, std::string{name}, visibility_timeout_s, max_attempts};
}

inline Transaction Database::begin() {
    return Transaction{db_};
}

inline Outbox Database::outbox(std::string_view name,
                               std::function<void(const nlohmann::json&)> delivery,
                               int64_t visibility_timeout_s,
                               int64_t max_attempts,
                               int64_t base_backoff_s) {
    const std::string n{name};
    return Outbox{
        queue("_outbox:" + n, visibility_timeout_s, max_attempts),
        n,
        std::move(delivery),
        base_backoff_s,
    };
}

inline Stream Database::stream(std::string_view name) {
    return Stream{this, db_, std::string{name}};
}

inline Scheduler Database::scheduler() {
    return Scheduler{this};
}

inline std::optional<Lock> Database::try_lock(std::string_view name,
                                               std::string_view owner,
                                               int64_t ttl_sec) {
    const std::string n{name};
    const std::string o{owner};
    const auto rc = honker_cpp_lock_acquire(db_, n.c_str(), o.c_str(), ttl_sec);
    if (rc < 0) throw Error{"lock_acquire failed: SQL error"};
    if (rc == 0) return std::nullopt;
    return Lock{db_, n, o};
}

inline bool Database::try_rate_limit(std::string_view name, int64_t limit, int64_t per_sec) {
    const std::string n{name};
    const auto rc = honker_cpp_rate_limit_try(db_, n.c_str(), limit, per_sec);
    if (rc < 0) throw Error{"rate_limit_try failed: SQL error"};
    return rc > 0;
}

inline int64_t Database::save_result(int64_t job_id, std::string_view value_json, int64_t ttl_sec) {
    const std::string v{value_json};
    const auto rc = honker_cpp_result_save(db_, job_id, v.c_str(), ttl_sec);
    if (rc < 0) throw Error{"save_result failed: SQL error"};
    return rc;
}

inline std::optional<std::string> Database::get_result(int64_t job_id) {
    char* ptr = honker_cpp_result_get(db_, job_id);
    if (!ptr) return std::nullopt;
    std::string val{ptr};
    honker_cpp_free(ptr);
    return val;
}

inline int64_t Database::sweep_results() {
    return honker_cpp_result_sweep(db_);
}

inline int64_t Database::notify(std::string_view channel, std::string_view payload_json) {
    const std::string c{channel};
    const std::string p{payload_json};
    const auto rc = honker_cpp_notify(db_, c.c_str(), p.c_str());
    if (rc < 0) throw Error{"notify failed: SQL error"};
    return rc;
}

inline Subscription Database::listen(std::string_view channel) {
    return Subscription{this, std::string{channel}};
}

// =====================================================================
// Database commit watcher (internal)
// =====================================================================

inline void Database::open_core_watcher() {
    char err[1024] = {0};
#if defined(_WIN32)
    watcher_lib_ = static_cast<void*>(LoadLibraryA(ext_path_.c_str()));
    if (!watcher_lib_) {
        throw Error{"LoadLibrary failed for " + ext_path_};
    }
    auto open_fn = reinterpret_cast<watcher_open_fn>(
        GetProcAddress(static_cast<HMODULE>(watcher_lib_), "honker_watcher_open_v2"));
    watcher_wait_ = reinterpret_cast<watcher_wait_fn>(
        GetProcAddress(static_cast<HMODULE>(watcher_lib_), "honker_watcher_wait"));
    watcher_close_ = reinterpret_cast<watcher_close_fn>(
        GetProcAddress(static_cast<HMODULE>(watcher_lib_), "honker_watcher_close"));
#else
    watcher_lib_ = dlopen(ext_path_.c_str(), RTLD_NOW | RTLD_LOCAL);
    if (!watcher_lib_) {
        const char* msg = dlerror();
        throw Error{"dlopen failed for " + ext_path_ + ": " + (msg ? msg : "unknown error")};
    }
    auto open_fn = reinterpret_cast<watcher_open_fn>(dlsym(watcher_lib_, "honker_watcher_open_v2"));
    watcher_wait_ = reinterpret_cast<watcher_wait_fn>(dlsym(watcher_lib_, "honker_watcher_wait"));
    watcher_close_ = reinterpret_cast<watcher_close_fn>(dlsym(watcher_lib_, "honker_watcher_close"));
#endif
    if (!open_fn || !watcher_wait_ || !watcher_close_) {
        close_watcher_lib();
        throw Error{"Honker extension missing core watcher ABI symbols"};
    }
    core_watcher_ = open_fn(
        path_.c_str(),
        watcher_backend_.c_str(),
        watcher_poll_interval_ms_,
        err,
        sizeof(err));
    if (!core_watcher_) {
        close_watcher_lib();
        throw Error{std::string{"watcher_backend probe failed: "} + err};
    }
}

// Platform-specific file identity for the dead-man's switch.
#if defined(_WIN32)
inline bool file_identity(const std::string& path, uint64_t& dev, uint64_t& ino) {
    // Windows: std::filesystem doesn't expose volume serial or file index
    // cheaply. Best-effort: return zeros so the identity check is a no-op.
    (void)path;
    dev = 0;
    ino = 0;
    return true;
}
#else
#include <sys/stat.h>
inline bool file_identity(const std::string& path, uint64_t& dev, uint64_t& ino) {
    struct stat st;
    if (stat(path.c_str(), &st) != 0) return false;
    dev = static_cast<uint64_t>(st.st_dev);
    ino = static_cast<uint64_t>(st.st_ino);
    return true;
}
#endif

inline void Database::start_update_watcher() {
    if (update_watcher_active_.exchange(true)) return;
    update_watcher_ = std::thread([this]() {
        while (update_watcher_active_.load()) {
            int code = watcher_wait_(core_watcher_, 100);
            if (code == 1) {
                {
                    std::lock_guard<std::mutex> lk(update_mtx_);
                    update_changed_ = true;
                }
                update_cv_.notify_all();
            } else if (code == 0) {
                continue;
            } else {
                {
                    std::lock_guard<std::mutex> lk(update_mtx_);
                    update_changed_ = true;
                    update_watcher_active_ = false;
                }
                update_cv_.notify_all();
                return;
            }
        }
    });
}

inline void Database::stop_update_watcher() {
    if (!update_watcher_active_.exchange(false)) return;
    update_cv_.notify_all();
    if (update_watcher_.joinable()) update_watcher_.join();
}

inline bool Database::wait_update(std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lk(update_mtx_);
    const bool woke = update_cv_.wait_for(lk, timeout, [this]() { return update_changed_ || !update_watcher_active_.load(); });
    const bool changed = woke && update_changed_;
    update_changed_ = false;
    return changed;
}

inline void Database::mark_updated() {
    {
        std::lock_guard<std::mutex> lk(update_mtx_);
        update_changed_ = true;
    }
    update_cv_.notify_all();
}

// =====================================================================
// Stream inline implementations
// =====================================================================

inline StreamSubscription Stream::subscribe(std::string_view consumer,
                                            int64_t save_every_n,
                                            std::chrono::milliseconds poll_interval) {
    return StreamSubscription{owner_, db_, name_, std::string{consumer}, save_every_n, poll_interval};
}

} // namespace honker
