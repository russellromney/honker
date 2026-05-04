// Binding surface and watcher-backend smoke/e2e tests.

#include "honker.hpp"

#include <cassert>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <sys/wait.h>
#include <spawn.h>
#include <unistd.h>

namespace fs = std::filesystem;
extern char **environ;

static std::atomic<int> tmp_counter{0};

static fs::path tmp_db(std::string_view label) {
    const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    return fs::temp_directory_path() /
        ("honker-cpp-" + std::string{label} + "-" + std::to_string(now) + "-" +
         std::to_string(tmp_counter.fetch_add(1)) + ".db");
}

static void clean_db(const fs::path& p) {
    fs::remove(p);
    fs::remove(p.string() + "-wal");
    fs::remove(p.string() + "-shm");
}

static void enqueue_range(const fs::path& p, const char* ext, int first, int count) {
    honker::Database db{p.string(), ext};
    auto q = db.queue("shared");
    for (int i = first; i < first + count; ++i) {
        q.enqueue(std::string{"{\"i\":"} + std::to_string(i) + "}");
    }
}

static void enqueue_stops(const fs::path& p, const char* ext, int count) {
    honker::Database db{p.string(), ext};
    auto q = db.queue("shared");
    for (int i = 0; i < count; ++i) {
        q.enqueue(R"({"stop":true})");
    }
}

static std::vector<int> drain_queue_process(
    const fs::path& p,
    const char* ext,
    std::string backend,
    std::string worker_id,
    const fs::path& ready_path) {
    honker::Database db{p.string(), ext, backend};
    db.start_update_watcher();
    auto q = db.queue("shared");
    { std::ofstream ready{ready_path}; ready << "ready"; }
    std::vector<int> processed;
    while (true) {
        auto job = q.claim_one(worker_id);
        if (job.has_value()) {
            auto payload = nlohmann::json::parse(job->payload());
            assert(job->ack());
            if (payload.value("stop", false)) break;
            processed.push_back(payload.value("i", -1));
            continue;
        }
        assert(db.wait_update(std::chrono::seconds(10)) && "watcher timed out before stop job");
    }
    return processed;
}

static void assert_int_set(std::vector<int> values, int count) {
    std::sort(values.begin(), values.end());
    std::vector<int> expected;
    for (int i = 0; i < count; ++i) expected.push_back(i);
    assert(values == expected);
}

static std::vector<int> read_result(const fs::path& p) {
    std::ifstream in{p};
    std::vector<int> values;
    std::string line;
    while (std::getline(in, line)) {
        if (!line.empty()) values.push_back(std::stoi(line));
    }
    return values;
}

static void write_result(const fs::path& p, const std::vector<int>& values) {
    std::ofstream out{p};
    for (int value : values) out << value << '\n';
}

static void wait_ready(const fs::path& p) {
    for (int i = 0; i < 200; ++i) {
        if (fs::exists(p)) return;
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }
    assert(false && "worker did not become ready");
}

static pid_t spawn_process(const char* exe, const std::vector<std::string>& args) {
    std::vector<char*> argv;
    argv.push_back(const_cast<char*>(exe));
    for (const auto& arg : args) argv.push_back(const_cast<char*>(arg.c_str()));
    argv.push_back(nullptr);
    pid_t pid = 0;
    const int rc = posix_spawnp(&pid, exe, nullptr, nullptr, argv.data(), environ);
    if (rc != 0) {
        std::fprintf(stderr, "posix_spawnp failed: %s\n", std::strerror(rc));
        assert(false && "posix_spawnp failed");
    }
    return pid;
}

static void wait_process(pid_t pid) {
    int status = 0;
    assert(waitpid(pid, &status, 0) == pid);
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        if (WIFSIGNALED(status)) {
            std::fprintf(stderr, "child %d terminated by signal %d\n", pid, WTERMSIG(status));
        } else if (WIFEXITED(status)) {
            std::fprintf(stderr, "child %d exited with code %d\n", pid, WEXITSTATUS(status));
        } else {
            std::fprintf(stderr, "child %d failed with status %d\n", pid, status);
        }
        assert(false && "child process failed");
    }
}

static pid_t spawn_worker_process(
    const char* exe,
    const fs::path& p,
    const char* ext,
    const char* backend,
    const std::string& worker_id,
    const fs::path& ready,
    const fs::path& result) {
    return spawn_process(exe, {
        "--queue-worker",
        p.string(),
        ext,
        backend ? backend : "",
        worker_id,
        ready.string(),
        result.string(),
    });
}

static pid_t spawn_writer_process(const char* exe, const fs::path& p, const char* ext, int first, int count) {
    return spawn_process(exe, {
        "--queue-writer",
        p.string(),
        ext,
        std::to_string(first),
        std::to_string(count),
    });
}

int main(int argc, char** argv) {
    if (argc > 1 && std::string{argv[1]} == "--queue-worker") {
        assert(argc == 8);
        try {
            auto values = drain_queue_process(argv[2], argv[3], argv[4], argv[5], argv[6]);
            write_result(argv[7], values);
            return 0;
        } catch (const std::exception& e) {
            std::fprintf(stderr, "queue worker helper failed: %s\n", e.what());
            return 1;
        }
    }
    if (argc > 1 && std::string{argv[1]} == "--queue-writer") {
        assert(argc == 6);
        try {
            enqueue_range(argv[2], argv[3], std::stoi(argv[4]), std::stoi(argv[5]));
            return 0;
        } catch (const std::exception& e) {
            std::fprintf(stderr, "queue writer helper failed: %s\n", e.what());
            return 1;
        }
    }

    for (const char* backend : {"", "poll", "polling"}) {
        try {
            honker::Database db{"/tmp/honker-cpp-missing.db", "/missing/libhonker_ext.so", backend};
            std::fprintf(stderr, "expected missing extension failure for polling alias\n");
            return 1;
        } catch (const honker::Error& e) {
            const std::string msg{e.what()};
            assert(msg.find("watcher backend") == std::string::npos);
        }
    }

    const char* ext = std::getenv("HONKER_EXTENSION_PATH");
    if (!ext || !*ext) {
        std::fputs(
            "skip: HONKER_EXTENSION_PATH not set "
            "(export it to ./libhonker_ext.{dylib,so})\n",
            stderr);
        return 0;
    }

    for (const char* backend : {"bogus", "KERNEL", " polling "}) {
        try {
            honker::Database db{"/tmp/honker-cpp-unknown.db", ext, backend};
            std::fprintf(stderr, "expected unknown backend rejection\n");
            return 1;
        } catch (const honker::Error& e) {
            const std::string msg{e.what()};
            assert(msg.find("unknown watcher backend") != std::string::npos);
        }
    }

    for (const char* backend : {"", "poll", "polling", "kernel", "kernel-watcher", "shm", "shm-fast-path"}) {
        const auto p = tmp_db(std::string{"watch-"} + (backend && *backend ? backend : "default"));
        clean_db(p);
        try {
            honker::Database db{p.string(), ext, backend};
            auto sub = db.listen("backend");
            honker::Database writer{p.string(), ext};
            writer.notify("backend", R"({"ok":true})");
            auto n = sub.recv(std::chrono::seconds(2));
            assert(n.has_value() && "watcher backend should observe commit");
        } catch (const honker::Error& e) {
            const std::string msg{e.what()};
            if (msg.find("requires the") != std::string::npos ||
                msg.find("-shm unavailable") != std::string::npos ||
                msg.find("unsupported SQLite layout") != std::string::npos) {
                continue;
            }
            throw;
        }
        clean_db(p);
    }

    for (const char* backend : {"", "kernel", "shm"}) {
        const std::string label = backend && *backend ? backend : "default";

        {
            const auto p = tmp_db("queue-1x1-" + label);
            clean_db(p);
            try {
                { honker::Database db{p.string(), ext, backend}; db.queue("shared"); }
                std::optional<honker::Database> pin;
                if (label == "shm") pin.emplace(p.string(), ext);
                const auto ready = p.string() + ".w1.ready";
                const auto result = p.string() + ".w1.result";
                auto worker = spawn_worker_process(argv[0], p, ext, backend, "w1", ready, result);
                wait_ready(ready);
                enqueue_range(p, ext, 0, 25);
                enqueue_stops(p, ext, 1);
                wait_process(worker);
                assert_int_set(read_result(result), 25);
                pin.reset();
                fs::remove(ready);
                fs::remove(result);
            } catch (const honker::Error& e) {
                const std::string msg{e.what()};
                if (msg.find("requires the") == std::string::npos &&
                    msg.find("-shm unavailable") == std::string::npos &&
                    msg.find("unsupported SQLite layout") == std::string::npos) {
                    throw;
                }
            }
            clean_db(p);
        }

        {
            const auto p = tmp_db("queue-1xn-" + label);
            clean_db(p);
            try {
                { honker::Database db{p.string(), ext, backend}; db.queue("shared"); }
                std::optional<honker::Database> pin;
                if (label == "shm") pin.emplace(p.string(), ext);
                std::vector<pid_t> workers;
                std::vector<fs::path> ready_paths;
                std::vector<fs::path> result_paths;
                for (int i = 0; i < 3; ++i) {
                    const auto worker_id = "w" + std::to_string(i);
                    ready_paths.push_back(p.string() + "." + worker_id + ".ready");
                    result_paths.push_back(p.string() + "." + worker_id + ".result");
                    workers.push_back(spawn_worker_process(argv[0], p, ext, backend, worker_id, ready_paths.back(), result_paths.back()));
                }
                for (const auto& ready : ready_paths) wait_ready(ready);
                enqueue_range(p, ext, 0, 60);
                enqueue_stops(p, ext, static_cast<int>(workers.size()));
                std::vector<int> combined;
                for (auto worker : workers) wait_process(worker);
                for (const auto& result : result_paths) {
                    auto values = read_result(result);
                    combined.insert(combined.end(), values.begin(), values.end());
                }
                assert_int_set(combined, 60);
                assert(std::set<int>(combined.begin(), combined.end()).size() == 60);
                pin.reset();
                for (const auto& ready : ready_paths) fs::remove(ready);
                for (const auto& result : result_paths) fs::remove(result);
            } catch (const honker::Error& e) {
                const std::string msg{e.what()};
                if (msg.find("requires the") == std::string::npos &&
                    msg.find("-shm unavailable") == std::string::npos &&
                    msg.find("unsupported SQLite layout") == std::string::npos) {
                    throw;
                }
            }
            clean_db(p);
        }

        {
            const auto p = tmp_db("queue-nx1-" + label);
            clean_db(p);
            try {
                { honker::Database db{p.string(), ext, backend}; db.queue("shared"); }
                std::optional<honker::Database> pin;
                if (label == "shm") pin.emplace(p.string(), ext);
                const auto ready = p.string() + ".solo.ready";
                const auto result = p.string() + ".solo.result";
                auto worker = spawn_worker_process(argv[0], p, ext, backend, "solo", ready, result);
                wait_ready(ready);
                std::vector<pid_t> writers;
                for (int i = 0; i < 3; ++i) {
                    writers.push_back(spawn_writer_process(argv[0], p, ext, i * 20, 20));
                }
                for (auto writer : writers) wait_process(writer);
                enqueue_stops(p, ext, 1);
                wait_process(worker);
                assert_int_set(read_result(result), 60);
                pin.reset();
                fs::remove(ready);
                fs::remove(result);
            } catch (const honker::Error& e) {
                const std::string msg{e.what()};
                if (msg.find("requires the") == std::string::npos &&
                    msg.find("-shm unavailable") == std::string::npos &&
                    msg.find("unsupported SQLite layout") == std::string::npos) {
                    throw;
                }
            }
            clean_db(p);
        }
    }

    const auto tmp = tmp_db("test");
    clean_db(tmp);

    try {
        honker::Database db{tmp.string(), ext};
        auto q = db.queue("emails");

        const auto id = q.enqueue(R"({"to":"alice@example.com"})");
        assert(id > 0 && "enqueue should return a positive id");
        std::cout << "enqueued id=" << id << '\n';

        auto job = q.claim_one("worker-1");
        assert(job.has_value() && "should claim the job");
        std::cout << "claimed payload=" << job->payload() << '\n';

        const bool acked = job->ack();
        assert(acked && "fresh claim should ack");
        std::cout << "acked\n";

        auto second = q.claim_one("worker-1");
        assert(!second.has_value() && "queue should be empty after ack");
        std::cout << "queue empty after ack\n";

        std::vector<int> delivered;
        auto outbox = db.outbox("webhook", [&](const nlohmann::json& payload) {
            delivered.push_back(payload.value("order", 0));
        }, 60, 5, 0);

        {
            honker::Transaction tx = db.begin();
            outbox.enqueue_tx(tx, R"({"order":1})");
            tx.rollback();
        }
        assert(!outbox.queue().claim_one("outbox-worker").has_value());

        {
            honker::Transaction tx = db.begin();
            outbox.enqueue_tx(tx, R"({"order":2})");
            tx.commit();
        }

        assert(outbox.run_once("outbox-worker"));
        assert((delivered == std::vector<int>{2}));
        assert(!outbox.queue().claim_one("outbox-worker").has_value());

    } catch (const honker::Error& e) {
        std::fprintf(stderr, "honker error: %s\n", e.what());
        return 1;
    }

    std::cout << "ALL OK\n";
    return 0;
}
