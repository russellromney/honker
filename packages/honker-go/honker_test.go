package honker

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// findExtension locates libhonker.{dylib,so} under the repo's
// target/release/ dir so the test doesn't need to hardcode a path.
func findExtension(t *testing.T) string {
	t.Helper()
	_, thisFile, _, _ := runtime.Caller(0)
	// thisFile = .../packages/honker-go/honker_test.go
	repo := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
	candidates := []string{
		filepath.Join(repo, "target/release/libhonker_ext.dylib"),
		filepath.Join(repo, "target/release/libhonker_ext.so"),
		filepath.Join(repo, "target/release/libhonker_extension.dylib"),
		filepath.Join(repo, "target/release/libhonker_extension.so"),
	}
	for _, p := range candidates {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	t.Skipf(
		"honker extension not built — run `cargo build -p honker-extension --release` first. "+
			"looked for: %v",
		candidates,
	)
	return ""
}

func TestEnqueueClaimAck(t *testing.T) {
	extPath := findExtension(t)
	dbPath := filepath.Join(t.TempDir(), "t.db")

	db, err := Open(dbPath, extPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	q := db.Queue("emails", QueueOptions{})

	id, err := q.Enqueue(map[string]any{"to": "alice@example.com"}, EnqueueOptions{})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if id <= 0 {
		t.Fatalf("expected positive id, got %d", id)
	}

	job, err := q.ClaimOne("worker-1")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if job == nil {
		t.Fatal("expected a claimed job, got nil")
	}
	if job.ID != id {
		t.Fatalf("expected id=%d, got %d", id, job.ID)
	}

	var payload map[string]any
	if err := json.Unmarshal(job.Payload, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payload["to"] != "alice@example.com" {
		t.Fatalf("payload round-trip failed: %+v", payload)
	}

	ok, err := job.Ack()
	if err != nil {
		t.Fatalf("ack: %v", err)
	}
	if !ok {
		t.Fatal("expected ack=true for a fresh claim")
	}

	// Queue empty after ack.
	next, err := q.ClaimOne("worker-1")
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if next != nil {
		t.Fatalf("expected empty queue after ack, got job %d", next.ID)
	}
}

func TestOutboxTransactionalEnqueueAndDelivery(t *testing.T) {
	extPath := findExtension(t)
	dbPath := filepath.Join(t.TempDir(), "t.db")

	db, err := Open(dbPath, extPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	delivered := make(chan int, 1)
	outbox := db.Outbox("webhook", func(ctx context.Context, payload json.RawMessage) error {
		var row map[string]int
		if err := json.Unmarshal(payload, &row); err != nil {
			return err
		}
		delivered <- row["order"]
		return nil
	}, OutboxOptions{})

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin rollback tx: %v", err)
	}
	if _, err := outbox.EnqueueTx(tx, map[string]any{"order": 1}, EnqueueOptions{}); err != nil {
		t.Fatalf("outbox enqueue rollback tx: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if job, err := outbox.Queue().ClaimOne("w"); err != nil || job != nil {
		t.Fatalf("rollback outbox job = (%v, %v), want empty", job, err)
	}

	tx, err = db.Begin()
	if err != nil {
		t.Fatalf("begin commit tx: %v", err)
	}
	if _, err := outbox.EnqueueTx(tx, map[string]any{"order": 2}, EnqueueOptions{}); err != nil {
		t.Fatalf("outbox enqueue commit tx: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- outbox.RunWorker(ctx, "w") }()

	select {
	case got := <-delivered:
		if got != 2 {
			t.Fatalf("delivered order = %d, want 2", got)
		}
		cancel()
	case err := <-errCh:
		t.Fatalf("worker exited early: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for outbox delivery")
	}
}

func TestWatcherBackendOptionsDetectCommits(t *testing.T) {
	extPath := findExtension(t)
	tests := []string{"", "poll", "polling", "kernel", "kernel-watcher", "shm", "shm-fast-path"}
	for _, backend := range tests {
		t.Run(backend, func(t *testing.T) {
			dbPath := filepath.Join(t.TempDir(), "t.db")
			db, err := OpenWithOptions(dbPath, extPath, OpenOptions{WatcherBackend: backend})
			if err != nil {
				if strings.Contains(err.Error(), "requires the") ||
					strings.Contains(err.Error(), "-shm unavailable") ||
					strings.Contains(err.Error(), "unsupported SQLite layout") {
					t.Skipf("watcher backend %q unavailable in this build/environment: %v", backend, err)
				}
				t.Fatalf("open: %v", err)
			}
			defer db.Close()

			subID, updateCh := db.updates.subscribe()
			defer db.updates.unsubscribe(subID)

			writer, err := Open(dbPath, extPath)
			if err != nil {
				t.Fatalf("open writer: %v", err)
			}
			defer writer.Close()
			if _, err := writer.Notify("backend", map[string]any{"backend": backend}); err != nil {
				t.Fatalf("notify: %v", err)
			}

			select {
			case <-updateCh:
			case <-time.After(2 * time.Second):
				t.Fatalf("watcher backend %q did not observe commit", backend)
			}
		})
	}
}

func TestWatcherBackendOptionsAcceptPollingAliases(t *testing.T) {
	tests := []string{"", "poll", "polling"}
	for _, backend := range tests {
		t.Run(backend, func(t *testing.T) {
			_, err := OpenWithOptions(
				filepath.Join(t.TempDir(), "t.db"),
				"/missing/libhonker_ext.so",
				OpenOptions{WatcherBackend: backend},
			)
			if err == nil {
				t.Fatal("expected missing extension error after watcher backend validation")
			}
			if strings.Contains(err.Error(), "watcher backend") {
				t.Fatalf("polling alias should pass watcher validation, got %v", err)
			}
		})
	}
}

func TestWatcherBackendOptionsRejectUnknownNames(t *testing.T) {
	extPath := findExtension(t)
	tests := []string{"bogus", "KERNEL", " polling "}
	for _, backend := range tests {
		t.Run(backend, func(t *testing.T) {
			_, err := OpenWithOptions(
				filepath.Join(t.TempDir(), "t.db"),
				extPath,
				OpenOptions{WatcherBackend: backend},
			)
			if err == nil || !strings.Contains(err.Error(), "unknown watcher backend") {
				t.Fatalf("expected unknown watcher backend error, got %v", err)
			}
		})
	}
}

func TestRetryAndFail(t *testing.T) {
	extPath := findExtension(t)
	dbPath := filepath.Join(t.TempDir(), "t.db")
	db, err := Open(dbPath, extPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	q := db.Queue("retries", QueueOptions{MaxAttempts: 2})

	if _, err := q.Enqueue(map[string]any{"i": 1}, EnqueueOptions{}); err != nil {
		t.Fatal(err)
	}

	job, _ := q.ClaimOne("w")
	if job == nil {
		t.Fatal("no job")
	}

	// retry with 0 delay → claimable again immediately
	ok, err := job.Retry(0, "first attempt failed")
	if err != nil || !ok {
		t.Fatalf("retry: ok=%v err=%v", ok, err)
	}

	// claim again; attempts should now be 2 (max_attempts)
	job2, _ := q.ClaimOne("w")
	if job2 == nil {
		t.Fatal("retry didn't flip back to pending")
	}
	if job2.Attempts != 2 {
		t.Fatalf("expected attempts=2, got %d", job2.Attempts)
	}

	// second retry hits max; should move to dead
	if _, err := job2.Retry(0, "second attempt failed"); err != nil {
		t.Fatal(err)
	}
	// queue empty
	next, _ := q.ClaimOne("w")
	if next != nil {
		t.Fatalf("expected dead-letter path, got job %d", next.ID)
	}

	// dead row visible
	var count int
	row := db.Raw().QueryRow("SELECT COUNT(*) FROM _honker_dead WHERE queue='retries'")
	if err := row.Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("expected 1 dead row, got %d", count)
	}
}

func TestNotify(t *testing.T) {
	extPath := findExtension(t)
	dbPath := filepath.Join(t.TempDir(), "t.db")
	db, err := Open(dbPath, extPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	id, err := db.Notify("orders", map[string]any{"id": 42})
	if err != nil {
		t.Fatal(err)
	}
	if id <= 0 {
		t.Fatalf("expected positive notification id, got %d", id)
	}

	var channel, payload string
	row := db.Raw().QueryRow(
		"SELECT channel, payload FROM _honker_notifications WHERE id = ?",
		id,
	)
	if err := row.Scan(&channel, &payload); err != nil {
		t.Fatal(err)
	}
	if channel != "orders" {
		t.Fatalf("channel: got %q", channel)
	}
	var parsed map[string]any
	if err := json.Unmarshal([]byte(payload), &parsed); err != nil {
		t.Fatalf("payload unmarshal: %v (raw=%s)", err, payload)
	}
	if int(parsed["id"].(float64)) != 42 {
		t.Fatalf("payload mismatch: %+v", parsed)
	}
}
