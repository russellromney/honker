package honker

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

var queueWatcherBackends = []string{"", "kernel", "shm"}

func openBackendOrSkip(t *testing.T, dbPath, extPath, backend string) *Database {
	t.Helper()
	db, err := OpenWithOptions(dbPath, extPath, OpenOptions{WatcherBackend: backend})
	if err != nil {
		if strings.Contains(err.Error(), "requires the") ||
			strings.Contains(err.Error(), "-shm unavailable") ||
			strings.Contains(err.Error(), "unsupported SQLite layout") {
			t.Skipf("watcher backend %q unavailable in this build/environment: %v", backend, err)
		}
		t.Fatalf("open: %v", err)
	}
	return db
}

func TestWatcherBackendQueueE2E(t *testing.T) {
	extPath := findExtension(t)
	for _, backend := range queueWatcherBackends {
		label := backend
		if label == "" {
			label = "polling"
		}

		t.Run(label+"/1writer_1worker", func(t *testing.T) {
			dbPath := filepath.Join(t.TempDir(), "q.db")
			bootstrapQueueOrSkip(t, dbPath, extPath, backend)

			worker := spawnGoQueueWorker(t, dbPath, extPath, backend, "w1")
			runGoQueueWriter(t, dbPath, extPath, 25, 0, 0)
			processed := waitGoQueueWorker(t, worker, "w1")
			assertIntSet(t, processed, 25)
		})

		t.Run(label+"/1writer_many_workers", func(t *testing.T) {
			dbPath := filepath.Join(t.TempDir(), "q.db")
			bootstrapQueueOrSkip(t, dbPath, extPath, backend)

			workers := []*goQueueWorker{}
			for i := 0; i < 3; i++ {
				workers = append(workers, spawnGoQueueWorker(t, dbPath, extPath, backend, fmt.Sprintf("w%d", i)))
			}
			runGoQueueWriter(t, dbPath, extPath, 60, 0, 0)

			all := []int{}
			perWorker := make([][]int, len(workers))
			for i, worker := range workers {
				perWorker[i] = waitGoQueueWorker(t, worker, fmt.Sprintf("w%d", i))
				all = append(all, perWorker[i]...)
			}
			assertIntSet(t, all, 60)
			for i := range perWorker {
				seen := map[int]bool{}
				for _, value := range perWorker[i] {
					seen[value] = true
				}
				for j := i + 1; j < len(perWorker); j++ {
					for _, value := range perWorker[j] {
						if seen[value] {
							t.Fatalf("workers w%d and w%d double-claimed job %d", i, j, value)
						}
					}
				}
			}
		})

		t.Run(label+"/many_writers_1worker", func(t *testing.T) {
			dbPath := filepath.Join(t.TempDir(), "q.db")
			bootstrapQueueOrSkip(t, dbPath, extPath, backend)

			worker := spawnGoQueueWorker(t, dbPath, extPath, backend, "solo")
			writers := []*exec.Cmd{}
			for i := 0; i < 3; i++ {
				writers = append(writers, startGoQueueWriter(t, dbPath, extPath, 20, i*20, 0))
			}
			for _, writer := range writers {
				waitGoQueueWriter(t, writer)
			}
			processed := waitGoQueueWorker(t, worker, "solo")
			assertIntSet(t, processed, 60)
		})
	}
}

func bootstrapQueueOrSkip(t *testing.T, dbPath, extPath, backend string) {
	t.Helper()
	db := openBackendOrSkip(t, dbPath, extPath, backend)
	defer db.Close()
	db.Queue("shared", QueueOptions{})
}

type goQueueWorker struct {
	cmd    *exec.Cmd
	stdout *bufio.Reader
	stderr io.ReadCloser
	result string
}

func spawnGoQueueWorker(t *testing.T, dbPath, extPath, backend, workerID string) *goQueueWorker {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.v", "-test.run", "^TestWatcherBackendQueueHelper$")
	resultPath := filepath.Join(t.TempDir(), workerID+"-result.json")
	cmd.Env = append(os.Environ(),
		"HONKER_GO_QUEUE_HELPER=worker",
		"HONKER_GO_QUEUE_DB="+dbPath,
		"HONKER_GO_QUEUE_EXT="+extPath,
		"HONKER_GO_QUEUE_BACKEND="+backend,
		"HONKER_GO_QUEUE_WORKER="+workerID,
		"HONKER_GO_QUEUE_RESULT="+resultPath,
	)
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		t.Fatalf("stderr pipe: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start worker: %v", err)
	}
	worker := &goQueueWorker{cmd: cmd, stdout: bufio.NewReader(stdoutPipe), stderr: stderrPipe, result: resultPath}
	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			stderr, _ := io.ReadAll(stderrPipe)
			_ = cmd.Process.Kill()
			t.Fatalf("worker %s did not become ready: stderr=%s", workerID, stderr)
		default:
		}
		line, err := worker.stdout.ReadString('\n')
		if err != nil {
			stderr, _ := io.ReadAll(stderrPipe)
			_ = cmd.Process.Kill()
			t.Fatalf("worker %s did not become ready: err=%v stderr=%s", workerID, err, stderr)
		}
		if strings.TrimSpace(line) == "READY" {
			break
		}
	}
	return worker
}

func waitGoQueueWorker(t *testing.T, worker *goQueueWorker, workerID string) []int {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- worker.cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			stderr, _ := io.ReadAll(worker.stderr)
			stdout, _ := io.ReadAll(worker.stdout)
			resultBytes, _ := os.ReadFile(worker.result)
			t.Fatalf("worker %s failed: %v result=%s stdout=%s stderr=%s", workerID, err, resultBytes, stdout, stderr)
		}
	case <-time.After(20 * time.Second):
		_ = worker.cmd.Process.Kill()
		t.Fatalf("worker %s did not exit", workerID)
	}

	resultBytes, err := os.ReadFile(worker.result)
	if err != nil {
		stderr, _ := io.ReadAll(worker.stderr)
		t.Fatalf("worker %s produced no result file: %v stderr=%s", workerID, err, stderr)
	}
	var values []int
	if err := json.Unmarshal(resultBytes, &values); err != nil {
		t.Fatalf("parse worker result: %v", err)
	}
	return values
}

func startGoQueueWriter(t *testing.T, dbPath, extPath string, n, offset, holdMs int) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run", "^TestWatcherBackendQueueHelper$")
	cmd.Env = append(os.Environ(),
		"HONKER_GO_QUEUE_HELPER=writer",
		"HONKER_GO_QUEUE_DB="+dbPath,
		"HONKER_GO_QUEUE_EXT="+extPath,
		"HONKER_GO_QUEUE_N="+strconv.Itoa(n),
		"HONKER_GO_QUEUE_OFFSET="+strconv.Itoa(offset),
		"HONKER_GO_QUEUE_HOLD_MS="+strconv.Itoa(holdMs),
	)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start writer: %v", err)
	}
	return cmd
}

func waitGoQueueWriter(t *testing.T, cmd *exec.Cmd) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("writer failed: %v", err)
		}
	case <-time.After(15 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatal("writer did not exit")
	}
}

func runGoQueueWriter(t *testing.T, dbPath, extPath string, n, offset, holdMs int) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run", "^TestWatcherBackendQueueHelper$")
	cmd.Env = append(os.Environ(),
		"HONKER_GO_QUEUE_HELPER=writer",
		"HONKER_GO_QUEUE_DB="+dbPath,
		"HONKER_GO_QUEUE_EXT="+extPath,
		"HONKER_GO_QUEUE_N="+strconv.Itoa(n),
		"HONKER_GO_QUEUE_OFFSET="+strconv.Itoa(offset),
		"HONKER_GO_QUEUE_HOLD_MS="+strconv.Itoa(holdMs),
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("writer failed: %v output=%s", err, out)
	}
}

func assertIntSet(t *testing.T, values []int, n int) {
	t.Helper()
	sort.Ints(values)
	expected := make([]int, n)
	for i := range expected {
		expected[i] = i
	}
	gotJSON, _ := json.Marshal(values)
	wantJSON, _ := json.Marshal(expected)
	if string(gotJSON) != string(wantJSON) {
		t.Fatalf("processed jobs %s, expected %s", gotJSON, wantJSON)
	}
}

func TestWatcherBackendQueueHelper(t *testing.T) {
	mode := os.Getenv("HONKER_GO_QUEUE_HELPER")
	if mode == "" {
		return
	}
	dbPath := os.Getenv("HONKER_GO_QUEUE_DB")
	extPath := os.Getenv("HONKER_GO_QUEUE_EXT")

	switch mode {
	case "worker":
		if err := runQueueWorkerHelper(dbPath, extPath); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	case "writer":
		if err := runQueueWriterHelper(dbPath, extPath); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	default:
		t.Fatalf("unknown helper mode %q", mode)
	}
}

func runQueueWorkerHelper(dbPath, extPath string) error {
	backend := os.Getenv("HONKER_GO_QUEUE_BACKEND")
	workerID := os.Getenv("HONKER_GO_QUEUE_WORKER")
	db, err := OpenWithOptions(dbPath, extPath, OpenOptions{WatcherBackend: backend})
	if err != nil {
		return err
	}
	defer db.Close()
	q := db.Queue("shared", QueueOptions{})
	waker := q.ClaimWaker()
	defer waker.Close()
	fmt.Println("READY")

	processed := []int{}
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		job, err := waker.Next(ctx, workerID)
		cancel()
		if err != nil {
			return err
		}
		if job == nil {
			break
		}
		var payload struct {
			I int `json:"i"`
		}
		if err := json.Unmarshal(job.Payload, &payload); err != nil {
			return err
		}
		processed = append(processed, payload.I)
		if ok, err := job.Ack(); err != nil || !ok {
			return fmt.Errorf("ack: ok=%v err=%v", ok, err)
		}
	}
	out, _ := json.Marshal(processed)
	if resultPath := os.Getenv("HONKER_GO_QUEUE_RESULT"); resultPath != "" {
		return os.WriteFile(resultPath, out, 0o600)
	}
	fmt.Printf("RESULT %s\n", out)
	return nil
}

func runQueueWriterHelper(dbPath, extPath string) error {
	n, _ := strconv.Atoi(os.Getenv("HONKER_GO_QUEUE_N"))
	offset, _ := strconv.Atoi(os.Getenv("HONKER_GO_QUEUE_OFFSET"))
	db, err := Open(dbPath, extPath)
	if err != nil {
		return err
	}
	defer db.Close()
	q := db.Queue("shared", QueueOptions{})
	for i := offset; i < offset+n; i++ {
		if _, err := q.Enqueue(map[string]any{"i": i}, EnqueueOptions{}); err != nil {
			return err
		}
	}
	if holdMs, _ := strconv.Atoi(os.Getenv("HONKER_GO_QUEUE_HOLD_MS")); holdMs > 0 {
		time.Sleep(time.Duration(holdMs) * time.Millisecond)
	}
	return nil
}
