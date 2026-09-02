package honker

import (
	"path/filepath"
	"testing"
	"time"
)

// Issue #136: a claimed Job and a GetJob snapshot must both carry the
// full twelve-field job shape the core already returns, and a typed
// payload must survive the round trip through the database.

type emailPayload struct {
	Recipient string `json:"recipient"`
	Template  string `json:"template"`
	Version   int    `json:"version"`
}

// openTypedJobs returns two independent Database handles on one file:
// `worker` claims, `reader` reads. Separate handles mean separate
// connection pools, so the reader sees only what the worker committed.
func openTypedJobs(t *testing.T) (worker *Database, reader *Database) {
	t.Helper()
	extPath := findExtension(t)
	dbPath := filepath.Join(t.TempDir(), "typed.db")

	worker, err := Open(dbPath, extPath)
	if err != nil {
		t.Fatalf("open worker: %v", err)
	}
	t.Cleanup(func() { _ = worker.Close() })

	reader, err = Open(dbPath, extPath)
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	t.Cleanup(func() { _ = reader.Close() })

	return worker, reader
}

func TestClaimedJobAndSnapshotCarryEveryField(t *testing.T) {
	worker, reader := openTypedJobs(t)
	wq := worker.Queue("emails", QueueOptions{MaxAttempts: 5, VisibilityTimeoutS: 120})
	rq := reader.Queue("emails", QueueOptions{MaxAttempts: 5, VisibilityTimeoutS: 120})

	expires := int64(600)
	before := time.Now().Unix()
	id, err := wq.Enqueue(
		emailPayload{Recipient: "alice@example.com", Template: "welcome", Version: 2},
		EnqueueOptions{Priority: 7, Expires: &expires},
	)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	after := time.Now().Unix()
	if id <= 0 {
		t.Fatalf("enqueue returned id %d", id)
	}

	// ---- pending snapshot -------------------------------------------
	pending, err := rq.GetJob(id)
	if err != nil || pending == nil {
		t.Fatalf("GetJob(pending): row=%v err=%v", pending, err)
	}
	if pending.ID != id {
		t.Errorf("pending.ID = %d, want %d", pending.ID, id)
	}
	if pending.Queue != "emails" {
		t.Errorf("pending.Queue = %q, want %q", pending.Queue, "emails")
	}
	if pending.State != "pending" {
		t.Errorf("pending.State = %q, want %q", pending.State, "pending")
	}
	if pending.Priority != 7 {
		t.Errorf("pending.Priority = %d, want 7", pending.Priority)
	}
	if pending.RunAt < before || pending.RunAt > after {
		t.Errorf("pending.RunAt = %d, want within [%d, %d]", pending.RunAt, before, after)
	}
	if pending.WorkerID != nil {
		t.Errorf("pending.WorkerID = %q, want nil before any claim", *pending.WorkerID)
	}
	if pending.ClaimExpiresAt != nil {
		t.Errorf("pending.ClaimExpiresAt = %d, want nil before any claim", *pending.ClaimExpiresAt)
	}
	if pending.Attempts != 0 {
		t.Errorf("pending.Attempts = %d, want 0", pending.Attempts)
	}
	if pending.MaxAttempts != 5 {
		t.Errorf("pending.MaxAttempts = %d, want 5", pending.MaxAttempts)
	}
	if pending.CreatedAt < before || pending.CreatedAt > after {
		t.Errorf("pending.CreatedAt = %d, want within [%d, %d]", pending.CreatedAt, before, after)
	}
	if pending.ExpiresAt == nil {
		t.Fatalf("pending.ExpiresAt = nil, want run_at + %d", expires)
	}
	// enqueue derives run_at and expires_at from one unixepoch() read,
	// so the gap is exactly the requested TTL.
	if got := *pending.ExpiresAt - pending.RunAt; got != expires {
		t.Errorf("pending.ExpiresAt - RunAt = %d, want %d", got, expires)
	}
	pendingPayload, err := DecodePayload[emailPayload](pending.PayloadBytes())
	if err != nil {
		t.Fatalf("decode pending payload: %v", err)
	}
	want := emailPayload{Recipient: "alice@example.com", Template: "welcome", Version: 2}
	if pendingPayload != want {
		t.Errorf("pending payload = %+v, want %+v", pendingPayload, want)
	}

	// ---- claimed job -------------------------------------------------
	claimBefore := time.Now().Unix()
	job, err := wq.ClaimOne("worker-go")
	if err != nil || job == nil {
		t.Fatalf("ClaimOne: job=%v err=%v", job, err)
	}
	claimAfter := time.Now().Unix()

	if job.ID != id {
		t.Errorf("job.ID = %d, want %d", job.ID, id)
	}
	if job.Queue != "emails" {
		t.Errorf("job.Queue = %q, want %q", job.Queue, "emails")
	}
	if job.State != "processing" {
		t.Errorf("job.State = %q, want %q", job.State, "processing")
	}
	if job.Priority != 7 {
		t.Errorf("job.Priority = %d, want 7", job.Priority)
	}
	if job.RunAt != pending.RunAt {
		t.Errorf("job.RunAt = %d, want %d (unchanged by claim)", job.RunAt, pending.RunAt)
	}
	if job.WorkerID != "worker-go" {
		t.Errorf("job.WorkerID = %q, want %q", job.WorkerID, "worker-go")
	}
	if job.ClaimExpiresAt < claimBefore+120 || job.ClaimExpiresAt > claimAfter+120 {
		t.Errorf(
			"job.ClaimExpiresAt = %d, want within [%d, %d] (claim time + 120s timeout)",
			job.ClaimExpiresAt, claimBefore+120, claimAfter+120,
		)
	}
	if job.Attempts != 1 {
		t.Errorf("job.Attempts = %d, want 1 (claim increments)", job.Attempts)
	}
	if job.MaxAttempts != 5 {
		t.Errorf("job.MaxAttempts = %d, want 5", job.MaxAttempts)
	}
	if job.CreatedAt != pending.CreatedAt {
		t.Errorf("job.CreatedAt = %d, want %d (unchanged by claim)", job.CreatedAt, pending.CreatedAt)
	}
	if job.ExpiresAt == nil || *job.ExpiresAt != *pending.ExpiresAt {
		t.Errorf("job.ExpiresAt = %v, want %d", job.ExpiresAt, *pending.ExpiresAt)
	}
	claimedPayload, err := DecodePayload[emailPayload](job.Payload)
	if err != nil {
		t.Fatalf("decode claimed payload: %v", err)
	}
	if claimedPayload != want {
		t.Errorf("claimed payload = %+v, want %+v", claimedPayload, want)
	}

	// ---- the reader sees the processing details ----------------------
	processing, err := rq.GetJob(id)
	if err != nil || processing == nil {
		t.Fatalf("GetJob(processing): row=%v err=%v", processing, err)
	}
	if processing.State != "processing" {
		t.Errorf("processing.State = %q, want %q", processing.State, "processing")
	}
	if processing.WorkerID == nil || *processing.WorkerID != "worker-go" {
		t.Errorf("processing.WorkerID = %v, want %q", processing.WorkerID, "worker-go")
	}
	if processing.ClaimExpiresAt == nil || *processing.ClaimExpiresAt != job.ClaimExpiresAt {
		t.Errorf("processing.ClaimExpiresAt = %v, want %d", processing.ClaimExpiresAt, job.ClaimExpiresAt)
	}
	if processing.Attempts != 1 {
		t.Errorf("processing.Attempts = %d, want 1", processing.Attempts)
	}

	// ---- after ack the reader gets nothing ---------------------------
	acked, err := job.Ack()
	if err != nil || !acked {
		t.Fatalf("Ack: acked=%v err=%v", acked, err)
	}
	gone, err := rq.GetJob(id)
	if err != nil {
		t.Fatalf("GetJob after ack: %v", err)
	}
	if gone != nil {
		t.Fatalf("GetJob after ack = %+v, want nil", gone)
	}
}

func TestDelayedJobReportsItsRunAt(t *testing.T) {
	worker, _ := openTypedJobs(t)
	q := worker.Queue("delayed", QueueOptions{})

	delay := int64(3600)
	before := time.Now().Unix()
	id, err := q.Enqueue(map[string]any{"to": "later"}, EnqueueOptions{Delay: &delay})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	after := time.Now().Unix()

	row, err := q.GetJob(id)
	if err != nil || row == nil {
		t.Fatalf("GetJob: row=%v err=%v", row, err)
	}
	if row.State != "pending" {
		t.Errorf("row.State = %q, want %q", row.State, "pending")
	}
	if row.RunAt < before+delay || row.RunAt > after+delay {
		t.Errorf("row.RunAt = %d, want within [%d, %d]", row.RunAt, before+delay, after+delay)
	}
	// Not claimable until run_at.
	job, err := q.ClaimOne("worker-go")
	if err != nil {
		t.Fatalf("ClaimOne: %v", err)
	}
	if job != nil {
		t.Fatalf("delayed job claimed early: %+v", job)
	}
}

// TestClaimedJobReportsBackDatedRunAt pins the claimed Job's RunAt and
// CreatedAt apart. An immediately-enqueued job has run_at == created_at
// to the second, so asserting both against the same instant proves
// nothing: the two fields can be transposed in the claim decode and the
// assertions still hold. Back-dating run_at with an absolute timestamp
// keeps the job claimable (a delayed job is not) while the two values
// differ by 100 seconds.
func TestClaimedJobReportsBackDatedRunAt(t *testing.T) {
	worker, reader := openTypedJobs(t)
	wq := worker.Queue("backdated", QueueOptions{MaxAttempts: 3, VisibilityTimeoutS: 60})
	rq := reader.Queue("backdated", QueueOptions{MaxAttempts: 3, VisibilityTimeoutS: 60})

	before := time.Now().Unix()
	past := before - 100
	id, err := wq.Enqueue(
		emailPayload{Recipient: "bob@example.com", Template: "reminder", Version: 1},
		EnqueueOptions{RunAt: &past},
	)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	after := time.Now().Unix()

	// ---- snapshot keeps the two apart --------------------------------
	row, err := rq.GetJob(id)
	if err != nil || row == nil {
		t.Fatalf("GetJob: row=%v err=%v", row, err)
	}
	if row.RunAt != past {
		t.Errorf("row.RunAt = %d, want %d (the back-dated absolute run_at)", row.RunAt, past)
	}
	if row.CreatedAt < before || row.CreatedAt > after {
		t.Errorf("row.CreatedAt = %d, want within [%d, %d]", row.CreatedAt, before, after)
	}
	if row.RunAt == row.CreatedAt {
		t.Fatalf("row.RunAt == row.CreatedAt == %d, fixture no longer separates them", row.RunAt)
	}

	// ---- claimed job keeps the two apart -----------------------------
	job, err := wq.ClaimOne("worker-backdated")
	if err != nil {
		t.Fatalf("ClaimOne: %v", err)
	}
	if job == nil {
		t.Fatalf("ClaimOne returned nil, want the back-dated job to be claimable")
	}
	if job.RunAt != past {
		t.Errorf("job.RunAt = %d, want %d (the back-dated absolute run_at)", job.RunAt, past)
	}
	if job.CreatedAt < before || job.CreatedAt > after {
		t.Errorf("job.CreatedAt = %d, want within [%d, %d]", job.CreatedAt, before, after)
	}
	if job.RunAt == job.CreatedAt {
		t.Errorf("job.RunAt == job.CreatedAt == %d, want run_at %d and created_at ~%d",
			job.RunAt, past, before)
	}
	if job.CreatedAt-job.RunAt != row.CreatedAt-row.RunAt {
		t.Errorf(
			"claimed gap CreatedAt-RunAt = %d, snapshot gap = %d, want equal",
			job.CreatedAt-job.RunAt, row.CreatedAt-row.RunAt,
		)
	}
}

// TestDecodePayloadRejectsEmptyInput: an empty payload must not decode
// to a zero-valued T with a nil error. The core never emits an empty
// payload, so empty means the bytes never arrived, and a caller cannot
// otherwise tell that apart from a clean decode.
func TestDecodePayloadRejectsEmptyInput(t *testing.T) {
	for _, tc := range []struct {
		name    string
		payload []byte
	}{
		{"nil", nil},
		{"empty", []byte{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := DecodePayload[emailPayload](tc.payload)
			if err == nil {
				t.Fatalf("DecodePayload(%s) = %+v, nil — want an error", tc.name, got)
			}
			if got != (emailPayload{}) {
				t.Errorf("DecodePayload(%s) value = %+v, want the zero value", tc.name, got)
			}
		})
	}

	// A real payload still decodes.
	want := emailPayload{Recipient: "carol@example.com", Template: "digest", Version: 3}
	got, err := DecodePayload[emailPayload]([]byte(`{"recipient":"carol@example.com","template":"digest","version":3}`))
	if err != nil {
		t.Fatalf("DecodePayload(valid): %v", err)
	}
	if got != want {
		t.Errorf("DecodePayload(valid) = %+v, want %+v", got, want)
	}
}
