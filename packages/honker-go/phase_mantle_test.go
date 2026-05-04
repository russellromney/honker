package honker

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"time"
)

// Phase Mantle: Scheduler lifecycle (pause/resume/list/update) +
// Queue cancel/get_job.

func openMantle(t *testing.T) *Database {
	t.Helper()
	extPath := findExtension(t)
	dbPath := filepath.Join(t.TempDir(), "t.db")
	db, err := Open(dbPath, extPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestSchedulerListRoundTripsFields(t *testing.T) {
	db := openMantle(t)
	sched := db.Scheduler()
	if err := sched.Add(ScheduledTask{
		Name: "recap", Queue: "emails",
		Schedule: "0 9 * * 1",
		Payload:  map[string]any{"team": "premier-league"},
		Priority: 3,
	}); err != nil {
		t.Fatal(err)
	}
	if err := sched.Add(ScheduledTask{
		Name: "sync", Queue: "syncs",
		Schedule: "@every 1h",
	}); err != nil {
		t.Fatal(err)
	}

	rows, err := sched.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 schedules, got %d", len(rows))
	}
	var recap *ScheduleRow
	for i := range rows {
		if rows[i].Name == "recap" {
			recap = &rows[i]
		}
	}
	if recap == nil {
		t.Fatal("missing 'recap' row")
	}
	if recap.Queue != "emails" || recap.Priority != 3 || !recap.Enabled {
		t.Fatalf("recap row mismatch: %+v", recap)
	}
	var payload map[string]any
	_ = json.Unmarshal([]byte(recap.Payload), &payload)
	if payload["team"] != "premier-league" {
		t.Fatalf("payload mismatch: %v", payload)
	}
}

func TestSchedulerPauseResumeIdempotent(t *testing.T) {
	db := openMantle(t)
	sched := db.Scheduler()
	if err := sched.Add(ScheduledTask{Name: "a", Queue: "q", Schedule: "0 9 * * *"}); err != nil {
		t.Fatal(err)
	}

	for _, c := range []struct {
		name string
		want bool
	}{
		{"a", true},
		{"a", false}, // already paused
		{"missing", false},
	} {
		got, err := sched.Pause(c.name)
		if err != nil {
			t.Fatal(err)
		}
		if got != c.want {
			t.Fatalf("Pause(%q): got %v want %v", c.name, got, c.want)
		}
	}

	rows, _ := sched.List()
	if rows[0].Enabled {
		t.Fatal("row should be disabled after pause")
	}

	got, _ := sched.Resume("a")
	if !got {
		t.Fatal("resume should report true")
	}
	got, _ = sched.Resume("a")
	if got {
		t.Fatal("second resume should report false (idempotent)")
	}
	rows, _ = sched.List()
	if !rows[0].Enabled {
		t.Fatal("row should be enabled after resume")
	}
}

func TestSchedulerUpdateMutatesAndNoop(t *testing.T) {
	db := openMantle(t)
	sched := db.Scheduler()
	if err := sched.Add(ScheduledTask{
		Name: "t", Queue: "q", Schedule: "0 9 * * *",
		Payload: map[string]any{"v": 1.0},
	}); err != nil {
		t.Fatal(err)
	}

	newPayload := any(map[string]any{"v": 99.0})
	newPriority := int64(5)
	ok, err := sched.Update("t", ScheduleUpdate{Payload: &newPayload, Priority: &newPriority})
	if err != nil || !ok {
		t.Fatalf("Update: ok=%v err=%v", ok, err)
	}
	rows, _ := sched.List()
	row := rows[0]
	var payload map[string]any
	_ = json.Unmarshal([]byte(row.Payload), &payload)
	if payload["v"] != 99.0 {
		t.Fatalf("payload not updated: %v", payload)
	}
	if row.Priority != 5 {
		t.Fatalf("priority not updated: %d", row.Priority)
	}

	// Cron change recomputes next_fire_at.
	before := row.NextFireAt
	newCron := "*/5 * * * *"
	ok, err = sched.Update("t", ScheduleUpdate{CronExpr: &newCron})
	if err != nil || !ok {
		t.Fatalf("update cron: ok=%v err=%v", ok, err)
	}
	rows, _ = sched.List()
	if rows[0].CronExpr != "*/5 * * * *" {
		t.Fatalf("cron not updated: %s", rows[0].CronExpr)
	}
	if rows[0].NextFireAt == before {
		t.Fatal("next_fire_at should have changed after cron update")
	}

	// Empty update returns false.
	ok, _ = sched.Update("t", ScheduleUpdate{})
	if ok {
		t.Fatal("empty update should be no-op")
	}

	// Missing name returns false.
	ok, _ = sched.Update("missing", ScheduleUpdate{Priority: &newPriority})
	if ok {
		t.Fatal("update of missing schedule should return false")
	}
}

func TestQueueCancelAndGetJob(t *testing.T) {
	db := openMantle(t)
	q := db.Queue("emails", QueueOptions{})
	id, err := q.Enqueue(map[string]any{"to": "alice"}, EnqueueOptions{})
	if err != nil {
		t.Fatal(err)
	}

	row, err := q.GetJob(id)
	if err != nil || row == nil {
		t.Fatalf("GetJob: row=%v err=%v", row, err)
	}
	if row.ID != id || row.State != "pending" || row.Queue != "emails" {
		t.Fatalf("row mismatch: %+v", row)
	}

	ok, _ := q.Cancel(id)
	if !ok {
		t.Fatal("cancel should return true")
	}
	ok, _ = q.Cancel(id)
	if ok {
		t.Fatal("cancel should be idempotent (false on already-gone)")
	}
	row, _ = q.GetJob(id)
	if row != nil {
		t.Fatalf("get_job after cancel should be nil, got %+v", row)
	}
}

func TestCancelProcessingInvalidatesAck(t *testing.T) {
	db := openMantle(t)
	q := db.Queue("emails", QueueOptions{})
	id, _ := q.Enqueue(map[string]any{"to": "x"}, EnqueueOptions{})
	job, _ := q.ClaimOne("worker-1")
	if job == nil || job.ID != id {
		t.Fatalf("claim failed")
	}
	ok, _ := q.Cancel(id)
	if !ok {
		t.Fatal("cancel should remove processing row")
	}
	acked, _ := job.Ack()
	if acked {
		t.Fatal("ack should return false after cancel (same shape as expired claim)")
	}
}

func TestPausedScheduleDoesNotEmit(t *testing.T) {
	db := openMantle(t)
	sched := db.Scheduler()
	if err := sched.Add(ScheduledTask{
		Name: "due", Queue: "emails", Schedule: "@every 1s",
		Payload: map[string]any{"x": 1},
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1100 * time.Millisecond)
	if _, err := sched.Pause("due"); err != nil {
		t.Fatal(err)
	}

	fires, err := sched.Tick()
	if err != nil {
		t.Fatal(err)
	}
	if len(fires) != 0 {
		t.Fatalf("paused schedule must not emit; got %v", fires)
	}

	// Resume and tick again — now it fires.
	if _, err := sched.Resume("due"); err != nil {
		t.Fatal(err)
	}
	fires2, _ := sched.Tick()
	if len(fires2) < 1 {
		t.Fatalf("resumed schedule should emit; got %v", fires2)
	}
}

func TestGetJobMissesAfterAck(t *testing.T) {
	db := openMantle(t)
	q := db.Queue("emails", QueueOptions{})
	id, _ := q.Enqueue(map[string]any{"to": "x"}, EnqueueOptions{})
	job, _ := q.ClaimOne("worker-1")
	if acked, _ := job.Ack(); !acked {
		t.Fatal("ack should succeed on fresh claim")
	}
	row, _ := q.GetJob(id)
	if row != nil {
		t.Fatalf("get_job after ack should be nil, got %+v", row)
	}
}

func TestUpdatePayloadNullVsOmitted(t *testing.T) {
	db := openMantle(t)
	sched := db.Scheduler()
	if err := sched.Add(ScheduledTask{
		Name: "t", Queue: "q", Schedule: "0 9 * * *",
		Payload: map[string]any{"v": 1.0},
	}); err != nil {
		t.Fatal(err)
	}

	// Omitted payload — leaves it alone.
	priority := int64(7)
	ok, _ := sched.Update("t", ScheduleUpdate{Priority: &priority})
	if !ok {
		t.Fatal("priority-only update should succeed")
	}
	rows, _ := sched.List()
	var payload map[string]any
	_ = json.Unmarshal([]byte(rows[0].Payload), &payload)
	if payload["v"] != 1.0 {
		t.Fatalf("payload should be untouched: %v", payload)
	}

	// payload: any(nil) — explicitly write JSON null.
	var nullPayload any = nil
	ok, _ = sched.Update("t", ScheduleUpdate{Payload: &nullPayload})
	if !ok {
		t.Fatal("payload-null update should succeed")
	}
	rows, _ = sched.List()
	if rows[0].Payload != "null" {
		t.Fatalf("payload should be JSON null, got %q", rows[0].Payload)
	}
}
