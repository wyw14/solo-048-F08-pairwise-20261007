package protection

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"heatpulse/internal/exchanger"
	"heatpulse/internal/pump"
	"heatpulse/internal/store"
)

func newTestAntifreeze(t *testing.T, directory string) (*Antifreeze, *store.AntifreezeSessionStore, *store.EventLog) {
	t.Helper()
	circulation := exchanger.NewCirculationRunner(pump.NewRunner())
	sessions := store.NewAntifreezeSessionStore(directory)
	events := store.NewEventLog(filepath.Join(directory, "events.jsonl"))
	antifreeze := NewAntifreeze(circulation, sessions, events)
	antifreeze.CheckpointInterval = 5 * time.Millisecond
	t.Cleanup(antifreeze.Close)
	return antifreeze, sessions, events
}

func eventKinds(t *testing.T, events *store.EventLog) []string {
	t.Helper()
	recorded, err := events.List(0)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	kinds := make([]string, 0, len(recorded))
	for _, event := range recorded {
		kinds = append(kinds, event.Kind)
	}
	return kinds
}

func containsKind(kinds []string, want string) bool {
	for _, kind := range kinds {
		if kind == want {
			return true
		}
	}
	return false
}

func TestAntifreezeStartStopPersistsLifecycle(t *testing.T) {
	directory := t.TempDir()
	antifreeze, sessions, events := newTestAntifreeze(t, directory)

	session, err := antifreeze.Start(context.Background(), "xincheng")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if !session.Active {
		t.Fatalf("expected active session, got %+v", session)
	}
	record, ok, err := sessions.Get("xincheng")
	if err != nil || !ok {
		t.Fatalf("persisted record missing: ok=%v err=%v", ok, err)
	}
	if !record.Active || record.ID != session.ID {
		t.Fatalf("expected persisted active record, got %+v", record)
	}

	time.Sleep(30 * time.Millisecond)
	stopped, err := antifreeze.Stop("xincheng")
	if err != nil {
		t.Fatalf("stop: %v", err)
	}
	if stopped.Active || stopped.StopReason != store.AntifreezeReasonOperator {
		t.Fatalf("expected operator-stopped session, got %+v", stopped)
	}
	if stopped.Ticks == 0 {
		t.Fatalf("expected loop ticks to be recorded, got %+v", stopped)
	}
	record, _, err = sessions.Get("xincheng")
	if err != nil {
		t.Fatalf("get record: %v", err)
	}
	if record.Active || record.StopReason != store.AntifreezeReasonOperator || record.Ticks != stopped.Ticks {
		t.Fatalf("persisted stop mismatch: %+v", record)
	}
	if record.StoppedAt.IsZero() {
		t.Fatalf("expected stop timestamp, got %+v", record)
	}

	kinds := eventKinds(t, events)
	if !containsKind(kinds, "antifreeze.started") || !containsKind(kinds, "antifreeze.stopped") {
		t.Fatalf("expected start/stop events, got %v", kinds)
	}
}

func TestAntifreezeDuplicateStartDoesNotSpawnSecondLoop(t *testing.T) {
	directory := t.TempDir()
	antifreeze, _, _ := newTestAntifreeze(t, directory)

	first, err := antifreeze.Start(context.Background(), "binhe")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if _, err := antifreeze.Start(context.Background(), "binhe"); err == nil {
		t.Fatal("expected duplicate start to fail")
	}
	antifreeze.mu.RLock()
	loops := len(antifreeze.active)
	antifreeze.mu.RUnlock()
	if loops != 1 {
		t.Fatalf("expected exactly one loop, got %d", loops)
	}
	current, ok := antifreeze.Session("binhe")
	if !ok || current.ID != first.ID {
		t.Fatalf("expected original session to remain, got %+v", current)
	}
	if _, err := antifreeze.Stop("binhe"); err != nil {
		t.Fatalf("stop after duplicate start: %v", err)
	}
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met before deadline")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestAntifreezeFailedStartLeavesNoStaleEntry(t *testing.T) {
	directory := t.TempDir()
	antifreeze, _, _ := newTestAntifreeze(t, directory)

	// Occupy the pump so the circulation loop cannot start.
	sharedPump := pump.NewRunner()
	blocker := exchanger.NewCirculationRunner(sharedPump)
	blockCtx, blockCancel := context.WithCancel(context.Background())
	defer blockCancel()
	go blocker.Run(blockCtx, "nanhu", nil)
	waitFor(t, func() bool { return sharedPump.Running("nanhu") })

	antifreeze.runner = exchanger.NewCirculationRunner(sharedPump)
	if _, err := antifreeze.Start(context.Background(), "nanhu"); err == nil {
		t.Fatal("expected start to fail while pump is occupied")
	}
	antifreeze.mu.RLock()
	_, stale := antifreeze.active["nanhu"]
	antifreeze.mu.RUnlock()
	if stale {
		t.Fatal("failed start left a stale active entry")
	}
	record, ok, err := antifreeze.sessions.Get("nanhu")
	if err != nil || !ok {
		t.Fatalf("expected persisted failure record: ok=%v err=%v", ok, err)
	}
	if record.Active || record.StopReason != store.AntifreezeReasonStartFailed {
		t.Fatalf("expected start_failed record, got %+v", record)
	}

	blockCancel()
	waitFor(t, func() bool { return !sharedPump.Running("nanhu") })
	if _, err := antifreeze.Start(context.Background(), "nanhu"); err != nil {
		t.Fatalf("start after failure cleanup: %v", err)
	}
}

func TestAntifreezeRecoverMarksInterruptedSessionForReview(t *testing.T) {
	directory := t.TempDir()
	antifreeze, _, events := newTestAntifreeze(t, directory)

	session, err := antifreeze.Start(context.Background(), "xijiao")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	// Simulate a crash: drop the in-memory loop without stopping it.
	antifreeze.mu.Lock()
	crashed := antifreeze.active["xijiao"]
	delete(antifreeze.active, "xijiao")
	antifreeze.mu.Unlock()
	crashed.cancel()
	<-crashed.done

	recoverer, recoveredSessions, recoveredEvents := newTestAntifreeze(t, directory)
	if err := recoverer.Recover(); err != nil {
		t.Fatalf("recover: %v", err)
	}
	record, ok, err := recoveredSessions.Get("xijiao")
	if err != nil || !ok {
		t.Fatalf("record missing after recovery: ok=%v err=%v", ok, err)
	}
	if record.Active || !record.NeedsReview || record.StopReason != store.AntifreezeReasonInterrupted {
		t.Fatalf("expected interrupted needs-review record, got %+v", record)
	}
	if record.ID != session.ID {
		t.Fatalf("expected original session id %s, got %s", session.ID, record.ID)
	}
	if recoverer.Running("xijiao") {
		t.Fatal("interrupted session must not be restarted automatically")
	}
	if !containsKind(eventKinds(t, recoveredEvents), "antifreeze.needs_review") {
		t.Fatalf("expected needs_review event, got %v", eventKinds(t, events))
	}

	resumed, err := recoverer.Resume("xijiao")
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if !resumed.Active || resumed.NeedsReview {
		t.Fatalf("expected active reviewed session, got %+v", resumed)
	}
	record, _, _ = recoveredSessions.Get("xijiao")
	if !record.Active || record.NeedsReview {
		t.Fatalf("expected persisted active record after resume, got %+v", record)
	}
	if !containsKind(eventKinds(t, recoveredEvents), "antifreeze.resumed") {
		t.Fatal("expected resumed event")
	}
}

func TestAntifreezeRecoverResumesGracefulShutdown(t *testing.T) {
	directory := t.TempDir()
	antifreeze, sessions, _ := newTestAntifreeze(t, directory)

	if _, err := antifreeze.Start(context.Background(), "xincheng"); err != nil {
		t.Fatalf("start: %v", err)
	}
	antifreeze.Close()

	record, _, err := sessions.Get("xincheng")
	if err != nil {
		t.Fatalf("get record: %v", err)
	}
	if record.Active || record.StopReason != store.AntifreezeReasonShutdown || !record.ResumeOnStart {
		t.Fatalf("expected resumable shutdown record, got %+v", record)
	}

	recoverer, _, _ := newTestAntifreeze(t, directory)
	if err := recoverer.Recover(); err != nil {
		t.Fatalf("recover: %v", err)
	}
	if !recoverer.Running("xincheng") {
		t.Fatal("expected graceful-shutdown session to be resumed")
	}
	if _, err := recoverer.Start(context.Background(), "xincheng"); err == nil {
		t.Fatal("recovered session must reject duplicate start")
	}
	recoverer.mu.RLock()
	loops := len(recoverer.active)
	recoverer.mu.RUnlock()
	if loops != 1 {
		t.Fatalf("expected exactly one recovered loop, got %d", loops)
	}
	record, _, _ = sessions.Get("xincheng")
	if !record.Active || record.NeedsReview || record.ResumeOnStart {
		t.Fatalf("expected clean active record after resume, got %+v", record)
	}
}

func TestAntifreezeResumeRequiresReviewFlag(t *testing.T) {
	directory := t.TempDir()
	antifreeze, _, _ := newTestAntifreeze(t, directory)
	if _, err := antifreeze.Resume("binhe"); err == nil {
		t.Fatal("expected resume without a needs-review record to fail")
	}
}
