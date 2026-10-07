package protection

import (
	"context"
	"errors"
	"testing"
	"time"

	"heatpulse/internal/exchanger"
	"heatpulse/internal/pump"
)

func newTestAntifreeze(t *testing.T) (*Antifreeze, string) {
	t.Helper()
	directory := t.TempDir()
	runner := exchanger.NewCirculationRunner(pump.NewRunner())
	store := NewSessionStore(directory)
	return NewAntifreeze(runner, store), directory
}

func recreateAntifreeze(t *testing.T, directory string) *Antifreeze {
	t.Helper()
	runner := exchanger.NewCirculationRunner(pump.NewRunner())
	return NewAntifreeze(runner, NewSessionStore(directory))
}

// waitFor 轮询直到条件成立，模拟异步 tick 落盘。
func waitFor(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}

// crashSim 模拟进程被强杀：摘除活动条目后直接取消 worker，
// 不经过 Close，因此磁盘保留 running 记录。
func crashSim(a *Antifreeze, stationID string) {
	a.mu.Lock()
	entry := a.active[stationID]
	delete(a.active, stationID)
	a.mu.Unlock()
	entry.cancel()
	<-entry.done
}

func TestStartStopPersistsSessionAndTicks(t *testing.T) {
	a, _ := newTestAntifreeze(t)
	ctx := context.Background()

	session, err := a.Start(ctx, "xincheng")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if session.State != StateRunning || !session.Active {
		t.Fatalf("unexpected session after start: %+v", session)
	}

	// tick 循环每 5ms 一次，落盘每 200ms 一次。
	waitFor(t, 2*time.Second, func() bool {
		record, err := a.lastSession("xincheng")
		return err == nil && record.Ticks > 0
	})

	stopped, err := a.Stop("xincheng")
	if err != nil {
		t.Fatalf("stop: %v", err)
	}
	if stopped.State != StateStopped || stopped.Active {
		t.Fatalf("unexpected session after stop: %+v", stopped)
	}
	if stopped.EndReason != ReasonManualStop || stopped.StoppedAt == nil {
		t.Fatalf("stop reason/time not recorded: %+v", stopped)
	}
	if stopped.Ticks == 0 {
		t.Fatal("ticks not recorded")
	}

	// 磁盘上 current 已清空，最近记录进入历史；Session 仍能查到最终态。
	persisted, err := a.Session("xincheng")
	if err != nil {
		t.Fatalf("session after stop: %v", err)
	}
	if persisted.State != StateStopped || persisted.Ticks != stopped.Ticks {
		t.Fatalf("persisted session mismatch: %+v", persisted)
	}
	if a.Running("xincheng") {
		t.Fatal("station still marked running after stop")
	}
}

func TestDuplicateStartDoesNotCreateSecondLoop(t *testing.T) {
	a, _ := newTestAntifreeze(t)
	first, err := a.Start(context.Background(), "binhe")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	second, err := a.Start(context.Background(), "binhe")
	if err == nil {
		t.Fatalf("duplicate start unexpectedly succeeded: %+v", second)
	}
	running, err := a.Session("binhe")
	if err != nil || running.ID != first.ID {
		t.Fatalf("original session should be untouched: %+v err=%v", running, err)
	}
	// 磁盘上仍只有同一条会话记录。
	persisted, err := a.lastSession("binhe")
	if err != nil || persisted.ID != first.ID {
		t.Fatalf("no new session record should be persisted: %+v err=%v", persisted, err)
	}

	// 只能停一次，证明只存在一条循环。
	if _, err := a.Stop("binhe"); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if _, err := a.Stop("binhe"); err == nil {
		t.Fatal("second stop unexpectedly succeeded")
	}
}

func TestCrashLeavesSessionAwaitingConfirmation(t *testing.T) {
	a, directory := newTestAntifreeze(t)
	session, err := a.Start(context.Background(), "nanhu")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	waitFor(t, 2*time.Second, func() bool {
		record, err := a.lastSession("nanhu")
		return err == nil && record.Ticks > 0
	})
	// 以最后一次已落盘的 tick 为准：强杀无法挽救尚未刷新的计数。
	ticksBefore, err := a.lastSession("nanhu")
	if err != nil {
		t.Fatal(err)
	}
	crashSim(a, "nanhu")

	// 新进程启动：没有经过 Close，running 记录必须转人工确认。
	fresh := recreateAntifreeze(t, directory)
	report, err := fresh.Recover(context.Background(), map[string]bool{"nanhu": true})
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if len(report.Resumed) != 0 || len(report.AwaitingConfirmation) != 1 {
		t.Fatalf("unexpected recovery report: %+v", report)
	}
	record, err := fresh.Session("nanhu")
	if err != nil {
		t.Fatalf("session: %v", err)
	}
	if record.State != StateAwaitingConfirmation || record.Active {
		t.Fatalf("expected awaiting_confirmation, got %+v", record)
	}
	if record.Ticks != ticksBefore.Ticks {
		t.Fatalf("ticks lost across crash: got %d want %d", record.Ticks, ticksBefore.Ticks)
	}
	if record.ID != session.ID {
		t.Fatal("crash record should keep original session id")
	}

	// 未确认前禁止直接启动新循环。
	if _, err := fresh.Start(context.Background(), "nanhu"); err == nil {
		t.Fatal("start without confirmation unexpectedly succeeded")
	}

	// 人工确认恢复：沿用原会话，tick 续计。
	resumed, err := fresh.Resume(context.Background(), "nanhu")
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if resumed.ID != session.ID || resumed.RestoredAt == nil {
		t.Fatalf("resumed session mismatch: %+v", resumed)
	}
	if !fresh.Running("nanhu") {
		t.Fatal("resumed session not running")
	}
	waitFor(t, 2*time.Second, func() bool { return fresh.Ticks("nanhu") > ticksBefore.Ticks })
}

func TestAbandonAfterCrashAllowsFreshStart(t *testing.T) {
	a, directory := newTestAntifreeze(t)
	if _, err := a.Start(context.Background(), "xijiao"); err != nil {
		t.Fatalf("start: %v", err)
	}
	crashSim(a, "xijiao")

	fresh := recreateAntifreeze(t, directory)
	if _, err := fresh.Recover(context.Background(), map[string]bool{"xijiao": true}); err != nil {
		t.Fatalf("recover: %v", err)
	}
	abandoned, err := fresh.Abandon("xijiao")
	if err != nil {
		t.Fatalf("abandon: %v", err)
	}
	if abandoned.State != StateStopped || abandoned.EndReason != ReasonAbandoned {
		t.Fatalf("unexpected abandoned session: %+v", abandoned)
	}
	restart, err := fresh.Start(context.Background(), "xijiao")
	if err != nil {
		t.Fatalf("start after abandon: %v", err)
	}
	if restart.State != StateRunning {
		t.Fatalf("fresh start not running: %+v", restart)
	}
}

func TestGracefulShutdownAutoResumes(t *testing.T) {
	a, directory := newTestAntifreeze(t)
	session, err := a.Start(context.Background(), "xincheng")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	waitFor(t, 2*time.Second, func() bool { return a.Ticks("xincheng") > 0 })

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := a.Close(shutdownCtx); err != nil {
		t.Fatalf("close: %v", err)
	}

	// 关闭后磁盘记录为 interrupted 且保留 tick。
	left, err := a.lastSession("xincheng")
	if err != nil {
		t.Fatalf("load after close: %v", err)
	}
	if left.State != StateInterrupted || left.EndReason != ReasonShutdown || left.Ticks == 0 {
		t.Fatalf("unexpected record after close: %+v", left)
	}

	// 新进程自动恢复。
	fresh := recreateAntifreeze(t, directory)
	report, err := fresh.Recover(context.Background(), map[string]bool{"xincheng": true})
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if len(report.Resumed) != 1 || len(report.AwaitingConfirmation) != 0 {
		t.Fatalf("unexpected recovery report: %+v", report)
	}
	resumed := report.Resumed[0]
	if resumed.ID != session.ID || !resumed.Active || resumed.RestoredAt == nil {
		t.Fatalf("resumed session mismatch: %+v", resumed)
	}
	if resumed.Ticks != left.Ticks {
		t.Fatalf("ticks not carried over: got %d want %d", resumed.Ticks, left.Ticks)
	}
	if !fresh.Running("xincheng") {
		t.Fatal("session not running after auto recovery")
	}
	waitFor(t, 2*time.Second, func() bool { return fresh.Ticks("xincheng") > left.Ticks })
}

func TestRecoveryOfUnknownStationRequiresConfirmation(t *testing.T) {
	a, directory := newTestAntifreeze(t)
	if _, err := a.Start(context.Background(), "ghost"); err != nil {
		t.Fatalf("start: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := a.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}

	fresh := recreateAntifreeze(t, directory)
	report, err := fresh.Recover(context.Background(), map[string]bool{"xincheng": true})
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if len(report.Resumed) != 0 || len(report.AwaitingConfirmation) != 1 {
		t.Fatalf("unexpected report: %+v", report)
	}
	if report.AwaitingConfirmation[0].State != StateAwaitingConfirmation {
		t.Fatalf("expected awaiting confirmation, got %+v", report.AwaitingConfirmation[0])
	}
}

func TestResumeAndAbandonValidateState(t *testing.T) {
	a, _ := newTestAntifreeze(t)
	if _, err := a.Start(context.Background(), "xincheng"); err != nil {
		t.Fatalf("start: %v", err)
	}
	// 运行中不能 Resume/Abandon。
	if _, err := a.Resume(context.Background(), "xincheng"); err == nil {
		t.Fatal("resume on running session unexpectedly succeeded")
	}
	if _, err := a.Abandon("xincheng"); err == nil {
		t.Fatal("abandon on running session unexpectedly succeeded")
	}
	if _, err := a.Stop("xincheng"); err != nil {
		t.Fatalf("stop: %v", err)
	}
	// 已正常结束的会话不能 Resume/Abandon。
	if _, err := a.Resume(context.Background(), "xincheng"); err == nil {
		t.Fatal("resume on stopped session unexpectedly succeeded")
	}
	if _, err := a.Abandon("xincheng"); err == nil {
		t.Fatal("abandon on stopped session unexpectedly succeeded")
	}
	// 从未存在过的站点返回 ErrNoSession。
	if _, err := a.Session("unknown"); !errors.Is(err, ErrNoSession) {
		t.Fatalf("expected ErrNoSession, got %v", err)
	}
}

func TestSessionsIncludesRunningAndFinished(t *testing.T) {
	a, _ := newTestAntifreeze(t)
	if _, err := a.Start(context.Background(), "xincheng"); err != nil {
		t.Fatalf("start: %v", err)
	}
	if _, err := a.Start(context.Background(), "binhe"); err != nil {
		t.Fatalf("start: %v", err)
	}
	if _, err := a.Stop("binhe"); err != nil {
		t.Fatalf("stop: %v", err)
	}
	sessions, err := a.Sessions()
	if err != nil {
		t.Fatalf("sessions: %v", err)
	}
	byStation := map[string]AntifreezeSession{}
	for _, session := range sessions {
		byStation[session.StationID] = session
	}
	if byStation["xincheng"].State != StateRunning {
		t.Fatalf("xincheng should run: %+v", byStation["xincheng"])
	}
	if byStation["binhe"].State != StateStopped {
		t.Fatalf("binhe should be stopped: %+v", byStation["binhe"])
	}
}

func TestMarkRecoveryOnlyRewritesRunning(t *testing.T) {
	directory := t.TempDir()
	store := NewSessionStore(directory)
	now := time.Now().UTC()
	running := AntifreezeSession{ID: "r1", StationID: "a", Active: true, State: StateRunning, StartedAt: now}
	stopped := AntifreezeSession{ID: "s1", StationID: "b", Active: false, State: StateStopped, StartedAt: now}
	if err := store.PutCurrent(running); err != nil {
		t.Fatal(err)
	}
	if err := store.Complete(stopped); err != nil {
		t.Fatal(err)
	}
	current, err := store.MarkRecovery()
	if err != nil {
		t.Fatalf("mark recovery: %v", err)
	}
	if current["a"].State != StateAwaitingConfirmation || current["a"].EndReason != ReasonProcessExit {
		t.Fatalf("running record not rewritten: %+v", current["a"])
	}
	// 幂等：再次扫描不应产生变化。
	current2, err := store.MarkRecovery()
	if err != nil {
		t.Fatalf("second mark recovery: %v", err)
	}
	if current2["a"].State != StateAwaitingConfirmation {
		t.Fatalf("second scan changed state: %+v", current2["a"])
	}
}
