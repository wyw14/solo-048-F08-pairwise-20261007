package api

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNewAppRecoversInterruptedAntifreeze(t *testing.T) {
	directory := t.TempDir()

	app, err := NewApp(directory)
	if err != nil {
		t.Fatalf("new app: %v", err)
	}
	session, err := app.Antifreeze.Start(context.Background(), "xincheng")
	if err != nil {
		t.Fatalf("start antifreeze: %v", err)
	}
	if !app.Antifreeze.Running("xincheng") {
		t.Fatal("antifreeze should be running")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := app.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}

	// 模拟重启：新 App 应自动识别并恢复 interrupted 会话。
	restarted, err := NewApp(directory)
	if err != nil {
		t.Fatalf("restart app: %v", err)
	}
	defer func() { _ = restarted.Close(context.Background()) }()

	if len(restarted.AntifreezeRecovery.Resumed) != 1 {
		t.Fatalf("expected one resumed session, got %+v", restarted.AntifreezeRecovery)
	}
	resumed := restarted.AntifreezeRecovery.Resumed[0]
	if resumed.ID != session.ID || resumed.StationID != "xincheng" {
		t.Fatalf("resumed session mismatch: %+v", resumed)
	}
	if !restarted.Antifreeze.Running("xincheng") {
		t.Fatal("antifreeze should run again after restart")
	}
}

// TestNewAppMarksCrashedAntifreezeAwaitingConfirmation 直接构造一个崩溃遗留的
// running 状态文件（等价于上一进程被强杀，未执行 Close）。
func TestNewAppMarksCrashedAntifreezeAwaitingConfirmation(t *testing.T) {
	directory := t.TempDir()
	startedAt := time.Now().UTC().Format(time.RFC3339Nano)
	fixture := `{
  "version": 1,
  "updated_at": "` + startedAt + `",
  "current": {
    "binhe": {
      "id": "crashed-session-id",
      "station_id": "binhe",
      "active": true,
      "state": "running",
      "started_at": "` + startedAt + `",
      "ticks": 41
    }
  }
}
`
	if err := os.WriteFile(filepath.Join(directory, "antifreeze_sessions.json"), []byte(fixture), 0o644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}

	restarted, err := NewApp(directory)
	if err != nil {
		t.Fatalf("restart app: %v", err)
	}
	defer func() { _ = restarted.Close(context.Background()) }()

	if len(restarted.AntifreezeRecovery.AwaitingConfirmation) != 1 {
		t.Fatalf("expected one awaiting session, got %+v", restarted.AntifreezeRecovery)
	}
	pending := restarted.AntifreezeRecovery.AwaitingConfirmation[0]
	if pending.StationID != "binhe" || !pending.NeedsConfirmation() || pending.Ticks != 41 {
		t.Fatalf("unexpected pending session: %+v", pending)
	}
	if restarted.Antifreeze.Running("binhe") {
		t.Fatal("crashed session must not auto-start")
	}
	// 未确认前重复启动被拒绝，避免开出第二条循环。
	if _, err := restarted.Antifreeze.Start(context.Background(), "binhe"); err == nil {
		t.Fatal("start while awaiting confirmation should fail")
	}
	// 人工确认后恢复，沿用原会话 ID 与已累计 tick。
	resumed, err := restarted.Antifreeze.Resume(context.Background(), "binhe")
	if err != nil {
		t.Fatalf("resume after crash: %v", err)
	}
	if resumed.ID != "crashed-session-id" || resumed.Ticks != 41 {
		t.Fatalf("resumed session lost identity/ticks: %+v", resumed)
	}
	if !restarted.Antifreeze.Running("binhe") {
		t.Fatal("antifreeze should run after manual resume")
	}
}
