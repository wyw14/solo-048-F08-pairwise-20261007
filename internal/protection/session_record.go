package protection

import (
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"heatpulse/internal/store"
)

// 会话状态与结束原因：这些字符串同时是持久化到 JSON 中的值，
// 重启后需要据此判断会话是应自动恢复还是等待人工确认。
const (
	StateRunning              = "running"               // 循环正在执行
	StateStopped              = "stopped"               // 经 Stop 正常停止
	StateFailed               = "failed"                // 循环 worker 自行异常退出
	StateInterrupted          = "interrupted"           // 进程优雅关闭时仍在运行，重启可自动恢复
	StateAwaitingConfirmation = "awaiting_confirmation" // 上次进程异常退出（崩溃/断电），需人工确认

	ReasonManualStop   = "manual_stop"   // 操作员调用 Stop
	ReasonWorkerError  = "worker_error"  // 循环 worker 返回错误
	ReasonShutdown     = "shutdown"      // 进程优雅关闭打断
	ReasonStartFailure = "start_failure" // 启动后 worker 未在时限内就绪
	ReasonAbandoned    = "abandoned"     // 人工确认放弃崩溃遗留会话
	ReasonProcessExit  = "process_exit"  // 上次进程未正常停止循环即退出
)

// ErrNoSession 表示指定站点没有最近一次防冻会话记录。
var ErrNoSession = errors.New("antifreeze session not found")

// AntifreezeSession 是对外暴露、同时持久化到磁盘的会话记录。
type AntifreezeSession struct {
	ID            string     `json:"id"`
	StationID     string     `json:"station_id"`
	Active        bool       `json:"active"`
	State         string     `json:"state"`
	EndReason     string     `json:"end_reason,omitempty"`
	StartedAt     time.Time  `json:"started_at"`
	StoppedAt     *time.Time `json:"stopped_at,omitempty"`
	LastTickAt    *time.Time `json:"last_tick_at,omitempty"`
	RestoredAt    *time.Time `json:"restored_at,omitempty"`
	FailureDetail string     `json:"failure_detail,omitempty"`
	Ticks         int        `json:"ticks"`
}

// NeedsConfirmation 报告会话在重启后是否必须等待人工确认
// （崩溃/断电遗留的 running 会话无法确认现场是否安全）。
func (s AntifreezeSession) NeedsConfirmation() bool {
	return s.State == StateAwaitingConfirmation
}

// sessionFile 是 antifreeze_sessions.json 的磁盘结构。
// Current 指向每个站点最近一次会话；History 保留有界的已结束会话供审计。
type sessionFile struct {
	Version   int                          `json:"version"`
	UpdatedAt time.Time                    `json:"updated_at"`
	Current   map[string]AntifreezeSession `json:"current"`
	History   []AntifreezeSession          `json:"history,omitempty"`
}

const sessionHistoryLimit = 32

// SessionStore 负责会话记录的原子持久化。
type SessionStore struct {
	file *store.File[sessionFile]
}

func NewSessionStore(directory string) *SessionStore {
	return &SessionStore{file: store.NewFile[sessionFile](filepath.Join(directory, "antifreeze_sessions.json"))}
}

// Load 读取全部会话记录；文件不存在时返回空记录。
func (s *SessionStore) Load() (sessionFile, error) {
	record, err := s.file.Load()
	if err != nil {
		return sessionFile{}, err
	}
	if record.Current == nil {
		record.Current = make(map[string]AntifreezeSession)
	}
	return record, nil
}

// PutCurrent 写入（或覆盖）某站点的最近会话。
func (s *SessionStore) PutCurrent(session AntifreezeSession) error {
	if session.StationID == "" {
		return fmt.Errorf("antifreeze session station id is required")
	}
	_, err := s.file.Update(func(record *sessionFile) error {
		if record.Current == nil {
			record.Current = make(map[string]AntifreezeSession)
		}
		record.Version = 1
		record.UpdatedAt = time.Now().UTC()
		record.Current[session.StationID] = session
		return nil
	})
	return err
}

// Complete 把会话从 current 移出并追加到有界历史。
func (s *SessionStore) Complete(session AntifreezeSession) error {
	_, err := s.file.Update(func(record *sessionFile) error {
		if record.Current == nil {
			record.Current = make(map[string]AntifreezeSession)
		}
		delete(record.Current, session.StationID)
		record.History = append(record.History, session)
		if len(record.History) > sessionHistoryLimit {
			record.History = record.History[len(record.History)-sessionHistoryLimit:]
		}
		record.Version = 1
		record.UpdatedAt = time.Now().UTC()
		return nil
	})
	return err
}

// Latest 返回站点的最近会话：优先 current，其次历史记录末尾。
func (s *SessionStore) Latest(stationID string) (AntifreezeSession, bool, error) {
	record, err := s.Load()
	if err != nil {
		return AntifreezeSession{}, false, err
	}
	if session, ok := record.Current[stationID]; ok {
		return session, true, nil
	}
	for i := len(record.History) - 1; i >= 0; i-- {
		if record.History[i].StationID == stationID {
			return record.History[i], true, nil
		}
	}
	return AntifreezeSession{}, false, nil
}

// MarkRecovery 把每个站点崩溃遗留的 running 会话改写为 awaiting_confirmation，
// interrupted 会话保持不变（允许自动恢复）。返回各站点最终的最近会话记录。
func (s *SessionStore) MarkRecovery() (map[string]AntifreezeSession, error) {
	record, err := s.file.Update(func(record *sessionFile) error {
		if record.Current == nil {
			record.Current = make(map[string]AntifreezeSession)
		}
		now := time.Now().UTC()
		changed := false
		for stationID, session := range record.Current {
			if session.State != StateRunning {
				continue
			}
			// 优雅关闭会先写入 interrupted；这里仍是 running 只可能是进程被强杀。
			session.State = StateAwaitingConfirmation
			session.Active = false
			session.EndReason = ReasonProcessExit
			session.FailureDetail = "previous process exited without stopping antifreeze circulation"
			session.StoppedAt = &now
			record.Current[stationID] = session
			changed = true
		}
		if changed {
			record.Version = 1
			record.UpdatedAt = now
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return record.Current, nil
}
