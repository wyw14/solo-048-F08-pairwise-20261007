package protection

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"

	"heatpulse/internal/exchanger"
)

const (
	startTimeout  = time.Second
	flushInterval = 200 * time.Millisecond
)

type activeAntifreeze struct {
	session    AntifreezeSession
	baseTicks  int
	cancel     context.CancelFunc
	done       chan struct{}
	ready      chan struct{} // runner 成功就绪后关闭
	stopReason string        // 非空表示由 Stop/Close 主导终止，落盘由主导方负责
}

// RecoveryReport 描述进程启动时对历史会话的识别结果。
type RecoveryReport struct {
	Resumed              []AntifreezeSession `json:"resumed"`
	AwaitingConfirmation []AntifreezeSession `json:"awaiting_confirmation"`
	Failed               []AntifreezeSession `json:"failed"`
}

type Antifreeze struct {
	runner *exchanger.CirculationRunner
	store  *SessionStore
	mu     sync.RWMutex
	active map[string]*activeAntifreeze
}

func NewAntifreeze(runner *exchanger.CirculationRunner, sessions *SessionStore) *Antifreeze {
	return &Antifreeze{runner: runner, store: sessions, active: make(map[string]*activeAntifreeze)}
}

// Start 为站点开启新的防冻循环。存在活动循环、崩溃遗留待确认会话或
// 未处理的中断会话时拒绝启动，因此重复启动不会产生第二条循环。
func (a *Antifreeze) Start(requestContext context.Context, stationID string) (AntifreezeSession, error) {
	if err := requestContext.Err(); err != nil {
		return AntifreezeSession{}, err
	}
	if stationID == "" {
		return AntifreezeSession{}, fmt.Errorf("station id is required")
	}
	session := AntifreezeSession{
		ID: uuid.NewString(), StationID: stationID, Active: true,
		State: StateRunning, StartedAt: time.Now().UTC(),
	}
	return a.launch(session, false)
}

// Resume 由人工确认后恢复一个 awaiting_confirmation 会话，沿用原会话 ID，
// 累计的循环 tick 继续计数。
func (a *Antifreeze) Resume(requestContext context.Context, stationID string) (AntifreezeSession, error) {
	if err := requestContext.Err(); err != nil {
		return AntifreezeSession{}, err
	}
	a.mu.RLock()
	current := a.active[stationID]
	a.mu.RUnlock()
	if current != nil {
		return current.session, fmt.Errorf("antifreeze already active for %s", stationID)
	}
	session, err := a.lastSession(stationID)
	if err != nil {
		return AntifreezeSession{}, err
	}
	switch session.State {
	case StateAwaitingConfirmation:
		// 人工确认后允许恢复。
	case StateInterrupted:
		return AntifreezeSession{}, fmt.Errorf("antifreeze session for %s is pending automatic recovery", stationID)
	default:
		return AntifreezeSession{}, fmt.Errorf("antifreeze session for %s is not resumable (state %s)", stationID, session.State)
	}
	restoredAt := time.Now().UTC()
	session.RestoredAt = &restoredAt
	return a.launch(session, true)
}

// Abandon 人工确认放弃崩溃遗留会话：不恢复循环，记录收尾原因。
func (a *Antifreeze) Abandon(stationID string) (AntifreezeSession, error) {
	a.mu.RLock()
	current := a.active[stationID]
	a.mu.RUnlock()
	if current != nil {
		return AntifreezeSession{}, fmt.Errorf("antifreeze still active for %s, stop it before abandoning", stationID)
	}
	session, err := a.lastSession(stationID)
	if err != nil {
		return AntifreezeSession{}, err
	}
	if session.State != StateAwaitingConfirmation {
		return AntifreezeSession{}, fmt.Errorf("antifreeze session for %s does not need confirmation (state %s)", stationID, session.State)
	}
	now := time.Now().UTC()
	session.Active = false
	session.State = StateStopped
	session.EndReason = ReasonAbandoned
	session.FailureDetail = ""
	session.StoppedAt = &now
	if err := a.store.Complete(session); err != nil {
		return AntifreezeSession{}, err
	}
	return session, nil
}

// lastSession 读取站点最近会话：活动落盘记录优先，其次已结束历史。
func (a *Antifreeze) lastSession(stationID string) (AntifreezeSession, error) {
	session, ok, err := a.store.Latest(stationID)
	if err != nil {
		return AntifreezeSession{}, err
	}
	if !ok {
		return AntifreezeSession{}, fmt.Errorf("%w: %s", ErrNoSession, stationID)
	}
	return session, nil
}

// launch 持久化 running 记录后再启动循环 worker；resume=true 时沿用原会话。
func (a *Antifreeze) launch(session AntifreezeSession, resume bool) (AntifreezeSession, error) {
	a.mu.Lock()
	if current := a.active[session.StationID]; current != nil {
		a.mu.Unlock()
		return current.session, fmt.Errorf("antifreeze already active for %s", session.StationID)
	}
	if !resume {
		// 拦截未处理的遗留会话，避免在同一站点开出第二条循环。
		record, err := a.store.Load()
		if err != nil {
			a.mu.Unlock()
			return AntifreezeSession{}, err
		}
		if prior, ok := record.Current[session.StationID]; ok {
			switch prior.State {
			case StateRunning, StateInterrupted:
				a.mu.Unlock()
				return AntifreezeSession{}, fmt.Errorf("antifreeze session %s for %s is still %s", prior.ID, prior.StationID, prior.State)
			case StateAwaitingConfirmation:
				a.mu.Unlock()
				return AntifreezeSession{}, fmt.Errorf("antifreeze session %s for %s requires manual confirmation (resume or abandon)", prior.ID, prior.StationID)
			}
		}
	}
	session.Active = true
	session.State = StateRunning
	session.EndReason = ""
	session.FailureDetail = ""
	session.StoppedAt = nil
	if err := a.store.PutCurrent(session); err != nil {
		a.mu.Unlock()
		return AntifreezeSession{}, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	entry := &activeAntifreeze{
		session:   session,
		baseTicks: a.runner.Cycles(session.StationID),
		cancel:    cancel,
		done:      make(chan struct{}),
		ready:     make(chan struct{}),
	}
	a.active[session.StationID] = entry
	a.mu.Unlock()

	go a.loop(entry, ctx)
	select {
	case <-entry.ready:
		return session, nil
	case <-entry.done:
		// worker 在就绪前退出，finishWorker 已写入 failed 记录。
		failed, _ := a.lastSession(session.StationID)
		return failed, fmt.Errorf("antifreeze circulation did not start for %s", session.StationID)
	case <-time.After(startTimeout):
		entry.stopReason = ReasonStartFailure
		entry.cancel()
		<-entry.done
		failed := a.failLaunch(entry, "circulation did not become ready within the start timeout")
		return failed, fmt.Errorf("antifreeze circulation did not start for %s", session.StationID)
	}
}

// failLaunch 收尾一个未成功就绪的启动尝试：摘除活动表并落盘 failed。
func (a *Antifreeze) failLaunch(entry *activeAntifreeze, detail string) AntifreezeSession {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.active, entry.session.StationID)
	now := time.Now().UTC()
	entry.session.Active = false
	entry.session.State = StateFailed
	entry.session.EndReason = ReasonStartFailure
	entry.session.FailureDetail = detail
	entry.session.StoppedAt = &now
	_ = a.store.Complete(entry.session)
	return entry.session
}

// loop 运行循环并定期把 tick 计数落盘；worker 自行异常退出时记录 failed。
func (a *Antifreeze) loop(entry *activeAntifreeze, ctx context.Context) {
	defer close(entry.done)
	started := make(chan struct{})
	runErr := make(chan error, 1)
	go func() { runErr <- a.runner.Run(ctx, entry.session.StationID, started) }()
	select {
	case <-started:
		close(entry.ready)
	case err := <-runErr:
		a.finishWorker(entry, err)
		return
	case <-ctx.Done():
		<-runErr
		return
	}
	flusher := time.NewTicker(flushInterval)
	defer flusher.Stop()
	for {
		select {
		case <-flusher.C:
			a.persistTicks(entry)
		case err := <-runErr:
			a.persistTicks(entry)
			a.finishWorker(entry, err)
			return
		}
	}
}

// persistTicks 把 pump 累计 tick 折算进会话并落盘；循环仍在运行时只更新 tick 字段。
func (a *Antifreeze) persistTicks(entry *activeAntifreeze) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.active[entry.session.StationID] != entry {
		return
	}
	total := a.runner.Cycles(entry.session.StationID)
	ticks := entry.session.Ticks + total - entry.baseTicks
	if ticks < entry.session.Ticks {
		return // pump 计数器异常回退时不覆盖
	}
	if ticks == entry.session.Ticks && entry.session.LastTickAt != nil {
		return
	}
	if ticks > 0 {
		now := time.Now().UTC()
		entry.session.LastTickAt = &now
	}
	// session.Ticks 存绝对值；baseTicks 同步到当前泵计数，避免终止时重复折算。
	entry.session.Ticks = ticks
	entry.baseTicks = total
	// 磁盘写失败不致命：下一个 flush 周期或终止时会重试。
	_ = a.store.PutCurrent(entry.session)
}

// finishWorker 处理 worker 退出：由 Stop/Close 主导的终止落盘归主导方；
// 否则是循环自行异常退出，记录 failed + worker_error。
func (a *Antifreeze) finishWorker(entry *activeAntifreeze, runErr error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.active[entry.session.StationID] != entry {
		return
	}
	if entry.stopReason != "" {
		return
	}
	delete(a.active, entry.session.StationID)
	now := time.Now().UTC()
	total := a.runner.Cycles(entry.session.StationID)
	entry.session.Ticks += total - entry.baseTicks
	entry.session.Active = false
	entry.session.State = StateFailed
	entry.session.EndReason = ReasonWorkerError
	entry.session.StoppedAt = &now
	if runErr != nil {
		entry.session.FailureDetail = runErr.Error()
	} else {
		entry.session.FailureDetail = "circulation worker exited unexpectedly"
	}
	_ = a.store.Complete(entry.session)
}

func (a *Antifreeze) Stop(stationID string) (AntifreezeSession, error) {
	a.mu.Lock()
	current := a.active[stationID]
	if current == nil {
		a.mu.Unlock()
		return AntifreezeSession{}, fmt.Errorf("antifreeze not active for %s", stationID)
	}
	current.stopReason = ReasonManualStop
	// 先从活动表摘除，worker 退出时的 tick flush 便不会回写 running 覆盖终态。
	delete(a.active, stationID)
	current.cancel()
	a.mu.Unlock()
	waitErr := error(nil)
	select {
	case <-current.done:
	case <-time.After(time.Second):
		waitErr = fmt.Errorf("antifreeze worker did not stop for %s", stationID)
	}
	a.mu.Lock()
	now := time.Now().UTC()
	total := a.runner.Cycles(stationID)
	current.session.Ticks += total - current.baseTicks
	current.session.Active = false
	current.session.State = StateStopped
	current.session.EndReason = ReasonManualStop
	current.session.StoppedAt = &now
	finalized := current.session
	storeErr := a.store.Complete(finalized)
	a.mu.Unlock()
	if waitErr != nil {
		return AntifreezeSession{}, waitErr
	}
	if storeErr != nil {
		return AntifreezeSession{}, storeErr
	}
	return finalized, nil
}

// Close 在进程优雅关闭时把活动会话标记为 interrupted，重启后可自动恢复。
func (a *Antifreeze) Close(ctx context.Context) error {
	a.mu.Lock()
	entries := make([]*activeAntifreeze, 0, len(a.active))
	for _, entry := range a.active {
		entries = append(entries, entry)
	}
	for _, entry := range entries {
		entry.stopReason = ReasonShutdown
		// 摘除后 worker 的退出 flush 不会覆盖这里写入的 interrupted 记录。
		delete(a.active, entry.session.StationID)
		total := a.runner.Cycles(entry.session.StationID)
		entry.session.Ticks += total - entry.baseTicks
		entry.session.Active = false
		entry.session.State = StateInterrupted
		entry.session.EndReason = ReasonShutdown
		if err := a.store.PutCurrent(entry.session); err != nil {
			// 保留在活动表以便重试 Close；已经摘除的条目恢复回去。
			a.active[entry.session.StationID] = entry
			a.mu.Unlock()
			return err
		}
		entry.cancel()
	}
	a.mu.Unlock()
	var firstErr error
	for _, entry := range entries {
		select {
		case <-entry.done:
		case <-ctx.Done():
			if firstErr == nil {
				firstErr = ctx.Err()
			}
		}
	}
	return firstErr
}

// Recover 在启动时识别磁盘上的遗留会话：
// 崩溃遗留（running）由存储层改写为 awaiting_confirmation；
// 优雅关闭（interrupted）且站点仍在册的会话自动续跑，其余转人工确认。
func (a *Antifreeze) Recover(ctx context.Context, knownStations map[string]bool) (RecoveryReport, error) {
	report := RecoveryReport{Resumed: []AntifreezeSession{}, AwaitingConfirmation: []AntifreezeSession{}, Failed: []AntifreezeSession{}}
	current, err := a.store.MarkRecovery()
	if err != nil {
		return report, err
	}
	stationIDs := make([]string, 0, len(current))
	for stationID := range current {
		stationIDs = append(stationIDs, stationID)
	}
	sort.Strings(stationIDs)
	for _, stationID := range stationIDs {
		session := current[stationID]
		switch session.State {
		case StateInterrupted:
			if !knownStations[stationID] {
				report.AwaitingConfirmation = append(report.AwaitingConfirmation,
					a.requireConfirmation(session, "station no longer registered"))
				continue
			}
			restoredAt := time.Now().UTC()
			session.RestoredAt = &restoredAt
			if _, err := a.launch(session, true); err != nil {
				failed, _ := a.lastSession(stationID)
				report.Failed = append(report.Failed, failed)
				continue
			}
			resumed, _ := a.Session(stationID)
			report.Resumed = append(report.Resumed, resumed)
		case StateAwaitingConfirmation:
			report.AwaitingConfirmation = append(report.AwaitingConfirmation, session)
		}
	}
	return report, ctx.Err()
}

func (a *Antifreeze) requireConfirmation(session AntifreezeSession, detail string) AntifreezeSession {
	now := time.Now().UTC()
	session.Active = false
	session.State = StateAwaitingConfirmation
	session.FailureDetail = detail
	session.StoppedAt = &now
	_ = a.store.PutCurrent(session)
	return session
}

// Session 返回站点最近一次会话（活动中或磁盘记录）。
func (a *Antifreeze) Session(stationID string) (AntifreezeSession, error) {
	a.mu.RLock()
	current := a.active[stationID]
	if current != nil {
		session := current.session
		a.mu.RUnlock()
		return session, nil
	}
	a.mu.RUnlock()
	return a.lastSession(stationID)
}

// Sessions 返回全部站点的最近会话记录，按站点排序。
func (a *Antifreeze) Sessions() ([]AntifreezeSession, error) {
	a.mu.RLock()
	live := make(map[string]AntifreezeSession, len(a.active))
	for stationID, entry := range a.active {
		live[stationID] = entry.session
	}
	a.mu.RUnlock()
	record, err := a.store.Load()
	if err != nil {
		return nil, err
	}
	merged := make(map[string]AntifreezeSession, len(record.Current)+len(live)+len(record.History))
	for _, session := range record.History {
		merged[session.StationID] = session
	}
	for stationID, session := range record.Current {
		merged[stationID] = session
	}
	for stationID, session := range live {
		merged[stationID] = session
	}
	result := make([]AntifreezeSession, 0, len(merged))
	for _, session := range merged {
		result = append(result, session)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].StationID < result[j].StationID })
	return result, nil
}

// Ticks 返回会话累计的循环 tick（含尚未落盘的部分）；重启后从磁盘续计。
func (a *Antifreeze) Ticks(stationID string) int {
	a.mu.RLock()
	current := a.active[stationID]
	if current != nil {
		ticks := current.session.Ticks + a.runner.Cycles(stationID) - current.baseTicks
		a.mu.RUnlock()
		if ticks < 0 {
			return 0
		}
		return ticks
	}
	a.mu.RUnlock()
	session, err := a.lastSession(stationID)
	if err != nil {
		return 0
	}
	return session.Ticks
}

func (a *Antifreeze) Running(stationID string) bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.active[stationID] != nil
}
