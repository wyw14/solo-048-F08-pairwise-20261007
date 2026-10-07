package protection

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"heatpulse/internal/exchanger"
	"heatpulse/internal/store"
)

type AntifreezeSession struct {
	ID          string    `json:"id"`
	StationID   string    `json:"station_id"`
	Active      bool      `json:"active"`
	StartedAt   time.Time `json:"started_at"`
	StoppedAt   time.Time `json:"stopped_at,omitempty"`
	StopReason  string    `json:"stop_reason,omitempty"`
	Ticks       int       `json:"ticks"`
	NeedsReview bool      `json:"needs_review,omitempty"`
}

type activeAntifreeze struct {
	session     AntifreezeSession
	cancel      context.CancelFunc
	done        chan struct{}
	startCycles int
}

type Antifreeze struct {
	runner   *exchanger.CirculationRunner
	sessions *store.AntifreezeSessionStore
	events   *store.EventLog
	// CheckpointInterval controls how often the loop tick count is flushed
	// to the persisted session record.
	CheckpointInterval time.Duration
	mu                 sync.RWMutex
	active             map[string]*activeAntifreeze
}

func NewAntifreeze(runner *exchanger.CirculationRunner, sessions *store.AntifreezeSessionStore, events *store.EventLog) *Antifreeze {
	return &Antifreeze{
		runner: runner, sessions: sessions, events: events,
		CheckpointInterval: time.Second,
		active:             make(map[string]*activeAntifreeze),
	}
}

func (a *Antifreeze) Start(requestContext context.Context, stationID string) (AntifreezeSession, error) {
	if err := requestContext.Err(); err != nil {
		return AntifreezeSession{}, err
	}
	return a.start(stationID, "")
}

// Resume starts circulation for a session that is persisted as needing
// manual confirmation (crash or circulation failure). It is the operator's
// explicit acknowledgement that the station is safe to circulate again.
func (a *Antifreeze) Resume(stationID string) (AntifreezeSession, error) {
	record, ok, err := a.sessions.Get(stationID)
	if err != nil {
		return AntifreezeSession{}, err
	}
	if !ok || !record.NeedsReview {
		return AntifreezeSession{}, fmt.Errorf("no antifreeze session awaiting confirmation for %s", stationID)
	}
	return a.start(stationID, record.ID)
}

func (a *Antifreeze) start(stationID, previousID string) (AntifreezeSession, error) {
	a.mu.Lock()
	if current := a.active[stationID]; current != nil {
		a.mu.Unlock()
		return current.session, fmt.Errorf("antifreeze already active for %s", stationID)
	}
	session := AntifreezeSession{ID: uuid.NewString(), StationID: stationID, Active: true, StartedAt: time.Now().UTC()}
	ctx, cancel := context.WithCancel(context.Background())
	current := &activeAntifreeze{session: session, cancel: cancel, done: make(chan struct{}), startCycles: a.runner.Cycles(stationID)}
	a.active[stationID] = current
	a.mu.Unlock()
	if _, err := a.sessions.Put(recordFromSession(session, false)); err != nil {
		cancel()
		a.deactivate(stationID, current)
		return AntifreezeSession{}, fmt.Errorf("persist antifreeze session: %w", err)
	}
	kind := "antifreeze.started"
	fields := map[string]any{"session_id": session.ID}
	if previousID != "" {
		kind = "antifreeze.resumed"
		fields["previous_session_id"] = previousID
	}
	a.appendEvent(kind, stationID, fmt.Sprintf("antifreeze circulation started for %s", stationID), fields)
	started := make(chan struct{})
	go func() {
		defer close(current.done)
		a.runner.Run(ctx, stationID, started)
		select {
		case <-started:
			// The loop was running; ending without a cancel is unexpected.
			if ctx.Err() == nil {
				a.finishUnexpected(stationID, current)
			}
		default:
			// The loop never started; the Start timeout path records the failure.
		}
	}()
	go a.checkpointLoop(ctx, stationID, current)
	select {
	case <-started:
		return session, nil
	case <-time.After(time.Second):
		cancel()
		<-current.done
		if a.deactivate(stationID, current) {
			a.finishRecord(current, store.AntifreezeReasonStartFailed, false, false)
			a.appendEvent("antifreeze.start_failed", stationID, fmt.Sprintf("antifreeze circulation did not start for %s", stationID), map[string]any{"session_id": session.ID})
		}
		return AntifreezeSession{}, fmt.Errorf("antifreeze circulation did not start")
	}
}

func (a *Antifreeze) Stop(stationID string) (AntifreezeSession, error) {
	return a.stopWithReason(stationID, store.AntifreezeReasonOperator, false)
}

func (a *Antifreeze) stopWithReason(stationID, reason string, resumeOnStart bool) (AntifreezeSession, error) {
	a.mu.Lock()
	current := a.active[stationID]
	if current == nil {
		a.mu.Unlock()
		return AntifreezeSession{}, fmt.Errorf("antifreeze not active for %s", stationID)
	}
	delete(a.active, stationID)
	current.cancel()
	a.mu.Unlock()
	select {
	case <-current.done:
	case <-time.After(time.Second):
		return AntifreezeSession{}, fmt.Errorf("antifreeze worker did not stop")
	}
	a.updateTicks(stationID, current)
	a.mu.Lock()
	current.session.Active = false
	current.session.StoppedAt = time.Now().UTC()
	current.session.StopReason = reason
	session := current.session
	a.mu.Unlock()
	if _, err := a.sessions.Put(recordFromSession(session, resumeOnStart)); err != nil {
		return session, fmt.Errorf("persist antifreeze stop: %w", err)
	}
	a.appendEvent("antifreeze.stopped", stationID, fmt.Sprintf("antifreeze circulation stopped for %s (%s)", stationID, reason), map[string]any{
		"session_id": session.ID, "reason": reason, "ticks": session.Ticks, "resume_on_start": resumeOnStart,
	})
	return session, nil
}

// finishUnexpected records a loop that ended without an explicit stop
// (circulation failure). The session is flagged for manual confirmation.
func (a *Antifreeze) finishUnexpected(stationID string, current *activeAntifreeze) {
	if !a.deactivate(stationID, current) {
		return
	}
	a.updateTicks(stationID, current)
	a.finishRecord(current, store.AntifreezeReasonCirculation, false, true)
	a.appendEvent("antifreeze.needs_review", stationID, fmt.Sprintf("antifreeze circulation for %s ended unexpectedly", stationID), map[string]any{
		"session_id": current.session.ID, "reason": store.AntifreezeReasonCirculation, "ticks": current.session.Ticks,
	})
}

func (a *Antifreeze) finishRecord(current *activeAntifreeze, reason string, resumeOnStart, needsReview bool) {
	a.mu.Lock()
	current.session.Active = false
	current.session.StoppedAt = time.Now().UTC()
	current.session.StopReason = reason
	current.session.NeedsReview = needsReview
	session := current.session
	a.mu.Unlock()
	_, _ = a.sessions.Put(recordFromSession(session, resumeOnStart))
}

// Recover inspects persisted sessions at startup. Sessions that were still
// active when the process last exited (crash) are marked for manual
// confirmation; sessions stopped by a graceful shutdown are restarted.
func (a *Antifreeze) Recover() error {
	records, err := a.sessions.List()
	if err != nil {
		return fmt.Errorf("load antifreeze sessions: %w", err)
	}
	for _, record := range records {
		if record.Active {
			record.Active = false
			record.StoppedAt = time.Now().UTC()
			record.StopReason = store.AntifreezeReasonInterrupted
			record.ResumeOnStart = false
			record.NeedsReview = true
			if _, err := a.sessions.Put(record); err != nil {
				return fmt.Errorf("mark antifreeze session %s interrupted: %w", record.StationID, err)
			}
			a.appendEvent("antifreeze.needs_review", record.StationID, fmt.Sprintf("antifreeze session for %s was interrupted and needs confirmation", record.StationID), map[string]any{
				"session_id": record.ID, "reason": store.AntifreezeReasonInterrupted, "ticks": record.Ticks,
			})
			continue
		}
		if !record.ResumeOnStart || record.NeedsReview {
			continue
		}
		if _, err := a.start(record.StationID, record.ID); err != nil {
			if current, ok, getErr := a.sessions.Get(record.StationID); getErr == nil && ok {
				current.NeedsReview = true
				_, _ = a.sessions.Put(current)
			}
			a.appendEvent("antifreeze.needs_review", record.StationID, fmt.Sprintf("antifreeze session for %s could not be resumed: %v", record.StationID, err), map[string]any{
				"session_id": record.ID, "reason": store.AntifreezeReasonStartFailed,
			})
		}
	}
	return nil
}

func (a *Antifreeze) Session(stationID string) (AntifreezeSession, bool) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	current := a.active[stationID]
	if current == nil {
		return AntifreezeSession{}, false
	}
	return current.session, true
}

// Sessions returns the persisted session state for every station, with live
// tick counts overlaid for stations whose loop is currently running.
func (a *Antifreeze) Sessions() ([]AntifreezeSession, error) {
	records, err := a.sessions.List()
	if err != nil {
		return nil, err
	}
	sessions := make([]AntifreezeSession, 0, len(records))
	for _, record := range records {
		session := sessionFromRecord(record)
		a.mu.RLock()
		if current := a.active[record.StationID]; current != nil && current.session.ID == record.ID {
			session = current.session
		}
		a.mu.RUnlock()
		sessions = append(sessions, session)
	}
	return sessions, nil
}

func (a *Antifreeze) Running(stationID string) bool {
	return a.runner.Running(stationID)
}

func (a *Antifreeze) Close() {
	a.mu.RLock()
	ids := make([]string, 0, len(a.active))
	for stationID := range a.active {
		ids = append(ids, stationID)
	}
	a.mu.RUnlock()
	for _, stationID := range ids {
		_, _ = a.stopWithReason(stationID, store.AntifreezeReasonShutdown, true)
	}
}

// deactivate removes the station's entry only if it still points at the
// given loop, so a stale loop can never remove or replace a newer one.
func (a *Antifreeze) deactivate(stationID string, current *activeAntifreeze) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.active[stationID] != current {
		return false
	}
	delete(a.active, stationID)
	return true
}

func (a *Antifreeze) checkpointLoop(ctx context.Context, stationID string, current *activeAntifreeze) {
	interval := a.CheckpointInterval
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.updateTicks(stationID, current)
		}
	}
}

func (a *Antifreeze) updateTicks(stationID string, current *activeAntifreeze) {
	ticks := a.runner.Cycles(stationID) - current.startCycles
	if ticks < 0 {
		ticks = 0
	}
	a.mu.Lock()
	current.session.Ticks = ticks
	a.mu.Unlock()
	_ = a.sessions.RecordTicks(stationID, ticks)
}

func (a *Antifreeze) appendEvent(kind, stationID, message string, fields map[string]any) {
	if a.events == nil {
		return
	}
	_ = a.events.Append(store.Event{
		ID: uuid.NewString(), Kind: kind, EntityID: stationID, Message: message, Fields: fields,
	})
}

func recordFromSession(session AntifreezeSession, resumeOnStart bool) store.AntifreezeSessionRecord {
	return store.AntifreezeSessionRecord{
		ID: session.ID, StationID: session.StationID, Active: session.Active,
		StartedAt: session.StartedAt, StoppedAt: session.StoppedAt, StopReason: session.StopReason,
		Ticks: session.Ticks, ResumeOnStart: resumeOnStart, NeedsReview: session.NeedsReview,
	}
}

func sessionFromRecord(record store.AntifreezeSessionRecord) AntifreezeSession {
	return AntifreezeSession{
		ID: record.ID, StationID: record.StationID, Active: record.Active,
		StartedAt: record.StartedAt, StoppedAt: record.StoppedAt, StopReason: record.StopReason,
		Ticks: record.Ticks, NeedsReview: record.NeedsReview,
	}
}
