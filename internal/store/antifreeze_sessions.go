package store

import (
	"fmt"
	"path/filepath"
	"sort"
	"time"
)

// Antifreeze session stop/interrupt reasons recorded on the persisted record.
const (
	AntifreezeReasonOperator    = "operator_stop"
	AntifreezeReasonShutdown    = "shutdown"
	AntifreezeReasonInterrupted = "interrupted"
	AntifreezeReasonStartFailed = "start_failed"
	AntifreezeReasonCirculation = "circulation_error"
)

// AntifreezeSessionRecord is the persisted lifecycle of one station's
// antifreeze session. The event log holds the full history; this record is
// the latest known state per station and survives restarts.
type AntifreezeSessionRecord struct {
	ID         string    `json:"id"`
	StationID  string    `json:"station_id"`
	Active     bool      `json:"active"`
	StartedAt  time.Time `json:"started_at"`
	StoppedAt  time.Time `json:"stopped_at,omitempty"`
	StopReason string    `json:"stop_reason,omitempty"`
	Ticks      int       `json:"ticks"`
	// ResumeOnStart marks a session that was running during a graceful
	// shutdown and should be restarted automatically on the next boot.
	ResumeOnStart bool `json:"resume_on_start,omitempty"`
	// NeedsReview marks a session whose loop is no longer running but was
	// never cleanly stopped (crash or circulation failure); an operator
	// must confirm before circulation is started again.
	NeedsReview bool      `json:"needs_review,omitempty"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type AntifreezeSessionStore struct {
	file *File[map[string]AntifreezeSessionRecord]
}

func NewAntifreezeSessionStore(directory string) *AntifreezeSessionStore {
	return &AntifreezeSessionStore{file: NewFile[map[string]AntifreezeSessionRecord](filepath.Join(directory, "antifreeze_sessions.json"))}
}

func (s *AntifreezeSessionStore) Put(record AntifreezeSessionRecord) (AntifreezeSessionRecord, error) {
	if record.StationID == "" || record.ID == "" {
		return AntifreezeSessionRecord{}, fmt.Errorf("antifreeze session id and station id are required")
	}
	record.UpdatedAt = time.Now().UTC()
	_, err := s.file.Update(func(all *map[string]AntifreezeSessionRecord) error {
		if *all == nil {
			*all = make(map[string]AntifreezeSessionRecord)
		}
		(*all)[record.StationID] = record
		return nil
	})
	return record, err
}

// RecordTicks merges a tick checkpoint into the persisted record without
// touching lifecycle fields, so a late checkpoint cannot clobber a stop.
func (s *AntifreezeSessionStore) RecordTicks(stationID string, ticks int) error {
	if stationID == "" {
		return fmt.Errorf("station id is required")
	}
	_, err := s.file.Update(func(all *map[string]AntifreezeSessionRecord) error {
		record, ok := (*all)[stationID]
		if !ok {
			return fmt.Errorf("no antifreeze session recorded for %s", stationID)
		}
		record.Ticks = ticks
		record.UpdatedAt = time.Now().UTC()
		(*all)[stationID] = record
		return nil
	})
	return err
}

func (s *AntifreezeSessionStore) Get(stationID string) (AntifreezeSessionRecord, bool, error) {
	all, err := s.file.Load()
	if err != nil {
		return AntifreezeSessionRecord{}, false, err
	}
	record, ok := all[stationID]
	return record, ok, nil
}

func (s *AntifreezeSessionStore) List() ([]AntifreezeSessionRecord, error) {
	all, err := s.file.Load()
	if err != nil {
		return nil, err
	}
	result := make([]AntifreezeSessionRecord, 0, len(all))
	for _, record := range all {
		result = append(result, record)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].StationID < result[j].StationID })
	return result, nil
}
