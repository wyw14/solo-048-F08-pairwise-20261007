package exchanger

import (
	"context"
	"fmt"
	"sync"
	"time"

	"heatpulse/internal/pump"
)

type CirculationRunner struct {
	pumps  *pump.Runner
	mu     sync.RWMutex
	errors map[string]error
}

func NewCirculationRunner(pumps *pump.Runner) *CirculationRunner {
	return &CirculationRunner{pumps: pumps, errors: make(map[string]error)}
}

// Run 执行循环直到 ctx 取消或循环自行退出。
// ctx 取消返回 nil；循环自身异常时返回错误并同时记录在错误表中。
func (r *CirculationRunner) Run(ctx context.Context, stationID string, started chan<- struct{}) error {
	if err := r.pumps.Circulate(ctx, stationID, 5*time.Millisecond, started); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		wrapped := fmt.Errorf("antifreeze circulation: %w", err)
		r.mu.Lock()
		r.errors[stationID] = wrapped
		r.mu.Unlock()
		return wrapped
	}
	return nil
}

func (r *CirculationRunner) Running(stationID string) bool {
	return r.pumps.Running(stationID)
}

func (r *CirculationRunner) Cycles(stationID string) int {
	return r.pumps.Ticks(stationID)
}

func (r *CirculationRunner) Error(stationID string) error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.errors[stationID]
}
