package protection

import (
	"fmt"
	"sync"
	"time"

	"heatpulse/internal/store"
	"heatpulse/internal/timeline"
)

type Latches struct {
	StationID    string    `json:"station_id"`
	Revision     uint64    `json:"revision"`
	HighReturn   bool      `json:"high_return"`
	FreezeRisk   bool      `json:"freeze_risk"`
	PressureTrip bool      `json:"pressure_trip"`
	UpdatedAt    time.Time `json:"updated_at"`
}

type LatchStore struct {
	snapshots *store.StationSnapshotStore
	timeline  *timeline.Recorder
	mu        sync.RWMutex
	latches   map[string]Latches
}

func NewLatchStore(snapshots *store.StationSnapshotStore, timeline *timeline.Recorder) *LatchStore {
	return &LatchStore{snapshots: snapshots, timeline: timeline, latches: make(map[string]Latches)}
}

func (s *LatchStore) Apply(candidate Latches) (Latches, error) {
	if candidate.StationID == "" || candidate.Revision == 0 {
		return Latches{}, fmt.Errorf("latch identity and revision are required")
	}
	candidate.UpdatedAt = time.Now().UTC()
	s.mu.Lock()
	current := s.latches[candidate.StationID]
	if candidate.Revision < current.Revision {
		s.mu.Unlock()
		return current, fmt.Errorf("stale latch revision %d", candidate.Revision)
	}
	s.latches[candidate.StationID] = candidate
	s.mu.Unlock()
	_, err := s.snapshots.MergeProtection(candidate.StationID, store.ProtectionSnapshot{
		Revision: candidate.Revision, HighReturn: candidate.HighReturn,
		FreezeRisk: candidate.FreezeRisk, PressureTrip: candidate.PressureTrip,
		UpdatedAt: candidate.UpdatedAt,
	})
	if err != nil {
		return candidate, err
	}
	_, _ = s.timeline.RecordForStation(timeline.KindProtectionLatch, candidate.StationID, "protection latches updated",
		latchFragment(current), latchFragment(candidate))
	return candidate, nil
}

func latchFragment(latches Latches) map[string]any {
	return map[string]any{
		"latch_revision":      latches.Revision,
		"latch_high_return":   latches.HighReturn,
		"latch_freeze_risk":   latches.FreezeRisk,
		"latch_pressure_trip": latches.PressureTrip,
	}
}

func (s *LatchStore) Get(stationID string) Latches {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.latches[stationID]
}

func (s *LatchStore) Reset(stationID string, revision uint64) (Latches, error) {
	return s.Apply(Latches{StationID: stationID, Revision: revision})
}
