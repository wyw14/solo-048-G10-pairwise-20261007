package station

import (
	"fmt"
	"sync"
	"time"

	"heatpulse/internal/timeline"
)

type TargetService struct {
	registry *Registry
	timeline *timeline.Recorder
	mu       sync.Mutex
	revision uint64
}

func NewTargetService(registry *Registry, timelineRecorder *timeline.Recorder) *TargetService {
	var revision uint64
	for _, item := range registry.List() {
		if item.TargetRevision > revision {
			revision = item.TargetRevision
		}
	}
	return &TargetService{registry: registry, timeline: timelineRecorder, revision: revision}
}

func (s *TargetService) Revision() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.revision
}

func (s *TargetService) ManualHold(stationIDs []string, actor string) (uint64, error) {
	s.mu.Lock()
	s.revision++
	revision := s.revision
	updated := make([]Station, 0, len(stationIDs))
	for _, stationID := range stationIDs {
		item, err := s.registry.Update(stationID, func(item *Station) error {
			item.Hold = true
			item.TargetRevision = revision
			return nil
		})
		if err != nil {
			s.mu.Unlock()
			return 0, fmt.Errorf("hold %s by %s: %w", stationID, actor, err)
		}
		updated = append(updated, item)
	}
	s.mu.Unlock()
	for _, item := range updated {
		s.record(item, "manual-hold")
	}
	return revision, nil
}

func (s *TargetService) ReleaseHold(stationID string) (uint64, error) {
	s.mu.Lock()
	s.revision++
	revision := s.revision
	item, err := s.registry.Update(stationID, func(item *Station) error {
		item.Hold = false
		item.TargetRevision = revision
		return nil
	})
	s.mu.Unlock()
	if err == nil {
		s.record(item, "hold-release")
	}
	return revision, err
}

func (s *TargetService) CompareAndPublish(sourceRevision uint64, targets map[string]float64) error {
	s.mu.Lock()
	if sourceRevision != s.revision {
		s.mu.Unlock()
		return fmt.Errorf("dispatch source revision %d is stale; current is %d", sourceRevision, s.revision)
	}
	s.revision++
	revision := s.revision
	updated := make([]Station, 0, len(targets))
	for stationID, celsius := range targets {
		if celsius < 35 || celsius > 95 {
			s.mu.Unlock()
			return fmt.Errorf("target %.1f for %s outside operating envelope", celsius, stationID)
		}
		item, err := s.registry.Update(stationID, func(item *Station) error {
			if item.Hold {
				return fmt.Errorf("station %s is manually held", stationID)
			}
			item.TargetCelsius = celsius
			item.TargetRevision = revision
			item.UpdatedAt = time.Now().UTC()
			return nil
		})
		if err != nil {
			s.mu.Unlock()
			return err
		}
		updated = append(updated, item)
	}
	s.mu.Unlock()
	for _, item := range updated {
		s.record(item, "dispatch")
	}
	return nil
}

func (s *TargetService) record(item Station, source string) {
	_, _ = s.timeline.RecordStation(timeline.KindTargetChange, item.ID, item.ID,
		fmt.Sprintf("target %.1f °C revision %d (%s)", item.TargetCelsius, item.TargetRevision, source),
		timeline.TargetChangePayload{
			StationID: item.ID, Celsius: item.TargetCelsius,
			Revision: item.TargetRevision, Hold: item.Hold, Source: source,
		})
}
