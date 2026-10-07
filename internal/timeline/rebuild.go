package timeline

import (
	"encoding/json"
	"fmt"
	"sort"
)

// LatchState is the rebuilt protection-latch state of one station.
type LatchState struct {
	Revision     uint64 `json:"revision"`
	HighReturn   bool   `json:"high_return"`
	FreezeRisk   bool   `json:"freeze_risk"`
	PressureTrip bool   `json:"pressure_trip"`
}

// TargetState is the rebuilt dispatch-target state of one station.
type TargetState struct {
	Celsius  float64 `json:"celsius"`
	Revision uint64  `json:"revision"`
	Hold     bool    `json:"hold"`
}

// PartitionState is the folded operations state of one partition at a point
// on the timeline.
type PartitionState struct {
	Phase            string                 `json:"phase"`
	PumpStage        int                    `json:"pump_stage"`
	BypassOpen       bool                   `json:"bypass_open"`
	RecoveryRevision uint64                 `json:"recovery_revision"`
	IsolatedSections map[string]string      `json:"isolated_sections,omitempty"`
	Latches          map[string]LatchState  `json:"latches,omitempty"`
	Targets          map[string]TargetState `json:"targets,omitempty"`
	AppliedThrough   uint64                 `json:"applied_through"`
	Events           int                    `json:"events"`
}

// Reconstruction is the state of a partition immediately before and after
// the operation at a given watermark.
type Reconstruction struct {
	Partition string         `json:"partition"`
	Watermark uint64         `json:"watermark"`
	Before    PartitionState `json:"before"`
	After     PartitionState `json:"after"`
	// Operation is the annotated record at the watermark, if it exists.
	Operation *Marked `json:"operation,omitempty"`
	// Missing is true when no record carries the requested watermark.
	Missing bool `json:"missing"`
}

func newPartitionState() PartitionState {
	return PartitionState{
		Phase:            "unknown",
		IsolatedSections: map[string]string{},
		Latches:          map[string]LatchState{},
		Targets:          map[string]TargetState{},
	}
}

// clone deep-copies the mutable maps so captured snapshots are not mutated
// by later events.
func (s PartitionState) clone() PartitionState {
	sections := make(map[string]string, len(s.IsolatedSections))
	for key, value := range s.IsolatedSections {
		sections[key] = value
	}
	latches := make(map[string]LatchState, len(s.Latches))
	for key, value := range s.Latches {
		latches[key] = value
	}
	targets := make(map[string]TargetState, len(s.Targets))
	for key, value := range s.Targets {
		targets[key] = value
	}
	s.IsolatedSections = sections
	s.Latches = latches
	s.Targets = targets
	return s
}

// Rebuild folds the partition's records in watermark order and returns the
// state before and after the operation at watermark. Out-of-order records
// are applied at their own watermark position; gaps simply contribute no
// state change and remain visible via Inspect.
func Rebuild(records []Record, partition string, watermark uint64) (Reconstruction, error) {
	if partition == "" || watermark == 0 {
		return Reconstruction{}, fmt.Errorf("partition and a non-zero watermark are required")
	}
	ordered := make([]Record, 0)
	for _, record := range records {
		if record.Partition == partition {
			ordered = append(ordered, record)
		}
	}
	sort.SliceStable(ordered, func(i, j int) bool {
		if ordered[i].Watermark != ordered[j].Watermark {
			return ordered[i].Watermark < ordered[j].Watermark
		}
		return ordered[i].Seq < ordered[j].Seq
	})
	marked, _ := Inspect(records, Filter{Partition: partition})
	annotations := make(map[uint64]Marked, len(marked))
	for _, entry := range marked {
		annotations[entry.Seq] = entry
	}
	result := Reconstruction{Partition: partition, Watermark: watermark, Missing: true}
	state := newPartitionState()
	for _, record := range ordered {
		if record.Watermark == watermark && result.Missing {
			result.Before = state.clone()
			result.Missing = false
			if entry, ok := annotations[record.Seq]; ok {
				entry := entry
				result.Operation = &entry
			}
		}
		if err := state.apply(record); err != nil {
			return Reconstruction{}, err
		}
		if record.Watermark == watermark {
			result.After = state.clone()
		}
	}
	if result.Missing {
		result.Before = state.clone()
		result.After = state.clone()
	}
	return result, nil
}

func (s *PartitionState) apply(record Record) error {
	switch record.Kind {
	case KindPumpCommand:
		var payload PumpCommandPayload
		if err := json.Unmarshal(record.Payload, &payload); err != nil {
			return fmt.Errorf("decode pump command %s: %w", record.ID, err)
		}
		if payload.Status == "applied" {
			s.PumpStage = payload.Stage
		}
	case KindValveCommand:
		var payload ValveCommandPayload
		if err := json.Unmarshal(record.Payload, &payload); err != nil {
			return fmt.Errorf("decode valve command %s: %w", record.ID, err)
		}
		if payload.Status == "applied" {
			s.BypassOpen = payload.Open
		}
	case KindProtectionLatch:
		var payload LatchPayload
		if err := json.Unmarshal(record.Payload, &payload); err != nil {
			return fmt.Errorf("decode latch %s: %w", record.ID, err)
		}
		s.Latches[payload.StationID] = LatchState{
			Revision: payload.Revision, HighReturn: payload.HighReturn,
			FreezeRisk: payload.FreezeRisk, PressureTrip: payload.PressureTrip,
		}
	case KindIsolation:
		var payload IsolationPayload
		if err := json.Unmarshal(record.Payload, &payload); err != nil {
			return fmt.Errorf("decode isolation %s: %w", record.ID, err)
		}
		s.IsolatedSections[payload.SectionID] = payload.Status
		switch payload.Status {
		case "completed":
			s.Phase = "isolated"
		case "failed":
			s.Phase = "active"
		default:
			s.Phase = "isolating"
		}
	case KindRecovery:
		var payload RecoveryPayload
		if err := json.Unmarshal(record.Payload, &payload); err != nil {
			return fmt.Errorf("decode recovery %s: %w", record.ID, err)
		}
		if payload.Phase != "" && payload.ZoneID != "" {
			s.Phase = payload.Phase
		}
		if payload.Revision > s.RecoveryRevision {
			s.RecoveryRevision = payload.Revision
		}
	case KindTargetChange:
		var payload TargetChangePayload
		if err := json.Unmarshal(record.Payload, &payload); err != nil {
			return fmt.Errorf("decode target change %s: %w", record.ID, err)
		}
		s.Targets[payload.StationID] = TargetState{
			Celsius: payload.Celsius, Revision: payload.Revision, Hold: payload.Hold,
		}
	}
	s.AppliedThrough = record.Watermark
	s.Events++
	return nil
}
