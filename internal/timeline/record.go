package timeline

import (
	"encoding/json"
	"time"
)

// Kind enumerates the cross-component operations linked by the timeline.
type Kind string

const (
	KindPumpCommand     Kind = "pump_command"
	KindValveCommand    Kind = "valve_command"
	KindProtectionLatch Kind = "protection_latch"
	KindIsolation       Kind = "isolation"
	KindRecovery        Kind = "recovery"
	KindTargetChange    Kind = "target_change"
)

// Record is one immutable entry on the operations timeline. Payload holds the
// event original verbatim; once appended it is never overwritten.
type Record struct {
	ID        string          `json:"id"`
	Seq       uint64          `json:"seq"`       // global append order, assigned by the store
	Partition string          `json:"partition"` // zone id; station events resolve to their zone
	Watermark uint64          `json:"watermark"` // per-partition event watermark
	Kind      Kind            `json:"kind"`
	EntityID  string          `json:"entity_id"`
	Summary   string          `json:"summary"`
	Payload   json.RawMessage `json:"payload"`
	At        time.Time       `json:"at"`
}

// Clone returns a deep copy so callers can never mutate the stored original.
func (r Record) Clone() Record {
	r.Payload = append(json.RawMessage(nil), r.Payload...)
	return r
}

type PumpCommandPayload struct {
	CommandID string `json:"command_id"`
	ZoneID    string `json:"zone_id"`
	Stage     int    `json:"stage"`
	Status    string `json:"status"`
}

type ValveCommandPayload struct {
	CommandID string `json:"command_id"`
	ValveID   string `json:"valve_id"`
	ZoneID    string `json:"zone_id"`
	Open      bool   `json:"open"`
	Status    string `json:"status"`
}

type LatchPayload struct {
	StationID    string `json:"station_id"`
	Revision     uint64 `json:"revision"`
	HighReturn   bool   `json:"high_return"`
	FreezeRisk   bool   `json:"freeze_risk"`
	PressureTrip bool   `json:"pressure_trip"`
}

type IsolationPayload struct {
	SessionID string `json:"session_id"`
	SectionID string `json:"section_id"`
	ZoneID    string `json:"zone_id"`
	Status    string `json:"status"`
	Error     string `json:"error,omitempty"`
}

type RecoveryPayload struct {
	ZoneID    string `json:"zone_id"`
	StationID string `json:"station_id,omitempty"`
	Phase     string `json:"phase"`
	Revision  uint64 `json:"revision"`
	Detail    string `json:"detail,omitempty"`
}

type TargetChangePayload struct {
	StationID string  `json:"station_id"`
	Celsius   float64 `json:"celsius"`
	Revision  uint64  `json:"revision"`
	Hold      bool    `json:"hold"`
	Source    string  `json:"source"`
}
