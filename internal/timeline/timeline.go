package timeline

import "time"

// Kind identifies the component family an operations record belongs to.
type Kind string

const (
	KindPumpCommand     Kind = "pump.command"
	KindValveCommand    Kind = "valve.command"
	KindProtectionLatch Kind = "protection.latch"
	KindIsolation       Kind = "isolation.session"
	KindRecovery        Kind = "recovery.transition"
	KindTargetChange    Kind = "station.target"
)

// Flag marks an integrity problem derived while reading the log. Flags are
// annotations computed at read time; they are never written back into the
// stored record, so the original event text stays untouched.
type Flag string

const (
	// FlagSequenceGap marks a record whose watermark does not immediately
	// follow the previous one: intermediate records are missing.
	FlagSequenceGap Flag = "sequence_gap"
	// FlagOutOfOrder marks a record whose timestamp predates the timestamp
	// of the previous record on the timeline.
	FlagOutOfOrder Flag = "out_of_order"
)

// Record is one immutable entry on the cross-component operations timeline.
// Before and After hold the zone-state fragments observed around the
// operation, which is what per-zone reconstruction folds over.
type Record struct {
	Seq      uint64         `json:"seq"`
	Kind     Kind           `json:"kind"`
	ZoneID   string         `json:"zone_id,omitempty"`
	EntityID string         `json:"entity_id"`
	Message  string         `json:"message"`
	At       time.Time      `json:"at"`
	Before   map[string]any `json:"before,omitempty"`
	After    map[string]any `json:"after,omitempty"`
	Flags    []Flag         `json:"flags,omitempty"`
}

// Integrity aggregates the derived flags of a record set.
type Integrity struct {
	Records    int `json:"records"`
	Gaps       int `json:"gaps"`
	OutOfOrder int `json:"out_of_order"`
}

// Summarize counts the flagged records in an already-annotated record set.
func Summarize(records []Record) Integrity {
	summary := Integrity{Records: len(records)}
	for _, record := range records {
		for _, flag := range record.Flags {
			switch flag {
			case FlagSequenceGap:
				summary.Gaps++
			case FlagOutOfOrder:
				summary.OutOfOrder++
			}
		}
	}
	return summary
}
