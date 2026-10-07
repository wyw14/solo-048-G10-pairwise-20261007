package timeline

import "fmt"

// Reconstruction is the zone state rebuilt around one operation on the
// timeline. Before folds every record of the zone with a watermark below
// At.Seq; After additionally applies the operation itself. Incomplete is
// set when any folded record carries an integrity flag, meaning the
// reconstruction may be missing events.
type Reconstruction struct {
	ZoneID     string         `json:"zone_id"`
	At         *Record        `json:"at,omitempty"`
	Before     map[string]any `json:"before"`
	After      map[string]any `json:"after"`
	Incomplete bool           `json:"incomplete"`
	Issues     []Flag         `json:"issues,omitempty"`
}

// Rebuild folds the zone's timeline records up to the watermark atSeq and
// returns the reconstructed state immediately before and after that
// operation. An atSeq of zero targets the newest record of the zone.
func (s *Store) Rebuild(zoneID string, atSeq uint64) (Reconstruction, error) {
	records, err := s.List(0)
	if err != nil {
		return Reconstruction{}, err
	}
	result := Reconstruction{ZoneID: zoneID, Before: map[string]any{}, After: map[string]any{}}
	issues := make(map[Flag]bool)
	var target *Record
	for i := range records {
		record := records[i]
		if record.ZoneID != zoneID {
			continue
		}
		if atSeq != 0 && record.Seq > atSeq {
			break
		}
		for _, flag := range record.Flags {
			issues[flag] = true
		}
		if atSeq == 0 || record.Seq < atSeq {
			mergeState(result.Before, record.After)
		}
		mergeState(result.After, record.After)
		if atSeq == 0 || record.Seq == atSeq {
			copy := record
			target = &copy
		}
	}
	if target == nil {
		return Reconstruction{}, fmt.Errorf("no timeline record for zone %s at watermark %d", zoneID, atSeq)
	}
	result.At = target
	result.Incomplete = len(issues) > 0
	for flag := range issues {
		result.Issues = append(result.Issues, flag)
	}
	return result, nil
}

func mergeState(state, fragment map[string]any) {
	for key, value := range fragment {
		state[key] = value
	}
}
