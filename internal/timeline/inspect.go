package timeline

import "sort"

// MarkOutOfOrder flags a record whose watermark does not exceed the highest
// watermark already seen for its partition (duplicate or late arrival).
const MarkOutOfOrder = "out_of_order"

// Marked is a record annotated with integrity findings. Marks are computed
// at read time; the stored originals are never altered.
type Marked struct {
	Record
	Marks []string `json:"marks,omitempty"`
	// MissingBefore lists partition watermarks that never appeared before
	// this record — explicitly surfaced gaps in the event stream.
	MissingBefore []uint64 `json:"missing_before,omitempty"`
}

// PartitionHealth summarizes the integrity of one partition's watermark chain.
type PartitionHealth struct {
	Partition    string   `json:"partition"`
	Records      int      `json:"records"`
	MinWatermark uint64   `json:"min_watermark"`
	MaxWatermark uint64   `json:"max_watermark"`
	Missing      []uint64 `json:"missing,omitempty"`
	OutOfOrder   int      `json:"out_of_order"`
}

// Filter narrows which records Inspect returns. Health is always computed
// over the full partition so filtering never hides a gap.
type Filter struct {
	Partition string
	Kind      Kind
}

func (f Filter) match(record Record) bool {
	if f.Partition != "" && record.Partition != f.Partition {
		return false
	}
	if f.Kind != "" && record.Kind != f.Kind {
		return false
	}
	return true
}

// Inspect walks records in append order and flags watermark gaps and
// out-of-order arrivals per partition.
func Inspect(records []Record, filter Filter) ([]Marked, []PartitionHealth) {
	type tracker struct {
		maxSeen uint64
		health  PartitionHealth
	}
	trackers := make(map[string]*tracker)
	marked := make([]Marked, 0, len(records))
	for _, record := range records {
		state := trackers[record.Partition]
		if state == nil {
			state = &tracker{health: PartitionHealth{Partition: record.Partition, MinWatermark: record.Watermark, MaxWatermark: record.Watermark}}
			trackers[record.Partition] = state
		}
		entry := Marked{Record: record}
		if record.Watermark <= state.maxSeen {
			entry.Marks = append(entry.Marks, MarkOutOfOrder)
			state.health.OutOfOrder++
		} else {
			for watermark := state.maxSeen + 1; watermark < record.Watermark; watermark++ {
				entry.MissingBefore = append(entry.MissingBefore, watermark)
				state.health.Missing = append(state.health.Missing, watermark)
			}
			state.maxSeen = record.Watermark
		}
		state.health.Records++
		if record.Watermark < state.health.MinWatermark {
			state.health.MinWatermark = record.Watermark
		}
		if record.Watermark > state.health.MaxWatermark {
			state.health.MaxWatermark = record.Watermark
		}
		if filter.match(record) {
			marked = append(marked, entry)
		}
	}
	health := make([]PartitionHealth, 0, len(trackers))
	for _, state := range trackers {
		health = append(health, state.health)
	}
	sort.Slice(health, func(i, j int) bool { return health[i].Partition < health[j].Partition })
	return marked, health
}
