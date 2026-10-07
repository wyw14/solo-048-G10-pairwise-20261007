package timeline

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Recorder assigns per-partition event watermarks and appends immutable
// records. A nil *Recorder is valid and silently drops events, so services
// can record unconditionally.
type Recorder struct {
	store *Store

	mu          sync.Mutex
	stationZone func(stationID string) string
}

func NewRecorder(store *Store) *Recorder {
	return &Recorder{store: store}
}

// SetStationResolver teaches the recorder which partition (zone) a station
// belongs to, so station-scoped events join the zone timeline.
func (r *Recorder) SetStationResolver(resolve func(stationID string) string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stationZone = resolve
}

// Record appends an event to the partition timeline with the next watermark.
func (r *Recorder) Record(kind Kind, partition, entityID, summary string, payload any) (Record, error) {
	if r == nil || r.store == nil {
		return Record{}, nil
	}
	if partition == "" {
		return Record{}, fmt.Errorf("timeline partition is required")
	}
	raw := json.RawMessage(`{}`)
	if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return Record{}, fmt.Errorf("encode timeline payload: %w", err)
		}
		raw = encoded
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	watermark := r.store.NextWatermark(partition)
	return r.store.Append(Record{
		ID: uuid.NewString(), Partition: partition, Watermark: watermark,
		Kind: kind, EntityID: entityID, Summary: summary, Payload: raw,
		At: time.Now().UTC(),
	})
}

// RecordStation resolves the station's zone and records the event on that
// partition; unresolved stations fall back to an explicit station partition
// so the event is never silently dropped.
func (r *Recorder) RecordStation(kind Kind, stationID, entityID, summary string, payload any) (Record, error) {
	if r == nil || r.store == nil {
		return Record{}, nil
	}
	r.mu.Lock()
	resolve := r.stationZone
	r.mu.Unlock()
	partition := ""
	if resolve != nil {
		partition = resolve(stationID)
	}
	if partition == "" {
		partition = "station:" + stationID
	}
	return r.Record(kind, partition, entityID, summary, payload)
}
