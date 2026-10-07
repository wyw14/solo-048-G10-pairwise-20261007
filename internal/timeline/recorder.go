package timeline

// Recorder emits operations records into the append-only store. It resolves
// the zone for station-scoped events so every record carries its partition.
type Recorder struct {
	store         *Store
	zoneOfStation func(string) string
}

func NewRecorder(store *Store, zoneOfStation func(string) string) *Recorder {
	return &Recorder{store: store, zoneOfStation: zoneOfStation}
}

// Record appends one timeline record. before and after hold the zone-state
// fragments observed around the operation; either may be nil.
func (r *Recorder) Record(kind Kind, zoneID, entityID, message string, before, after map[string]any) (Record, error) {
	return r.store.Append(Record{
		Kind: kind, ZoneID: zoneID, EntityID: entityID, Message: message,
		Before: before, After: after,
	})
}

// RecordForStation resolves the station's zone and appends the record.
func (r *Recorder) RecordForStation(kind Kind, stationID, message string, before, after map[string]any) (Record, error) {
	zoneID := ""
	if r.zoneOfStation != nil {
		zoneID = r.zoneOfStation(stationID)
	}
	return r.Record(kind, zoneID, stationID, message, before, after)
}
