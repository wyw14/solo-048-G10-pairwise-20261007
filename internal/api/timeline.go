package api

import (
	"errors"
	"net/http"
	"strconv"

	"heatpulse/internal/timeline"
)

func (a *App) listTimeline(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	limit, _ := strconv.Atoi(query.Get("limit"))
	records, err := a.Timeline.List(0)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	zoneID := query.Get("zone_id")
	kind := timeline.Kind(query.Get("kind"))
	filtered := make([]timeline.Record, 0, len(records))
	for _, record := range records {
		if zoneID != "" && record.ZoneID != zoneID {
			continue
		}
		if kind != "" && record.Kind != kind {
			continue
		}
		filtered = append(filtered, record)
	}
	if limit > 0 && len(filtered) > limit {
		filtered = filtered[len(filtered)-limit:]
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"records":   filtered,
		"integrity": timeline.Summarize(filtered),
	})
}

func (a *App) rebuildTimeline(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	zoneID := query.Get("zone_id")
	if zoneID == "" {
		writeError(w, http.StatusBadRequest, errors.New("zone_id is required"))
		return
	}
	var atSeq uint64
	if raw := query.Get("at"); raw != "" {
		parsed, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, errors.New("at must be a timeline watermark"))
			return
		}
		atSeq = parsed
	}
	result, err := a.Timeline.Rebuild(zoneID, atSeq)
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}
