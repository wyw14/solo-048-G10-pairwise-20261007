package api

import (
	"errors"
	"net/http"
	"strconv"

	"heatpulse/internal/timeline"
)

type timelineResponse struct {
	Records      []timeline.Marked          `json:"records"`
	Partitions   []timeline.PartitionHealth `json:"partitions"`
	DroppedLines int                        `json:"dropped_lines,omitempty"`
}

// listTimeline returns the cross-component operations timeline with
// integrity marks. Query params: partition, kind, limit.
func (a *App) listTimeline(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	filter := timeline.Filter{Partition: query.Get("partition"), Kind: timeline.Kind(query.Get("kind"))}
	marked, health := timeline.Inspect(a.TimelineStore.List(), filter)
	if limit, err := strconv.Atoi(query.Get("limit")); err == nil && limit > 0 && len(marked) > limit {
		marked = marked[len(marked)-limit:]
	}
	writeJSON(w, http.StatusOK, timelineResponse{Records: marked, Partitions: health, DroppedLines: a.TimelineStore.DroppedLines()})
}

// rebuildTimeline reconstructs a partition's state immediately before and
// after the operation at the given watermark.
func (a *App) rebuildTimeline(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	watermark, err := strconv.ParseUint(query.Get("watermark"), 10, 64)
	if err != nil || watermark == 0 {
		writeError(w, http.StatusBadRequest, errInvalidWatermark)
		return
	}
	reconstruction, err := timeline.Rebuild(a.TimelineStore.List(), query.Get("partition"), watermark)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, reconstruction)
}

var errInvalidWatermark = errors.New("a positive integer watermark is required")
