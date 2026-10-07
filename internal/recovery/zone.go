package recovery

import (
	"fmt"
	"sync"

	"heatpulse/internal/exchanger"
	"heatpulse/internal/network"
	"heatpulse/internal/pump"
	"heatpulse/internal/timeline"
)

type ZoneRecovery struct {
	state     *network.State
	registry  *network.Registry
	warmup    *exchanger.WarmupService
	pressure  *pump.PressureMonitor
	timeline  *timeline.Recorder
	mu        sync.Mutex
	connected map[string]map[string]bool
}

func NewZoneRecovery(state *network.State, registry *network.Registry, warmup *exchanger.WarmupService, pressure *pump.PressureMonitor, timeline *timeline.Recorder) *ZoneRecovery {
	return &ZoneRecovery{state: state, registry: registry, warmup: warmup, pressure: pressure, timeline: timeline, connected: make(map[string]map[string]bool)}
}

func (r *ZoneRecovery) Begin(zoneID string) (network.Zone, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.connected[zoneID] = make(map[string]bool)
	before, _ := r.state.Get(zoneID)
	zone, err := r.state.Update(zoneID, func(zone *network.Zone) error {
		zone.RecoveryRevision++
		zone.Phase = network.PhaseWarming
		return nil
	})
	if err != nil {
		return zone, err
	}
	_, _ = r.timeline.Record(timeline.KindRecovery, zoneID, zoneID, "zone recovery started",
		map[string]any{"phase": string(before.Phase), "recovery_revision": before.RecoveryRevision},
		map[string]any{"phase": string(zone.Phase), "recovery_revision": zone.RecoveryRevision})
	return zone, nil
}

func (r *ZoneRecovery) OnConnected(zoneID, stationID string) (network.Zone, error) {
	r.mu.Lock()
	if r.connected[zoneID] == nil {
		r.connected[zoneID] = make(map[string]bool)
	}
	r.connected[zoneID][stationID] = true
	r.mu.Unlock()
	return r.evaluate(zoneID)
}

func (r *ZoneRecovery) Evaluate(zoneID string) (network.Zone, error) {
	return r.evaluate(zoneID)
}

func (r *ZoneRecovery) evaluate(zoneID string) (network.Zone, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	zone, ok := r.state.Get(zoneID)
	if !ok {
		return network.Zone{}, fmt.Errorf("zone %s not found", zoneID)
	}
	for _, stationID := range r.registry.RequiredForZone(zoneID) {
		if !r.connected[zoneID][stationID] || !r.warmup.Ready(stationID, zone.RecoveryRevision) || !r.pressure.Ready(stationID, zone.RecoveryRevision) {
			return zone, nil
		}
	}
	activated, err := r.state.Activate(zoneID, zone.RecoveryRevision)
	if err != nil {
		return activated, err
	}
	_, _ = r.timeline.Record(timeline.KindRecovery, zoneID, zoneID, "zone recovered to active",
		map[string]any{"phase": string(zone.Phase), "recovery_revision": zone.RecoveryRevision},
		map[string]any{"phase": string(activated.Phase), "recovery_revision": activated.RecoveryRevision})
	return activated, nil
}

func (r *ZoneRecovery) Readiness(zoneID string) map[string]map[string]bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	zone, _ := r.state.Get(zoneID)
	result := make(map[string]map[string]bool)
	for _, stationID := range r.registry.RequiredForZone(zoneID) {
		result[stationID] = map[string]bool{
			"connected":   r.connected[zoneID][stationID],
			"warm":        r.warmup.Ready(stationID, zone.RecoveryRevision),
			"pressurized": r.pressure.Ready(stationID, zone.RecoveryRevision),
		}
	}
	return result
}
