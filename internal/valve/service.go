package valve

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"

	"heatpulse/internal/audit"
	"heatpulse/internal/hydraulic"
	"heatpulse/internal/network"
	"heatpulse/internal/timeline"
)

type Command struct {
	ID        string    `json:"id"`
	ValveID   string    `json:"valve_id"`
	ZoneID    string    `json:"zone_id"`
	Open      bool      `json:"open"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

type Service struct {
	interlock *hydraulic.Interlock
	audit     *audit.Recorder
	timeline  *timeline.Recorder
	mu        sync.RWMutex
	commands  map[string]Command
}

func NewService(interlock *hydraulic.Interlock, recorder *audit.Recorder, timeline *timeline.Recorder) *Service {
	return &Service{interlock: interlock, audit: recorder, timeline: timeline, commands: make(map[string]Command)}
}

func (s *Service) SetBypass(ctx context.Context, zoneID, valveID string, open bool, actor string) (network.Zone, Command, error) {
	if err := ctx.Err(); err != nil {
		return network.Zone{}, Command{}, err
	}
	command := Command{ID: uuid.NewString(), ValveID: valveID, ZoneID: zoneID, Open: open, Status: "pending", CreatedAt: time.Now().UTC()}
	s.store(command)
	before, _ := s.interlock.Snapshot(zoneID)
	zone, err := s.interlock.SetBypass(zoneID, open)
	if err != nil {
		command.Status = "rejected"
		s.store(command)
		_ = s.audit.Rejected("valve.bypass", zoneID, actor, err.Error(), map[string]any{"command_id": command.ID, "open": open})
		_, _ = s.timeline.Record(timeline.KindValveCommand, zoneID, command.ID, "bypass command rejected: "+err.Error(),
			map[string]any{"bypass_open": before.BypassOpen},
			map[string]any{"bypass_open": before.BypassOpen, "command_status": "rejected"})
		return network.Zone{}, command, err
	}
	command.Status = "applied"
	s.store(command)
	if err := s.audit.Accepted("valve.bypass", zoneID, actor, "bypass position applied", map[string]any{"command_id": command.ID, "open": open}); err != nil {
		return network.Zone{}, command, err
	}
	_, _ = s.timeline.Record(timeline.KindValveCommand, zoneID, command.ID, "bypass position applied",
		map[string]any{"bypass_open": before.BypassOpen},
		map[string]any{"bypass_open": zone.BypassOpen, "command_status": "applied"})
	return zone, command, nil
}

func (s *Service) store(command Command) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.commands[command.ID] = command
}

func (s *Service) Commands() []Command {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]Command, 0, len(s.commands))
	for _, command := range s.commands {
		result = append(result, command)
	}
	return result
}
