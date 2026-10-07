package valve

import (
	"context"
	"fmt"
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

func NewService(interlock *hydraulic.Interlock, recorder *audit.Recorder, timelineRecorder *timeline.Recorder) *Service {
	return &Service{interlock: interlock, audit: recorder, timeline: timelineRecorder, commands: make(map[string]Command)}
}

func (s *Service) SetBypass(ctx context.Context, zoneID, valveID string, open bool, actor string) (network.Zone, Command, error) {
	if err := ctx.Err(); err != nil {
		return network.Zone{}, Command{}, err
	}
	command := Command{ID: uuid.NewString(), ValveID: valveID, ZoneID: zoneID, Open: open, Status: "pending", CreatedAt: time.Now().UTC()}
	s.store(command)
	zone, err := s.interlock.SetBypass(zoneID, open)
	if err != nil {
		command.Status = "rejected"
		s.store(command)
		s.record(command)
		_ = s.audit.Rejected("valve.bypass", zoneID, actor, err.Error(), map[string]any{"command_id": command.ID, "open": open})
		return network.Zone{}, command, err
	}
	command.Status = "applied"
	s.store(command)
	s.record(command)
	if err := s.audit.Accepted("valve.bypass", zoneID, actor, "bypass position applied", map[string]any{"command_id": command.ID, "open": open}); err != nil {
		return network.Zone{}, command, err
	}
	return zone, command, nil
}

func (s *Service) record(command Command) {
	position := "close"
	if command.Open {
		position = "open"
	}
	_, _ = s.timeline.Record(timeline.KindValveCommand, command.ZoneID, command.ID,
		fmt.Sprintf("bypass %s %s", position, command.Status),
		timeline.ValveCommandPayload{CommandID: command.ID, ValveID: command.ValveID, ZoneID: command.ZoneID, Open: command.Open, Status: command.Status})
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
