package pump

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
	ZoneID    string    `json:"zone_id"`
	Stage     int       `json:"stage"`
	Status    string    `json:"status"`
	IssuedAt  time.Time `json:"issued_at"`
	AppliedAt time.Time `json:"applied_at,omitempty"`
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

func (s *Service) SetStage(ctx context.Context, zoneID string, stage int, actor string) (network.Zone, Command, error) {
	if err := ctx.Err(); err != nil {
		return network.Zone{}, Command{}, err
	}
	command := Command{ID: uuid.NewString(), ZoneID: zoneID, Stage: stage, Status: "pending", IssuedAt: time.Now().UTC()}
	s.mu.Lock()
	s.commands[command.ID] = command
	s.mu.Unlock()
	before, _ := s.interlock.Snapshot(zoneID)
	zone, err := s.interlock.StagePump(zoneID, stage)
	if err != nil {
		command.Status = "rejected"
		s.save(command)
		_ = s.audit.Rejected("pump.stage", zoneID, actor, err.Error(), map[string]any{"command_id": command.ID, "stage": stage})
		_, _ = s.timeline.Record(timeline.KindPumpCommand, zoneID, command.ID, "pump stage rejected: "+err.Error(),
			map[string]any{"pump_stage": before.PumpStage},
			map[string]any{"pump_stage": before.PumpStage, "command_status": "rejected"})
		return network.Zone{}, command, err
	}
	command.Status = "applied"
	command.AppliedAt = time.Now().UTC()
	s.save(command)
	if err := s.audit.Accepted("pump.stage", zoneID, actor, "pump stage applied", map[string]any{"command_id": command.ID, "stage": stage}); err != nil {
		return network.Zone{}, command, fmt.Errorf("record pump command: %w", err)
	}
	_, _ = s.timeline.Record(timeline.KindPumpCommand, zoneID, command.ID, "pump stage applied",
		map[string]any{"pump_stage": before.PumpStage},
		map[string]any{"pump_stage": zone.PumpStage, "command_status": "applied"})
	return zone, command, nil
}

func (s *Service) save(command Command) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.commands[command.ID] = command
}

func (s *Service) Command(commandID string) (Command, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	command, ok := s.commands[commandID]
	return command, ok
}

func (s *Service) CommandsForZone(zoneID string) []Command {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]Command, 0)
	for _, command := range s.commands {
		if command.ZoneID == zoneID {
			result = append(result, command)
		}
	}
	return result
}
