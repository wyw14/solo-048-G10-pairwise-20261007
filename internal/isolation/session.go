package isolation

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"heatpulse/internal/audit"
	"heatpulse/internal/network"
	"heatpulse/internal/timeline"
	"heatpulse/internal/valve"
)

type Status string

const (
	StatusOpeningRelief   Status = "opening_relief"
	StatusClosingBoundary Status = "closing_boundaries"
	StatusCompleted       Status = "completed"
	StatusFailed          Status = "failed"
)

type Session struct {
	ID          string                `json:"id"`
	SectionID   string                `json:"section_id"`
	ZoneID      string                `json:"zone_id"`
	Status      Status                `json:"status"`
	Relief      valve.ReliefProof     `json:"relief"`
	Boundaries  []valve.BoundaryProof `json:"boundaries"`
	Error       string                `json:"error,omitempty"`
	StartedAt   time.Time             `json:"started_at"`
	CompletedAt time.Time             `json:"completed_at,omitempty"`
}

type Service struct {
	topology   *network.Topology
	state      *network.State
	relief     *valve.ReliefService
	boundaries *valve.BoundaryService
	audit      *audit.Recorder
	timeline   *timeline.Recorder
	mu         sync.RWMutex
	sessions   map[string]Session
}

func NewService(topology *network.Topology, state *network.State, relief *valve.ReliefService, boundaries *valve.BoundaryService, recorder *audit.Recorder, timelineRecorder *timeline.Recorder) *Service {
	return &Service{topology: topology, state: state, relief: relief, boundaries: boundaries, audit: recorder, timeline: timelineRecorder, sessions: make(map[string]Session)}
}

func (s *Service) Isolate(ctx context.Context, sectionID, actor string) (Session, error) {
	section, err := s.topology.Section(sectionID)
	if err != nil {
		return Session{}, err
	}
	session := Session{
		ID: uuid.NewString(), SectionID: sectionID, ZoneID: section.ZoneID,
		Status: StatusOpeningRelief, StartedAt: time.Now().UTC(), Boundaries: make([]valve.BoundaryProof, 0, len(section.BoundaryValves)),
	}
	s.save(session)
	s.record(session)
	if _, err := s.state.Update(section.ZoneID, func(zone *network.Zone) error {
		zone.Phase = network.PhaseIsolating
		return nil
	}); err != nil {
		return s.fail(session, err)
	}
	proof, err := s.relief.Open(ctx, section.ReliefValve, session.ID)
	if err != nil {
		return s.fail(session, err)
	}
	session.Relief = proof
	session.Status = StatusClosingBoundary
	s.save(session)
	for _, valveID := range section.BoundaryValves {
		boundary, err := s.boundaries.Close(ctx, valveID, session.ID, proof)
		if err != nil {
			return s.fail(session, err)
		}
		session.Boundaries = append(session.Boundaries, boundary)
		s.save(session)
	}
	if _, err := s.state.Update(section.ZoneID, func(zone *network.Zone) error {
		zone.Phase = network.PhaseIsolated
		return nil
	}); err != nil {
		return s.fail(session, err)
	}
	session.Status = StatusCompleted
	session.CompletedAt = time.Now().UTC()
	s.save(session)
	s.record(session)
	if err := s.audit.Accepted("isolation.complete", sectionID, actor, "section safely isolated", map[string]any{"session_id": session.ID}); err != nil {
		return session, err
	}
	return session, nil
}

func (s *Service) fail(session Session, cause error) (Session, error) {
	session.Status = StatusFailed
	session.Error = cause.Error()
	session.CompletedAt = time.Now().UTC()
	s.save(session)
	s.record(session)
	_ = s.audit.Rejected("isolation.failed", session.SectionID, "system", cause.Error(), map[string]any{"session_id": session.ID})
	return session, fmt.Errorf("isolation %s: %w", session.ID, cause)
}

func (s *Service) record(session Session) {
	_, _ = s.timeline.Record(timeline.KindIsolation, session.ZoneID, session.ID,
		fmt.Sprintf("section %s isolation %s", session.SectionID, session.Status),
		timeline.IsolationPayload{
			SessionID: session.ID, SectionID: session.SectionID, ZoneID: session.ZoneID,
			Status: string(session.Status), Error: session.Error,
		})
}

func (s *Service) save(session Session) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessions[session.ID] = session
}

func (s *Service) Get(sessionID string) (Session, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	session, ok := s.sessions[sessionID]
	session.Boundaries = append([]valve.BoundaryProof(nil), session.Boundaries...)
	return session, ok
}

func (s *Service) List() []Session {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]Session, 0, len(s.sessions))
	for _, session := range s.sessions {
		session.Boundaries = append([]valve.BoundaryProof(nil), session.Boundaries...)
		result = append(result, session)
	}
	return result
}
