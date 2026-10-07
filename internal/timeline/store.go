package timeline

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Store is an append-only log of timeline records. It exposes no update or
// delete operations: event originals cannot be overwritten. Corrupt lines
// encountered while loading are skipped and counted so the log stays
// readable; the count is surfaced as an integrity anomaly.
type Store struct {
	path string

	mu           sync.RWMutex
	records      []Record
	seq          uint64
	maxWatermark map[string]uint64
	droppedLines int
}

func OpenStore(path string) (*Store, error) {
	s := &Store{path: path, maxWatermark: make(map[string]uint64)}
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return s, nil
	}
	if err != nil {
		return nil, fmt.Errorf("open timeline log: %w", err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 1<<20), 4<<20)
	for scanner.Scan() {
		var record Record
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			s.droppedLines++
			continue
		}
		s.index(record)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan timeline log: %w", err)
	}
	return s, nil
}

func (s *Store) index(record Record) {
	s.records = append(s.records, record)
	if record.Seq > s.seq {
		s.seq = record.Seq
	}
	if record.Watermark > s.maxWatermark[record.Partition] {
		s.maxWatermark[record.Partition] = record.Watermark
	}
}

// Append persists the record and returns it with its global sequence number
// assigned. The caller sets the watermark (see Recorder); the store never
// rewrites an existing line.
func (s *Store) Append(record Record) (Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	record.Seq = s.seq
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return Record{}, fmt.Errorf("create timeline directory: %w", err)
	}
	file, err := os.OpenFile(s.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return Record{}, fmt.Errorf("open timeline log: %w", err)
	}
	defer file.Close()
	if err := json.NewEncoder(file).Encode(record); err != nil {
		return Record{}, fmt.Errorf("append timeline record: %w", err)
	}
	if err := file.Sync(); err != nil {
		return Record{}, fmt.Errorf("sync timeline log: %w", err)
	}
	s.index(record)
	return record, nil
}

// NextWatermark returns the next per-partition watermark derived from the
// highest watermark ever appended (including across restarts).
func (s *Store) NextWatermark(partition string) uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.maxWatermark[partition] + 1
}

// List returns deep copies of every record in append order.
func (s *Store) List() []Record {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]Record, 0, len(s.records))
	for _, record := range s.records {
		result = append(result, record.Clone())
	}
	return result
}

// DroppedLines reports how many undecodable lines were skipped at load.
func (s *Store) DroppedLines() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.droppedLines
}
