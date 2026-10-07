package timeline

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Store is an append-only log of timeline records. Each record is written
// once with the next monotonically increasing watermark and is never
// modified or removed afterwards — the store deliberately exposes no update
// or delete operations, so the original event text cannot be overwritten.
type Store struct {
	path string
	mu   sync.Mutex
	next uint64
}

// OpenStore opens the log at path, creating it on first append, and resumes
// watermark assignment after the highest watermark already stored.
func OpenStore(path string) (*Store, error) {
	store := &Store{path: path, next: 1}
	records, err := store.readAll()
	if err != nil {
		return nil, err
	}
	for _, record := range records {
		if record.Seq >= store.next {
			store.next = record.Seq + 1
		}
	}
	return store, nil
}

// Append assigns the next watermark, persists the record and returns the
// stored copy. Any flags on the input are cleared: flags are derived at
// read time and never persisted.
func (s *Store) Append(record Record) (Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record.Seq = s.next
	record.Flags = nil
	if record.At.IsZero() {
		record.At = time.Now().UTC()
	}
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
	s.next++
	return record, nil
}

// List returns records in watermark order, annotated with integrity flags.
// A positive limit keeps the newest records while preserving order.
func (s *Store) List(limit int) ([]Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	records, err := s.readAll()
	if err != nil {
		return nil, err
	}
	annotate(records)
	if limit > 0 && len(records) > limit {
		records = records[len(records)-limit:]
	}
	return records, nil
}

func (s *Store) readAll() ([]Record, error) {
	file, err := os.Open(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return []Record{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("open timeline log: %w", err)
	}
	defer file.Close()
	records := make([]Record, 0)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var record Record
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			return nil, fmt.Errorf("decode timeline record: %w", err)
		}
		records = append(records, record)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan timeline log: %w", err)
	}
	return records, nil
}

// annotate derives integrity flags in place. A record is flagged
// FlagSequenceGap when its watermark does not continue the previous one
// (records in between are missing) and FlagOutOfOrder when its timestamp
// predates the previous record.
func annotate(records []Record) {
	var previous *Record
	for i := range records {
		record := &records[i]
		record.Flags = nil
		if previous == nil {
			if record.Seq != 1 {
				record.Flags = append(record.Flags, FlagSequenceGap)
			}
		} else {
			if record.Seq != previous.Seq+1 {
				record.Flags = append(record.Flags, FlagSequenceGap)
			}
			if record.At.Before(previous.At) {
				record.Flags = append(record.Flags, FlagOutOfOrder)
			}
		}
		previous = record
	}
}
