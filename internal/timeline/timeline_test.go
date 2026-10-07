package timeline

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func openTempStore(t *testing.T) (*Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "timeline.jsonl")
	store, err := OpenStore(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	return store, path
}

func TestAppendAssignsWatermarksAndPersists(t *testing.T) {
	store, path := openTempStore(t)
	first, err := store.Append(Record{Kind: KindPumpCommand, ZoneID: "east", EntityID: "cmd-1"})
	if err != nil {
		t.Fatalf("append first: %v", err)
	}
	second, err := store.Append(Record{Kind: KindValveCommand, ZoneID: "east", EntityID: "cmd-2"})
	if err != nil {
		t.Fatalf("append second: %v", err)
	}
	if first.Seq != 1 || second.Seq != 2 {
		t.Fatalf("expected watermarks 1,2 got %d,%d", first.Seq, second.Seq)
	}
	if first.At.IsZero() {
		t.Fatal("expected append to stamp the record time")
	}

	reopened, err := OpenStore(path)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	third, err := reopened.Append(Record{Kind: KindRecovery, ZoneID: "east", EntityID: "east"})
	if err != nil {
		t.Fatalf("append after reopen: %v", err)
	}
	if third.Seq != 3 {
		t.Fatalf("expected watermark 3 after reopen, got %d", third.Seq)
	}
}

func TestListFlagsMissingAndOutOfOrderRecords(t *testing.T) {
	_, path := openTempStore(t)
	base := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	content := `{"seq":2,"kind":"pump.command","entity_id":"cmd-1","message":"kept","at":"` + base.Format(time.RFC3339Nano) + `"}` + "\n" +
		`{"seq":4,"kind":"valve.command","entity_id":"cmd-2","message":"kept","at":"` + base.Add(time.Minute).Format(time.RFC3339Nano) + `"}` + "\n" +
		`{"seq":5,"kind":"recovery.transition","entity_id":"east","message":"kept","at":"` + base.Add(-time.Minute).Format(time.RFC3339Nano) + `"}` + "\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("seed log: %v", err)
	}
	store, err := OpenStore(path)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	records, err := store.List(0)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(records) != 3 {
		t.Fatalf("expected 3 records, got %d", len(records))
	}
	if len(records[0].Flags) != 1 || records[0].Flags[0] != FlagSequenceGap {
		t.Fatalf("expected first record flagged for the missing watermark 1, got %v", records[0].Flags)
	}
	if len(records[1].Flags) != 1 || records[1].Flags[0] != FlagSequenceGap {
		t.Fatalf("expected gap flag after missing watermark 3, got %v", records[1].Flags)
	}
	if len(records[2].Flags) != 1 || records[2].Flags[0] != FlagOutOfOrder {
		t.Fatalf("expected out-of-order flag on regressed timestamp, got %v", records[2].Flags)
	}
	summary := Summarize(records)
	if summary.Gaps != 2 || summary.OutOfOrder != 1 {
		t.Fatalf("unexpected integrity summary: %+v", summary)
	}

	// The stored originals must remain untouched by flag annotation.
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	if string(raw) != content {
		t.Fatal("stored records were modified by read-time annotation")
	}
}

func TestRebuildFoldsZoneStateAroundOperation(t *testing.T) {
	store, _ := openTempStore(t)
	if _, err := store.Append(Record{Kind: KindPumpCommand, ZoneID: "east", EntityID: "cmd-1",
		Before: map[string]any{"pump_stage": 2}, After: map[string]any{"pump_stage": 3}}); err != nil {
		t.Fatalf("append pump: %v", err)
	}
	if _, err := store.Append(Record{Kind: KindProtectionLatch, ZoneID: "east", EntityID: "binhe",
		After: map[string]any{"latch_freeze_risk": true}}); err != nil {
		t.Fatalf("append latch: %v", err)
	}
	if _, err := store.Append(Record{Kind: KindPumpCommand, ZoneID: "west", EntityID: "cmd-9",
		After: map[string]any{"pump_stage": 4}}); err != nil {
		t.Fatalf("append other zone: %v", err)
	}

	result, err := store.Rebuild("east", 2)
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if result.At == nil || result.At.Seq != 2 {
		t.Fatalf("expected operation at watermark 2, got %+v", result.At)
	}
	if result.Before["pump_stage"] != float64(3) {
		t.Fatalf("expected before pump_stage 3, got %v", result.Before["pump_stage"])
	}
	if _, ok := result.Before["latch_freeze_risk"]; ok {
		t.Fatal("before state must not include the latch applied at watermark 2")
	}
	if result.After["latch_freeze_risk"] != true || result.After["pump_stage"] != float64(3) {
		t.Fatalf("unexpected after state: %v", result.After)
	}
	if result.Incomplete {
		t.Fatal("clean log must not be marked incomplete")
	}

	latest, err := store.Rebuild("east", 0)
	if err != nil {
		t.Fatalf("rebuild latest: %v", err)
	}
	if latest.At == nil || latest.At.Seq != 2 {
		t.Fatalf("expected newest east record at watermark 2, got %+v", latest.At)
	}
	if latest.After["pump_stage"] != float64(3) {
		t.Fatalf("other-zone records must not leak into east reconstruction, got %v", latest.After["pump_stage"])
	}

	if _, err := store.Rebuild("east", 9); err == nil {
		t.Fatal("expected error for unknown watermark")
	}
}
