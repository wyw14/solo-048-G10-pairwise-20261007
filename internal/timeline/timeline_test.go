package timeline

import (
	"encoding/json"
	"path/filepath"
	"testing"
)

func openTestStore(t *testing.T) (*Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "timeline.jsonl")
	store, err := OpenStore(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	return store, path
}

func TestRecorderAssignsWatermarksPerPartition(t *testing.T) {
	store, _ := openTestStore(t)
	recorder := NewRecorder(store)
	first, err := recorder.Record(KindPumpCommand, "north", "cmd-1", "stage up", PumpCommandPayload{CommandID: "cmd-1", ZoneID: "north", Stage: 3, Status: "applied"})
	if err != nil {
		t.Fatalf("record: %v", err)
	}
	second, err := recorder.Record(KindValveCommand, "north", "cmd-2", "bypass closed", ValveCommandPayload{CommandID: "cmd-2", ValveID: "v-1", ZoneID: "north", Open: false, Status: "applied"})
	if err != nil {
		t.Fatalf("record: %v", err)
	}
	other, err := recorder.Record(KindPumpCommand, "east", "cmd-3", "stage up", PumpCommandPayload{CommandID: "cmd-3", ZoneID: "east", Stage: 2, Status: "applied"})
	if err != nil {
		t.Fatalf("record: %v", err)
	}
	if first.Watermark != 1 || second.Watermark != 2 {
		t.Fatalf("north watermarks = %d, %d; want 1, 2", first.Watermark, second.Watermark)
	}
	if other.Watermark != 1 {
		t.Fatalf("east watermark = %d; want independent sequence starting at 1", other.Watermark)
	}
	if first.Seq == second.Seq || second.Seq == other.Seq {
		t.Fatalf("global sequence numbers must be unique: %d, %d, %d", first.Seq, second.Seq, other.Seq)
	}
	var payload PumpCommandPayload
	if err := json.Unmarshal(first.Payload, &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if payload.Stage != 3 || payload.Status != "applied" {
		t.Fatalf("payload = %+v; want stage 3 applied", payload)
	}
}

func TestStoreReloadPreservesOriginalsAndWatermark(t *testing.T) {
	store, path := openTestStore(t)
	recorder := NewRecorder(store)
	original, err := recorder.Record(KindProtectionLatch, "west", "nanhu", "latches applied", LatchPayload{StationID: "nanhu", Revision: 4, HighReturn: true})
	if err != nil {
		t.Fatalf("record: %v", err)
	}
	reopened, err := OpenStore(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	records := reopened.List()
	if len(records) != 1 {
		t.Fatalf("reloaded %d records; want 1", len(records))
	}
	if string(records[0].Payload) != string(original.Payload) || records[0].ID != original.ID || records[0].Watermark != 1 {
		t.Fatalf("reloaded record differs from original: %+v vs %+v", records[0], original)
	}
	if next := reopened.NextWatermark("west"); next != 2 {
		t.Fatalf("next watermark after reload = %d; want 2", next)
	}
}

func TestStoreListReturnsCopies(t *testing.T) {
	store, _ := openTestStore(t)
	recorder := NewRecorder(store)
	if _, err := recorder.Record(KindPumpCommand, "north", "cmd-1", "stage up", PumpCommandPayload{CommandID: "cmd-1", ZoneID: "north", Stage: 3, Status: "applied"}); err != nil {
		t.Fatalf("record: %v", err)
	}
	records := store.List()
	records[0].Payload[0] = 'X'
	records[0].Summary = "tampered"
	fresh := store.List()
	if fresh[0].Summary == "tampered" || fresh[0].Payload[0] == 'X' {
		t.Fatalf("stored original was mutated through List result")
	}
}

func appendRaw(t *testing.T, store *Store, partition string, watermark uint64, kind Kind, payload any) Record {
	t.Helper()
	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	record, err := store.Append(Record{ID: partition + "-" + string(rune('a'+watermark)), Partition: partition, Watermark: watermark, Kind: kind, EntityID: "entity", Payload: encoded})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	return record
}

func TestInspectMarksGapsAndOutOfOrder(t *testing.T) {
	store, _ := openTestStore(t)
	appendRaw(t, store, "north", 1, KindPumpCommand, PumpCommandPayload{Stage: 1, Status: "applied"})
	appendRaw(t, store, "north", 4, KindPumpCommand, PumpCommandPayload{Stage: 2, Status: "applied"}) // gap: 2, 3
	appendRaw(t, store, "north", 3, KindPumpCommand, PumpCommandPayload{Stage: 3, Status: "applied"}) // late arrival
	appendRaw(t, store, "east", 2, KindPumpCommand, PumpCommandPayload{Stage: 1, Status: "applied"})  // gap: 1

	marked, health := Inspect(store.List(), Filter{})
	if len(marked) != 4 {
		t.Fatalf("marked %d records; want 4", len(marked))
	}
	if got := marked[1].MissingBefore; len(got) != 2 || got[0] != 2 || got[1] != 3 {
		t.Fatalf("record 4 missing_before = %v; want [2 3]", got)
	}
	if len(marked[2].Marks) != 1 || marked[2].Marks[0] != MarkOutOfOrder {
		t.Fatalf("late record marks = %v; want [out_of_order]", marked[2].Marks)
	}
	byPartition := map[string]PartitionHealth{}
	for _, item := range health {
		byPartition[item.Partition] = item
	}
	north := byPartition["north"]
	if len(north.Missing) != 2 || north.OutOfOrder != 1 || north.MaxWatermark != 4 {
		t.Fatalf("north health = %+v; want 2 missing, 1 out-of-order, max 4", north)
	}
	east := byPartition["east"]
	if len(east.Missing) != 1 || east.Missing[0] != 1 {
		t.Fatalf("east health = %+v; want missing [1]", east)
	}
}

func TestInspectFilterKeepsPartitionHealthWhole(t *testing.T) {
	store, _ := openTestStore(t)
	appendRaw(t, store, "north", 2, KindPumpCommand, PumpCommandPayload{Stage: 1, Status: "applied"})
	appendRaw(t, store, "north", 3, KindValveCommand, ValveCommandPayload{Open: true, Status: "applied"})
	marked, health := Inspect(store.List(), Filter{Kind: KindValveCommand})
	if len(marked) != 1 || marked[0].Kind != KindValveCommand {
		t.Fatalf("filtered marked = %+v; want only the valve record", marked)
	}
	if len(health) != 1 || len(health[0].Missing) != 1 || health[0].Missing[0] != 1 {
		t.Fatalf("health = %+v; want missing [1] even under kind filter", health)
	}
}

func TestRebuildBeforeAndAfter(t *testing.T) {
	store, _ := openTestStore(t)
	appendRaw(t, store, "north", 1, KindPumpCommand, PumpCommandPayload{CommandID: "c1", Stage: 2, Status: "applied"})
	appendRaw(t, store, "north", 2, KindValveCommand, ValveCommandPayload{CommandID: "c2", Open: false, Status: "applied"})
	appendRaw(t, store, "north", 3, KindProtectionLatch, LatchPayload{StationID: "xincheng", Revision: 7, HighReturn: true})
	appendRaw(t, store, "north", 4, KindTargetChange, TargetChangePayload{StationID: "xincheng", Celsius: 70, Revision: 9})
	appendRaw(t, store, "north", 5, KindPumpCommand, PumpCommandPayload{CommandID: "c3", Stage: 4, Status: "applied"})
	appendRaw(t, store, "east", 1, KindPumpCommand, PumpCommandPayload{CommandID: "c9", Stage: 9, Status: "applied"})

	result, err := Rebuild(store.List(), "north", 3)
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if result.Missing {
		t.Fatalf("watermark 3 reported missing")
	}
	if result.Before.PumpStage != 2 || result.Before.BypassOpen != false {
		t.Fatalf("before = %+v; want stage 2, bypass closed", result.Before)
	}
	if _, ok := result.Before.Latches["xincheng"]; ok {
		t.Fatalf("before should not contain the latch applied at watermark 3")
	}
	if result.After.PumpStage != 2 || !result.After.Latches["xincheng"].HighReturn {
		t.Fatalf("after = %+v; want stage 2 with high-return latch", result.After)
	}
	if _, ok := result.After.Targets["xincheng"]; ok {
		t.Fatalf("after watermark 3 should not contain the target change from watermark 4")
	}
	if result.Operation == nil || result.Operation.Kind != KindProtectionLatch {
		t.Fatalf("operation = %+v; want the latch record", result.Operation)
	}

	final, err := Rebuild(store.List(), "north", 5)
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if final.After.PumpStage != 4 {
		t.Fatalf("final stage = %d; want 4", final.After.PumpStage)
	}
	if final.Before.PumpStage != 2 {
		t.Fatalf("stage before watermark 5 = %d; want 2", final.Before.PumpStage)
	}
	if final.After.Targets["xincheng"].Celsius != 70 {
		t.Fatalf("final targets = %+v; want 70 °C", final.After.Targets)
	}
}

func TestRebuildMissingWatermark(t *testing.T) {
	store, _ := openTestStore(t)
	appendRaw(t, store, "north", 1, KindPumpCommand, PumpCommandPayload{Stage: 2, Status: "applied"})
	result, err := Rebuild(store.List(), "north", 9)
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if !result.Missing {
		t.Fatalf("watermark 9 should be reported missing")
	}
	if result.Operation != nil {
		t.Fatalf("operation = %+v; want nil for missing watermark", result.Operation)
	}
	if result.Before.PumpStage != 2 || result.After.PumpStage != 2 {
		t.Fatalf("state = %+v/%+v; want folded state at stage 2", result.Before, result.After)
	}
	if _, err := Rebuild(store.List(), "", 1); err == nil {
		t.Fatalf("expected error for empty partition")
	}
}

func TestRebuildAppliesOutOfOrderAtOwnPosition(t *testing.T) {
	store, _ := openTestStore(t)
	appendRaw(t, store, "north", 1, KindPumpCommand, PumpCommandPayload{Stage: 1, Status: "applied"})
	appendRaw(t, store, "north", 3, KindPumpCommand, PumpCommandPayload{Stage: 3, Status: "applied"})
	appendRaw(t, store, "north", 2, KindPumpCommand, PumpCommandPayload{Stage: 2, Status: "applied"}) // late
	result, err := Rebuild(store.List(), "north", 2)
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if result.After.PumpStage != 2 {
		t.Fatalf("after watermark 2 stage = %d; want 2 (late record applied at its own position)", result.After.PumpStage)
	}
	if result.Operation == nil || len(result.Operation.Marks) != 1 || result.Operation.Marks[0] != MarkOutOfOrder {
		t.Fatalf("operation marks = %+v; want out_of_order", result.Operation)
	}
}

func TestRecordStationResolvesPartition(t *testing.T) {
	store, _ := openTestStore(t)
	recorder := NewRecorder(store)
	recorder.SetStationResolver(func(stationID string) string {
		if stationID == "nanhu" {
			return "west"
		}
		return ""
	})
	known, err := recorder.RecordStation(KindTargetChange, "nanhu", "nanhu", "target", TargetChangePayload{StationID: "nanhu", Celsius: 64})
	if err != nil {
		t.Fatalf("record: %v", err)
	}
	if known.Partition != "west" {
		t.Fatalf("partition = %q; want west", known.Partition)
	}
	unknown, err := recorder.RecordStation(KindTargetChange, "ghost", "ghost", "target", TargetChangePayload{StationID: "ghost", Celsius: 60})
	if err != nil {
		t.Fatalf("record: %v", err)
	}
	if unknown.Partition != "station:ghost" {
		t.Fatalf("partition = %q; want explicit station fallback", unknown.Partition)
	}
}

func TestNilRecorderDropsSilently(t *testing.T) {
	var recorder *Recorder
	if _, err := recorder.Record(KindPumpCommand, "north", "e", "s", nil); err != nil {
		t.Fatalf("nil recorder record: %v", err)
	}
	if _, err := recorder.RecordStation(KindTargetChange, "s", "e", "s", nil); err != nil {
		t.Fatalf("nil recorder record station: %v", err)
	}
}
