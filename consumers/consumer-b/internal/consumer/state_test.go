package consumer

import (
	"os"
	"path/filepath"
	"testing"
)

func TestProcessedStatePersistsAndDeduplicates(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "processed.txt")
	state, err := OpenProcessedState(path)
	if err != nil {
		t.Fatal(err)
	}
	if fresh, err := state.Record("hazard-1"); err != nil || !fresh {
		t.Fatalf("first record fresh=%v err=%v", fresh, err)
	}
	if fresh, err := state.Record("hazard-1"); err != nil || fresh {
		t.Fatalf("duplicate fresh=%v err=%v", fresh, err)
	}
	state, err = OpenProcessedState(path)
	if err != nil {
		t.Fatal(err)
	}
	if fresh, err := state.Record("hazard-1"); err != nil || fresh {
		t.Fatalf("restart duplicate fresh=%v err=%v", fresh, err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
}

func TestProcessedStateRepairsInterruptedAppend(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ids.txt")
	if err := os.WriteFile(path, []byte("hazard-1\nhazard-incomplete"), 0600); err != nil {
		t.Fatal(err)
	}
	state, err := OpenProcessedState(path)
	if err != nil {
		t.Fatal(err)
	}
	if fresh, err := state.Record("hazard-1"); err != nil || fresh {
		t.Fatalf("committed ID lost: %v %v", fresh, err)
	}
	if fresh, err := state.Record("hazard-incomplete"); err != nil || !fresh {
		t.Fatalf("partial record treated as committed: %v %v", fresh, err)
	}
	raw, err := os.ReadFile(path)
	if err != nil || string(raw) != "hazard-1\nhazard-incomplete\n" {
		t.Fatalf("ledger=%q err=%v", raw, err)
	}
}

func TestRevisionDeduplicationAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger")
	state, err := OpenProcessedState(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, revision := range []int64{0, 2, 3} {
		if fresh, err := state.RecordRevision("hazard-1", revision); err != nil || !fresh {
			t.Fatal(revision, fresh, err)
		}
	}
	state, err = OpenProcessedState(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, revision := range []int64{0, 1, 2, 3} {
		if fresh, err := state.RecordRevision("hazard-1", revision); err != nil || fresh {
			t.Fatal(revision, fresh, err)
		}
	}
	if fresh, err := state.RecordRevision("hazard-1", 4); err != nil || !fresh {
		t.Fatal(fresh, err)
	}
	if _, err := state.RecordRevision("hazard-1", -1); err == nil {
		t.Fatal("negative revision accepted")
	}
}
