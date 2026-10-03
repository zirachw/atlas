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
