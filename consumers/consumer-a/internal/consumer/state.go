package consumer

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type ProcessedState struct {
	mu   sync.Mutex
	path string
	ids  map[string]struct{}
}

func OpenProcessedState(path string) (*ProcessedState, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	state := &ProcessedState{path: path, ids: make(map[string]struct{})}
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return state, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 256), 1024)
	for scanner.Scan() {
		id := strings.TrimSpace(scanner.Text())
		if validHazardID(id) {
			state.ids[id] = struct{}{}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return state, nil
}

func validHazardID(id string) bool {
	return id != "" && len(id) <= 512 && !strings.ContainsAny(id, "\r\n\x00")
}

// Record syncs the ID before the caller acknowledges the broker delivery.
func (s *ProcessedState) Record(id string) (bool, error) {
	if !validHazardID(id) {
		return false, errors.New("invalid hazard ID")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.ids[id]; exists {
		return false, nil
	}
	f, err := os.OpenFile(s.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return false, err
	}
	if _, err = fmt.Fprintln(f, id); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return false, err
	}
	if closeErr != nil {
		return false, closeErr
	}
	s.ids[id] = struct{}{}
	return true, nil
}
