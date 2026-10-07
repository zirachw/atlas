package consumer

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
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
	// A crash during append may leave an incomplete final record. Only complete
	// newline-terminated records are committed effects; repair the tail first.
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	committed := int64(0)
	for offset := info.Size(); offset > 0; {
		length := min(offset, 1024)
		offset -= length
		tail := make([]byte, length)
		if _, err := file.ReadAt(tail, offset); err != nil {
			return nil, err
		}
		if index := bytes.LastIndexByte(tail, '\n'); index >= 0 {
			committed = offset + int64(index) + 1
			break
		}
	}
	if committed != info.Size() {
		if err := os.Truncate(path, committed); err != nil {
			return nil, err
		}
		repair, err := os.OpenFile(path, os.O_WRONLY, 0600)
		if err != nil {
			return nil, err
		}
		err = repair.Sync()
		closeErr := repair.Close()
		if err != nil {
			return nil, err
		}
		if closeErr != nil {
			return nil, closeErr
		}
	}
	scanner := bufio.NewScanner(io.LimitReader(file, committed))
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
	before, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return false, err
	}
	if _, err = fmt.Fprintln(f, id); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		// A failed append must not leave a second committed record on retry.
		_ = os.Truncate(s.path, before.Size())
		return false, err
	}
	if closeErr != nil {
		// Sync already committed the effect. Remember it even if closing failed,
		// so a reconnect/redelivery cannot append it again in this process.
		s.ids[id] = struct{}{}
		return false, closeErr
	}
	s.ids[id] = struct{}{}
	return true, nil
}
