package store

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Stream Operations

func (s *Store) StreamAdd(key, rawID string, fields []string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.checkType(key, KeyTypeStream); err != nil {
		return "", err
	}

	sd := s.streamDict[key]
	ms, seq, err := resolveStreamID(rawID, sd.lastMs, sd.lastSeq, len(sd.entries) > 0)
	if err != nil {
		return "", err
	}

	id := fmt.Sprintf("%d-%d", ms, seq)
	sd.entries = append(sd.entries, StreamRecord{ID: id, Fields: fields})
	sd.lastMs = ms
	sd.lastSeq = seq

	// Notify any XREAD BLOCK waiters
	for _, ch := range sd.waiters {
		select {
		case ch <- struct{}{}:
		default:
		}
	}

	s.streamDict[key] = sd
	s.keyTypes[key] = KeyTypeStream
	s.incrementVersionLocked(key)
	return id, nil
}

// StreamTopID returns the ID of the last entry in the stream, or "0-0" if empty.
func (s *Store) StreamTopID(key string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sd, ok := s.streamDict[key]
	if !ok || len(sd.entries) == 0 {
		return "0-0"
	}
	return sd.entries[len(sd.entries)-1].ID
}

// StreamRegisterWaiter registers a channel to be notified on stream append.
func (s *Store) StreamRegisterWaiter(key string, ch chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sd, ok := s.streamDict[key]
	if !ok {
		sd = streamData{}
		s.keyTypes[key] = KeyTypeStream
	}
	sd.waiters = append(sd.waiters, ch)
	s.streamDict[key] = sd
}

// StreamRemoveWaiter removes a notification channel from the stream's waiters.
func (s *Store) StreamRemoveWaiter(key string, ch chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sd, ok := s.streamDict[key]
	if !ok {
		return
	}
	for i, w := range sd.waiters {
		if w == ch {
			sd.waiters = append(sd.waiters[:i], sd.waiters[i+1:]...)
			s.streamDict[key] = sd
			break
		}
	}
}

// resolveStreamID parses or generates the (ms, seq) parts of a stream entry ID.
// It is a package-level helper so it stays pure and independently testable.
func resolveStreamID(rawID string, lastMs, lastSeq uint64, hasEntries bool) (ms, seq uint64, err error) {
	now := uint64(time.Now().UnixMilli())

	if rawID == "*" {
		ms = now
		if ms < lastMs {
			ms = lastMs // clock went backwards — clamp to last
		}
		if ms == lastMs && hasEntries {
			seq = lastSeq + 1
		} else {
			seq = 0
		}
		return ms, seq, nil
	}

	parts := strings.SplitN(rawID, "-", 2)
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("ERR Invalid stream ID specified as stream command argument")
	}

	ms, err = strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("ERR Invalid stream ID specified as stream command argument")
	}

	if parts[1] == "*" {
		// ms given, auto-generate sequence
		if ms < lastMs {
			return 0, 0, fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
		}
		if ms == lastMs {
			seq = lastSeq + 1
		} // else ms > lastMs → seq stays 0
		return ms, seq, nil
	}

	// Fully explicit ID
	seq, err = strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("ERR Invalid stream ID specified as stream command argument")
	}
	if ms == 0 && seq == 0 {
		return 0, 0, fmt.Errorf("ERR The ID specified in XADD must be greater than 0-0")
	}
	if hasEntries && (ms < lastMs || (ms == lastMs && seq <= lastSeq)) {
		return 0, 0, fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
	}
	return ms, seq, nil
}

// StreamRange returns entries in [startID, endID] (inclusive).
// startID may be "-" (stream minimum) and endID may be "+" (stream maximum).
// Returns an empty slice when the key doesn't exist; returns an error only on WRONGTYPE.
func (s *Store) StreamRange(key, startID, endID string) ([]StreamRecord, error) {
	s.mu.RLock() // read-only: no writes needed
	defer s.mu.RUnlock()

	if err := s.checkType(key, KeyTypeStream); err != nil {
		return nil, err // propagate WRONGTYPE so the caller can send an error reply
	}

	sd, exists := s.streamDict[key]
	if !exists {
		return []StreamRecord{}, nil // empty array response, not null
	}

	var results []StreamRecord
	for _, record := range sd.entries {
		// Use numeric comparison — string comparison breaks for "10-0" vs "9-0"
		if streamIDCmp(record.ID, startID) >= 0 && streamIDCmp(record.ID, endID) <= 0 {
			results = append(results, record)
		}
	}
	return results, nil // return empty slice (not nil) when nothing matches
}

func (s *Store) StreamRead(key, startID string) ([]StreamRecord, error) {
	s.mu.RLock() // read-only: no writes needed
	defer s.mu.RUnlock()

	if err := s.checkType(key, KeyTypeStream); err != nil {
		return nil, err // propagate WRONGTYPE so the caller can send an error reply
	}

	sd, exists := s.streamDict[key]
	if !exists {
		return []StreamRecord{}, nil // empty array response, not null
	}

	var results []StreamRecord
	for _, record := range sd.entries {
		// Use numeric comparison — string comparison breaks for "10-0" vs "9-0"
		if streamIDCmp(record.ID, startID) > 0 {
			results = append(results, record)
		}
	}
	return results, nil // return empty slice (not nil) when nothing matches
}

// streamIDCmp compares two stream IDs numerically.
// Returns negative if a < b, 0 if equal, positive if a > b.
// Handles the Redis special values "-" (minimum) and "+" (maximum).
func streamIDCmp(a, b string) int {
	if a == b {
		return 0
	}
	// Special boundary values
	if a == "-" || b == "+" {
		return -1
	}
	if a == "+" || b == "-" {
		return 1
	}
	aMs, aSeq, _ := parseStreamID(a)
	bMs, bSeq, _ := parseStreamID(b)
	if aMs != bMs {
		if aMs < bMs {
			return -1
		}
		return 1
	}
	if aSeq != bSeq {
		if aSeq < bSeq {
			return -1
		}
		return 1
	}
	return 0
}

// parseStreamID splits a "ms-seq" string into its numeric parts.
func parseStreamID(id string) (ms, seq uint64, err error) {
	parts := strings.SplitN(id, "-", 2)
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("ERR Invalid stream ID: %s", id)
	}
	ms, err = strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("ERR Invalid stream ID: %s", id)
	}
	seq, err = strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("ERR Invalid stream ID: %s", id)
	}
	return ms, seq, nil
}
