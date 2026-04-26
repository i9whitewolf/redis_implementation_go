package store

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

// KeyType is the Redis data type for a key.
// Using a string-backed type means the constant value IS the wire format —
// no translation needed, no iota fragility, and it's exported for cross-package use.
type KeyType string

const (
	KeyTypeNone      KeyType = "none"
	KeyTypeString    KeyType = "string"
	KeyTypeList      KeyType = "list"
	KeyTypeHash      KeyType = "hash"
	KeyTypeZSet      KeyType = "zset"
	KeyTypeStream    KeyType = "stream"
	KeyTypeVectorSet KeyType = "vectorset"
)

type expiry struct {
	expiresAt time.Time
}

func (e expiry) isExpired() bool {
	return !e.expiresAt.IsZero() && time.Now().After(e.expiresAt)
}

type stringEntry struct {
	expiry
	value string
}

type listEntry struct {
	expiry
	values  []string
	waiters []chan string // BLPOP clients waiting for a value
}

// StreamRecord is a single entry inside a Redis stream.
type StreamRecord struct {
	ID     string
	Fields []string // flat [field, value, field, value, ...]
}

type streamData struct {
	expiry
	entries []StreamRecord
	lastMs  uint64 // milliseconds part of the last inserted ID
	lastSeq uint64 // sequence part of the last inserted ID
}

type Store struct {
	mu         sync.RWMutex
	keyTypes   map[string]KeyType
	stringDict map[string]stringEntry
	listDict   map[string]listEntry
	streamDict map[string]streamData
	versions   map[string]uint64 // incremented on every write; used by WATCH
}

// NewStore creates and returns an initialised Store.
func NewStore() *Store {
	return &Store{
		keyTypes:   make(map[string]KeyType),
		stringDict: make(map[string]stringEntry),
		listDict:   make(map[string]listEntry),
		streamDict: make(map[string]streamData),
		versions:   make(map[string]uint64),
	}
}

// GetVersion returns the current write version for key.
// A WATCH snapshot taken before a write will differ after the write.
func (s *Store) GetVersion(key string) uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.versions[key]
}

// GetTypeName returns the Redis type name for the key (e.g. "string", "list", "none").
// Because KeyType values ARE the wire-format strings, this is just a map lookup.
func (s *Store) GetTypeName(key string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if t, ok := s.keyTypes[key]; ok {
		return string(t)
	}
	return string(KeyTypeNone)
}

// incrementVersionLocked bumps the version for key. Caller must hold mu.Lock().
func (s *Store) incrementVersionLocked(key string) {
	s.versions[key]++
}

// checkType must be called with at least a read lock already held by the caller.
func (s *Store) checkType(key string, expected KeyType) error {
	if t, ok := s.keyTypes[key]; ok && t != expected {
		return fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return nil
}

// String operations

func (s *Store) StringSet(key, value string, expiresAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.checkType(key, KeyTypeString); err != nil {
		return err
	}
	s.stringDict[key] = stringEntry{expiry: expiry{expiresAt: expiresAt}, value: value}
	s.keyTypes[key] = KeyTypeString
	s.incrementVersionLocked(key)
	return nil
}

func (s *Store) StringGet(key string) (string, bool) {
	s.mu.Lock() // full lock: expiry may trigger a delete (write)
	defer s.mu.Unlock()

	e, ok := s.stringDict[key]
	if !ok {
		return "", false
	}
	if e.isExpired() {
		s.stringDeleteLocked(key) // internal helper — lock already held
		return "", false
	}
	return e.value, true
}

// not usefull for now
func (s *Store) StringDelete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stringDeleteLocked(key)
}

// stringDeleteLocked deletes a string key; caller must already hold mu.Lock().
func (s *Store) stringDeleteLocked(key string) {
	delete(s.stringDict, key)
	delete(s.keyTypes, key)
	s.incrementVersionLocked(key)
}

// List operations

func (s *Store) Listlength(key string) (int, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.listDict[key]
	if !ok {
		return 0, false
	}
	if e.isExpired() {
		s.listDeleteLocked(key)
		return 0, false
	}
	return len(e.values), true
}

func (s *Store) ListLPush(key string, values ...string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.checkType(key, KeyTypeList); err != nil {
		return 0, err
	}
	// Push all values into the list first so the length is correct.
	e := s.listDict[key]
	for _, v := range values {
		e.values = append([]string{v}, e.values...)
	}
	s.listDict[key] = e
	s.keyTypes[key] = KeyTypeList
	s.incrementVersionLocked(key)
	newLen := len(e.values)

	// Wake up one BLPOP waiter per pushed value (they pop from the list).
	for range values {
		if !s.notifyWaiterFromListLocked(key) {
			break
		}
	}
	return newLen, nil
}

func (s *Store) ListPush(key string, values ...string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.checkType(key, KeyTypeList); err != nil {
		return 0, err
	}
	// Push all values into the list first so the length is correct.
	e := s.listDict[key]
	e.values = append(e.values, values...)
	s.listDict[key] = e
	s.keyTypes[key] = KeyTypeList
	s.incrementVersionLocked(key)
	newLen := len(e.values)

	// Wake up one BLPOP waiter per pushed value (they pop from the list).
	for range values {
		if !s.notifyWaiterFromListLocked(key) {
			break
		}
	}
	return newLen, nil
}

func (s *Store) ListPop(key string, count int) ([]string, bool) {
	s.mu.Lock() // full lock: modifies the list
	defer s.mu.Unlock()

	e, ok := s.listDict[key]
	if !ok {
		return nil, false
	}
	if e.isExpired() {
		s.listDeleteLocked(key)
		return nil, false
	}
	if len(e.values) == 0 {
		return nil, false
	}
	if count > len(e.values) {
		count = len(e.values)
	}
	popped := make([]string, count)
	copy(popped, e.values[:count])
	e.values = e.values[count:]
	s.listDict[key] = e
	s.incrementVersionLocked(key)
	return popped, true
}

func (s *Store) ListGet(key string) ([]string, bool) {
	s.mu.Lock() // full lock: expiry may trigger a delete (write)
	defer s.mu.Unlock()

	e, ok := s.listDict[key]
	if !ok {
		return nil, false
	}
	if e.isExpired() {
		s.listDeleteLocked(key)
		return nil, false
	}
	return e.values, true
}

// not usefull for now
func (s *Store) ListDelete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.listDeleteLocked(key)
}

// listDeleteLocked deletes a list key; caller must already hold mu.Lock().
func (s *Store) listDeleteLocked(key string) {
	delete(s.listDict, key)
	delete(s.keyTypes, key)
	s.incrementVersionLocked(key)
}

// notifyWaiterFromListLocked pops the head of the list and sends it to the first
// BLPOP waiter. The channel is buffered(1) so this never blocks.
// Caller must hold mu.Lock().
func (s *Store) notifyWaiterFromListLocked(key string) bool {
	e, ok := s.listDict[key]
	if !ok || len(e.waiters) == 0 || len(e.values) == 0 {
		return false
	}
	v := e.values[0]
	e.values = e.values[1:]
	ch := e.waiters[0]
	e.waiters = e.waiters[1:]
	s.listDict[key] = e
	ch <- v // buffered cap-1 channel; never blocks
	return true
}

// ListPopOrWait atomically either pops the head value (if the list is non-empty)
// or registers ch as a waiter and returns it. This is race-free: no push can
// slip between the emptiness check and the registration.
//
//	value, ok, _  → list had an item; use value directly
//	"",    false, ch → list was empty; block on ch until a push notifies you
func (s *Store) ListPopOrWait(key string) (string, bool, chan string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.listDict[key]
	if ok && !e.isExpired() && len(e.values) > 0 {
		v := e.values[0]
		e.values = e.values[1:]
		s.listDict[key] = e
		return v, true, nil
	}

	// List is empty — register as a waiter.
	ch := make(chan string, 1) // buffered so notifyWaiterLocked never blocks
	if !ok {
		// Key doesn't exist yet; create a skeleton entry so waiters are stored.
		e = listEntry{}
		s.keyTypes[key] = KeyTypeList
	}
	e.waiters = append(e.waiters, ch)
	s.listDict[key] = e
	return "", false, ch
}

// ListRemoveWaiter removes ch from the waiter list (called on timeout cleanup).
func (s *Store) ListRemoveWaiter(key string, ch chan string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.listDict[key]
	if !ok {
		return
	}
	for i, w := range e.waiters {
		if w == ch {
			e.waiters = append(e.waiters[:i], e.waiters[i+1:]...)
			break
		}
	}
	s.listDict[key] = e
}

func (s *Store) ListRange(key string, start, stop int) ([]string, bool) {
	s.mu.Lock() // full lock: expiry may trigger a delete (write)
	defer s.mu.Unlock()

	e, ok := s.listDict[key]
	if !ok {
		return nil, false
	}
	if e.isExpired() {
		s.listDeleteLocked(key)
		return nil, false
	}

	length := len(e.values)

	// Convert negative indices to positive
	if start < 0 {
		start = length + start
	}
	if stop < 0 {
		stop = length + stop
	}

	// Clamp to valid bounds
	if start < 0 {
		start = 0
	}
	if stop >= length {
		stop = length - 1
	}

	// Empty result if start is beyond stop
	if start > stop {
		return []string{}, true
	}

	return e.values[start : stop+1], true
}

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
	s.streamDict[key] = sd
	s.keyTypes[key] = KeyTypeStream
	s.incrementVersionLocked(key)
	return id, nil
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
			seq = 1
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
		// else stream is empty → seq stays 0
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
