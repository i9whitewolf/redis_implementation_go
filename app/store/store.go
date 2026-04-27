package store

import (
	"fmt"
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
