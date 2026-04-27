package store

import (
	"time"
)

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
