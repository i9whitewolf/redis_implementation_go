package store

import (
	"fmt"
	"time"
)

type keyType int

const (
	typeString keyType = iota
	typeList keyType = 1
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
	values []string
}

var keyTypes = make(map[string]keyType)
var stringDict = make(map[string]stringEntry)
var listDict = make(map[string]listEntry)

func checkType(key string, expected keyType) error {
	if t, ok := keyTypes[key]; ok && t != expected {
		return fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return nil
}

// String operations

func StringSet(key, value string, expiresAt time.Time) error {
	if err := checkType(key, typeString); err != nil {
		return err
	}
	stringDict[key] = stringEntry{expiry: expiry{expiresAt: expiresAt}, value: value}
	keyTypes[key] = typeString
	return nil
}

func StringGet(key string) (string, bool) {
	e, ok := stringDict[key]
	if !ok {
		return "", false
	}
	if e.isExpired() {
		StringDelete(key)
		return "", false
	}
	return e.value, true
}

func StringDelete(key string) {
	delete(stringDict, key)
	delete(keyTypes, key)
}

// List operations

func ListLPush(key string, values ...string) error {
	if err := checkType(key, typeList); err != nil {
		return err
	}
	e := listDict[key]
	for _, v := range values {
		e.values = append([]string{v}, e.values...)
	}
	listDict[key] = e
	keyTypes[key] = typeList
	return nil
}

func ListPush(key string, values ...string) error {
	if err := checkType(key, typeList); err != nil {
		return err
	}
	e := listDict[key]
	e.values = append(e.values, values...)
	listDict[key] = e
	keyTypes[key] = typeList
	return nil
}

func ListPop(key string, count int) ([]string, bool) {
	e, ok := listDict[key]
	if !ok {
		return nil, false
	}
	if e.isExpired() {
		ListDelete(key)
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
	listDict[key] = e
	return popped, true
}

func ListGet(key string) ([]string, bool) {
	e, ok := listDict[key]
	if !ok {
		return nil, false
	}
	if e.isExpired() {
		ListDelete(key)
		return nil, false
	}
	return e.values, true
}

func ListDelete(key string) {
	delete(listDict, key)
	delete(keyTypes, key)
}

func ListRange(key string, start, stop int) ([]string, bool) {
	e, ok := listDict[key]
	if !ok {
		return nil, false
	}
	if e.isExpired() {
		ListDelete(key)
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