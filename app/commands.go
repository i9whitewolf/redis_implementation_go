package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/store"
)

func handleCommand(db *store.Store, cmd Command) []byte {
	name := strings.ToUpper(cmd.Name())

	if min, ok := minArgs[name]; ok && len(cmd.Args) < min {
		return []byte(fmt.Sprintf("-ERR wrong number of arguments for '%s' command\r\n", strings.ToLower(name)))
	}

	switch name {
	case "PING":
		return handlePing()
	case "ECHO":
		return handleEcho(cmd)
	case "GET":
		return handleGet(db, cmd)
	case "SET":
		return handleSet(db, cmd)
	case "RPUSH":
		return handleRPush(db, cmd)
	case "LPUSH":
		return handleLPush(db, cmd)
	case "LRANGE":
		return handleLRange(db, cmd)
	case "LLEN":
		return handleLLen(db, cmd)
	case "LPOP":
		return handleLPop(db, cmd)
	case "BLPOP":
		return handleBLPop(db, cmd)
	case "INCR":
		return handleIncr(db, cmd)
	case "TYPE":
		return handleType(db, cmd)
	case "XADD":
		return handleXAdd(db, cmd)
	case "XRANGE":
		return handleXRange(db, cmd)
	case "XREAD":
		return handleXRead(db, cmd)
	default:
		return []byte("-ERR unknown command\r\n")
	}
}

func handlePing() []byte {
	return Pong
}

func handleEcho(cmd Command) []byte {
	return EncodeBulkString(cmd.Args[1])
}

func handleGet(db *store.Store, cmd Command) []byte {
	value, ok := db.StringGet(cmd.Args[1])
	if !ok {
		return EncodeNull()
	}
	return EncodeBulkString(value)
}

func handleSet(db *store.Store, cmd Command) []byte {

	var expiresAt time.Time

	// Parse optional EX/PX arguments
	for i := 3; i < len(cmd.Args)-1; i++ {
		switch strings.ToUpper(cmd.Args[i]) {
		case "EX":
			secs, err := strconv.Atoi(cmd.Args[i+1])
			if err != nil {
				return []byte("-ERR value is not an integer or out of range\r\n")
			}
			expiresAt = time.Now().Add(time.Duration(secs) * time.Second)
			i++
		case "PX":
			ms, err := strconv.Atoi(cmd.Args[i+1])
			if err != nil {
				return []byte("-ERR value is not an integer or out of range\r\n")
			}
			expiresAt = time.Now().Add(time.Duration(ms) * time.Millisecond)
			i++
		}
	}

	if err := db.StringSet(cmd.Args[1], cmd.Args[2], expiresAt); err != nil {
		return []byte("-" + err.Error() + "\r\n")
	}
	return EncodeSimpleString("OK")
}

func handleRPush(db *store.Store, cmd Command) []byte {
	n, err := db.ListPush(cmd.Args[1], cmd.Args[2:]...)
	if err != nil {
		return []byte("-" + err.Error() + "\r\n")
	}
	return EncodeInteger(n)
}

func handleLPush(db *store.Store, cmd Command) []byte {
	n, err := db.ListLPush(cmd.Args[1], cmd.Args[2:]...)
	if err != nil {
		return []byte("-" + err.Error() + "\r\n")
	}
	return EncodeInteger(n)
}

func handleLRange(db *store.Store, cmd Command) []byte {
	start, err := strconv.Atoi(cmd.Args[2])
	if err != nil {
		return []byte("-ERR value is not an integer or out of range\r\n")
	}
	stop, err := strconv.Atoi(cmd.Args[3])
	if err != nil {
		return []byte("-ERR value is not an integer or out of range\r\n")
	}
	values, _ := db.ListRange(cmd.Args[1], start, stop)
	return EncodeArray(values)
}

func handleLLen(db *store.Store, cmd Command) []byte {
	values, _ := db.ListGet(cmd.Args[1])
	return EncodeInteger(len(values))
}

func handleLPop(db *store.Store, cmd Command) []byte {
	count := 1
	hasCount := false
	if len(cmd.Args) > 2 {
		c, err := strconv.Atoi(cmd.Args[2])
		if err != nil {
			return []byte("-ERR value is not an integer or out of range\r\n")
		}
		count = c
		hasCount = true
	}
	values, ok := db.ListPop(cmd.Args[1], count)
	if !ok {
		return EncodeNull()
	}
	if !hasCount {
		return EncodeBulkString(values[0])
	}
	return EncodeArray(values)
}

func handleBLPop(db *store.Store, cmd Command) []byte {
	key := cmd.Args[1]

	var timeout float64
	if len(cmd.Args) > 2 {
		t, err := strconv.ParseFloat(cmd.Args[len(cmd.Args)-1], 64)
		if err != nil {
			return []byte("-ERR value is not an integer or out of range\r\n")
		}
		timeout = t
	}

	// Atomically: pop immediately if the list is non-empty, OR register as a
	// waiter. No polling loop, no race between the check and the registration.
	value, ok, ch := db.ListPopOrWait(key)
	if ok {
		return EncodeArray([]string{key, value})
	}

	// List was empty — ch is now registered. Clean it up when we return.
	defer db.ListRemoveWaiter(key, ch)

	if timeout == 0 {
		// Block indefinitely until a push wakes us.
		value = <-ch
		return EncodeArray([]string{key, value})
	}

	timer := time.NewTimer(time.Duration(timeout * float64(time.Second)))
	defer timer.Stop()

	select {
	case value = <-ch:
		return EncodeArray([]string{key, value})
	case <-timer.C:
		return EncodeNullArray()
	}
}

func handleIncr(db *store.Store, cmd Command) []byte {
	key := cmd.Args[1]

	var n int

	// If the key exists, it must hold a valid integer.
	if val, exists := db.StringGet(key); exists {
		var err error
		n, err = strconv.Atoi(val)
		if err != nil {
			return EncodeError("ERR value is not an integer or out of range")
		}
	}
	// Key absent → n stays 0, so n++ gives 1 (Redis default).

	n++
	if err := db.StringSet(key, strconv.Itoa(n), time.Time{}); err != nil {
		return EncodeError(err.Error())
	}
	return EncodeInteger(n)
}

func handleType(db *store.Store, cmd Command) []byte {
	key := cmd.Args[1]
	return EncodeSimpleString(db.GetTypeName(key))
}

func handleXAdd(db *store.Store, cmd Command) []byte {
	key := cmd.Args[1]
	rawID := cmd.Args[2]
	fields := cmd.Args[3:]

	// fields must come in pairs: field value [field value ...]
	if len(fields) == 0 || len(fields)%2 != 0 {
		return EncodeError("ERR wrong number of arguments for 'xadd' command")
	}

	id, err := db.StreamAdd(key, rawID, fields)
	if err != nil {
		return EncodeError(err.Error())
	}
	return EncodeBulkString(id) // return the GENERATED id, not the raw input
}

func handleXRange(db *store.Store, cmd Command) []byte {
	key := cmd.Args[1]
	startID := cmd.Args[2]
	endID := cmd.Args[3]

	entries, err := db.StreamRange(key, startID, endID)
	if err != nil {
		return EncodeError(err.Error())
	}

	// Each stream entry is encoded as [id, [field, value, ...]]
	records := make([][]byte, len(entries))
	for i, entry := range entries {
		records[i] = EncodeRawArray([][]byte{
			EncodeBulkString(entry.ID),
			EncodeArray(entry.Fields),
		})
	}
	return EncodeRawArray(records) // empty array for no matches, not null
}

func handleXRead(db *store.Store, cmd Command) []byte {
	// Syntax: XREAD [COUNT n] [BLOCK ms] STREAMS key [key ...] id [id ...]
	streamsIdx := -1
	blockMs := -1 // -1 means no block

	for i := 1; i < len(cmd.Args); i++ {
		arg := strings.ToUpper(cmd.Args[i])
		if arg == "BLOCK" && i+1 < len(cmd.Args) {
			ms, err := strconv.Atoi(cmd.Args[i+1])
			if err == nil {
				blockMs = ms
			}
			i++
		} else if arg == "STREAMS" {
			streamsIdx = i
			break
		}
	}

	if streamsIdx == -1 {
		return EncodeError("ERR syntax error")
	}

	rest := cmd.Args[streamsIdx+1:]
	if len(rest) == 0 || len(rest)%2 != 0 {
		return EncodeError("ERR syntax error")
	}
	numStreams := len(rest) / 2
	keys := rest[:numStreams]
	startIDs := rest[numStreams:]

	// Resolve "$" to top ID
	for i, id := range startIDs {
		if id == "$" {
			startIDs[i] = db.StreamTopID(keys[i])
		}
	}

	if blockMs != -1 {
		ch := make(chan struct{}, 1)
		for _, key := range keys {
			db.StreamRegisterWaiter(key, ch)
		}
		defer func() {
			for _, key := range keys {
				db.StreamRemoveWaiter(key, ch)
			}
		}()

		// Try to read first
		res, err := attemptXRead(db, keys, startIDs)
		if err != nil {
			return EncodeError(err.Error())
		}
		if len(res) > 0 {
			return EncodeRawArray(res)
		}

		// Block
		if blockMs == 0 {
			<-ch
		} else {
			select {
			case <-ch:
			case <-time.After(time.Duration(blockMs) * time.Millisecond):
				return EncodeNullArray()
			}
		}
	}

	// Read (either non-blocking, or after wakeup)
	res, err := attemptXRead(db, keys, startIDs)
	if err != nil {
		return EncodeError(err.Error())
	}
	if len(res) == 0 {
		return EncodeNullArray()
	}
	return EncodeRawArray(res)
}

func attemptXRead(db *store.Store, keys []string, startIDs []string) ([][]byte, error) {
	streamResults := make([][]byte, 0, len(keys))
	for i, key := range keys {
		entries, err := db.StreamRead(key, startIDs[i])
		if err != nil {
			return nil, err
		}
		if len(entries) == 0 {
			continue // skip streams with no new entries
		}

		// Encode entries as [[id, [field, value, ...]], ...]
		entryRecords := make([][]byte, len(entries))
		for j, entry := range entries {
			entryRecords[j] = EncodeRawArray([][]byte{
				EncodeBulkString(entry.ID),
				EncodeArray(entry.Fields),
			})
		}

		// Wrap as [stream_key, entries_array]
		streamResults = append(streamResults, EncodeRawArray([][]byte{
			EncodeBulkString(key),
			EncodeRawArray(entryRecords),
		}))
	}
	return streamResults, nil
}