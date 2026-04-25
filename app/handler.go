package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/store"
)

func handleCommand(cmd Command) []byte {
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
		return handleGet(cmd)
	case "SET":
		return handleSet(cmd)
	case "RPUSH":
		return handleRPush(cmd)
	case "LPUSH":
		return handleLPush(cmd)
	case "LRANGE":
		return handleLRange(cmd)
	case "LLEN":
		return handleLLen(cmd)
	case "LPOP":
		return handleLPop(cmd)
	case "BLPOP":
		return handleBLPop(cmd)
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

func handleGet(cmd Command) []byte {
	value, ok := store.StringGet(cmd.Args[1])
	if !ok {
		return EncodeNull()
	}
	return EncodeBulkString(value)
}

func handleSet(cmd Command) []byte {

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

	if err := store.StringSet(cmd.Args[1], cmd.Args[2], expiresAt); err != nil {
		return []byte("-" + err.Error() + "\r\n")
	}
	return EncodeSimpleString("OK")
}

func handleRPush(cmd Command) []byte {
	if err := store.ListPush(cmd.Args[1], cmd.Args[2:]...); err != nil {
		return []byte("-" + err.Error() + "\r\n")
	}
	values, _ := store.ListGet(cmd.Args[1])
	return EncodeInteger(len(values))
}

func handleLPush(cmd Command) []byte {
	if err := store.ListLPush(cmd.Args[1], cmd.Args[2:]...); err != nil {
		return []byte("-" + err.Error() + "\r\n")
	}
	values, _ := store.ListGet(cmd.Args[1])
	return EncodeInteger(len(values))
}

func handleLRange(cmd Command) []byte {
	start, err := strconv.Atoi(cmd.Args[2])
	if err != nil {
		return []byte("-ERR value is not an integer or out of range\r\n")
	}
	stop, err := strconv.Atoi(cmd.Args[3])
	if err != nil {
		return []byte("-ERR value is not an integer or out of range\r\n")
	}
	values, _ := store.ListRange(cmd.Args[1], start, stop)
	return EncodeArray(values)
}

func handleLLen(cmd Command) []byte {
	values, _ := store.ListGet(cmd.Args[1])
	return EncodeInteger(len(values))
}

func handleLPop(cmd Command) []byte {
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
	values, ok := store.ListPop(cmd.Args[1], count)
	if !ok {
		return EncodeNull()
	}
	if !hasCount {
		return EncodeBulkString(values[0])
	}
	return EncodeArray(values)
}

func handleBLPop(cmd Command) []byte {
	var timeout float64
	if len(cmd.Args) > 2 {
		t, err := strconv.ParseFloat(cmd.Args[2], 64)
		if err != nil {
			return []byte("-ERR value is not an integer or out of range\r\n")
		}
		timeout = t
	}
	deadline := time.Now().Add(time.Duration(timeout*float64(time.Second)))
	for timeout == 0 || time.Now().Before(deadline) {
		values, ok := store.ListPop(cmd.Args[1], 1)
		if ok {
			time.Sleep(time.Duration(timeout) * time.Second)
			return EncodeArray([]string{cmd.Args[1], values[0]})
		}
		time.Sleep(50 * time.Millisecond)
	}
	return EncodeNullArray()
}