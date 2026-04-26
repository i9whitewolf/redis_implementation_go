package main

import (
	"bytes"
	"fmt"
)

var Pong = []byte("+PONG\r\n")

func EncodeSimpleString(s string) []byte {
	return []byte(fmt.Sprintf("+%s\r\n", s))
}

func EncodeBulkString(s string) []byte {
	return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(s), s))
}

func EncodeNull() []byte {
	return []byte("$-1\r\n")
}

func EncodeInteger(i int) []byte {
	return []byte(fmt.Sprintf(":%d\r\n", i))
}

func EncodeArray(items []string) []byte {
	result := []byte(fmt.Sprintf("*%d\r\n", len(items)))
	for _, item := range items {
		result = append(result, EncodeBulkString(item)...)
	}
	return result
}

func EncodeNullArray() []byte {
	return []byte("*-1\r\n")
}

func EncodeError(msg string) []byte {
	return []byte(fmt.Sprintf("-%s\r\n", msg))
}

// EncodeRawArray wraps already-encoded RESP values into a RESP array.
// Used by EXEC to bundle individual command responses.
func EncodeRawArray(items [][]byte) []byte {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "*%d\r\n", len(items))
	for _, item := range items {
		buf.Write(item)
	}
	return buf.Bytes()
}