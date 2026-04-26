package main

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

type Command struct {
	Args []string
}

func (c Command) Name() string {
	if len(c.Args) == 0 {
		return ""
	}
	return c.Args[0]
}

func ParseCommand(r *bufio.Reader) (Command, error) {
	line, err := readLine(r)
	if err != nil {
		return Command{}, err
	}

	if len(line) == 0 {
		return Command{}, fmt.Errorf("empty input")
	}

	switch line[0] {
	case '*':
		return parseArray(r, line)
	case '+':
		// Simple string treated as a single-arg command (e.g. "+PING\r\n")
		return Command{Args: []string{string(line[1:])}}, nil
	default:
		return Command{}, fmt.Errorf("unsupported RESP type: %c", line[0])
	}
}

func parseArray(r *bufio.Reader, header string) (Command, error) {
	count, err := strconv.Atoi(header[1:])
	if err != nil {
		return Command{}, fmt.Errorf("invalid array length: %w", err)
	}

	args := make([]string, 0, count)
	for i := 0; i < count; i++ {
		s, err := parseBulkString(r)
		if err != nil {
			return Command{}, err
		}
		args = append(args, s)
	}

	return Command{Args: args}, nil
}

func parseBulkString(r *bufio.Reader) (string, error) {
	line, err := readLine(r)
	if err != nil {
		return "", err
	}

	if len(line) == 0 || line[0] != '$' {
		return "", fmt.Errorf("expected bulk string, got: %q", line)
	}

	length, err := strconv.Atoi(line[1:])
	if err != nil {
		return "", fmt.Errorf("invalid bulk string length: %w", err)
	}

	// Read exactly `length` bytes + 2 bytes for \r\n
	buf := make([]byte, length+2)
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return "", fmt.Errorf("reading bulk string data: %w", err)
	}

	return string(buf[:length]), nil
}

func readLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	// Strip trailing \r\n
	if len(line) >= 2 && line[len(line)-2] == '\r' {
		line = line[:len(line)-2]
	}
	return line, nil
}