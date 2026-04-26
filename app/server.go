package main

import (
	"bufio"
	"fmt"
	"net"
	"strings"
)

// Session holds per-connection transaction state for MULTI/EXEC.
type Session struct {
	inMulti bool
	queue   []Command
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	session := &Session{}
	reader := bufio.NewReader(conn)
	for {
		cmd, err := ParseCommand(reader)
		if err != nil {
			fmt.Println("Connection closed: ", err.Error())
			return
		}
		fmt.Println("Received command: ", cmd.Name(), cmd.Args)

		switch strings.ToUpper(cmd.Name()) {
		case "MULTI":
			if session.inMulti {
				conn.Write(EncodeError("ERR MULTI calls can not be nested"))
			} else {
				session.inMulti = true
				conn.Write(EncodeSimpleString("OK"))
			}

		case "EXEC":
			if !session.inMulti {
				conn.Write(EncodeError("ERR EXEC without MULTI"))
				continue
			}
			session.inMulti = false
			// Execute every queued command and collect their responses.
			results := make([][]byte, len(session.queue))
			for i, qcmd := range session.queue {
				results[i] = handleCommand(db, qcmd)
			}
			session.queue = nil
			conn.Write(EncodeRawArray(results))

		case "DISCARD":
			if !session.inMulti {
				conn.Write(EncodeError("ERR DISCARD without MULTI"))
			} else {
				session.inMulti = false
				session.queue = nil
				conn.Write(EncodeSimpleString("OK"))
			}

		default:
			if session.inMulti {
				// Inside MULTI: queue command, don't execute yet.
				session.queue = append(session.queue, cmd)
				conn.Write([]byte("+QUEUED\r\n"))
			} else {
				conn.Write(handleCommand(db, cmd))
			}
		}
	}
}
