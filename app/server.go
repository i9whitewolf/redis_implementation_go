package main

import (
	"bufio"
	"fmt"
	"net"
	"strings"
)

// Session holds per-connection state: MULTI/EXEC transaction queue and WATCH snapshots.
type Session struct {
	inMulti        bool
	queue          []Command
	watchedVersions map[string]uint64 // key → version at WATCH time; nil means nothing watched
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

			// If any watched key was modified since WATCH, abort the transaction.
			aborted := false
			for key, ver := range session.watchedVersions {
				if db.GetVersion(key) != ver {
					aborted = true
					break
				}
			}
			session.watchedVersions = nil // unwatch after EXEC regardless

			if aborted {
				session.queue = nil
				conn.Write(EncodeNullArray()) // signal to client: transaction aborted
				continue
			}

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
				session.watchedVersions = nil // unwatch on DISCARD too
				conn.Write(EncodeSimpleString("OK"))
			}

		case "WATCH":
			if session.inMulti {
				conn.Write(EncodeError("ERR WATCH inside MULTI is not allowed"))
				continue
			}
			// Snapshot the current version of each key to watch.
			if session.watchedVersions == nil {
				session.watchedVersions = make(map[string]uint64)
			}
			for _, key := range cmd.Args[1:] {
				session.watchedVersions[key] = db.GetVersion(key)
			}
			conn.Write(EncodeSimpleString("OK"))

		case "UNWATCH":
			session.watchedVersions = nil
			conn.Write(EncodeSimpleString("OK"))

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
