package main

import (
	"bufio"
	"fmt"
	"net"
	"os"

	"github.com/codecrafters-io/redis-starter-go/app/store"
)

// db is the single shared store instance for this server.
var db = store.NewStore()

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	for {
		cmd, err := ParseCommand(reader)
		if err != nil {
			fmt.Println("Connection closed: ", err.Error())
			return
		}
		fmt.Println("Received command: ", cmd.Name(), cmd.Args)

		response := handleCommand(db, cmd)
		conn.Write(response)
	}
}