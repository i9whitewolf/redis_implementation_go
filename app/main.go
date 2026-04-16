package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"strings"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func handleConnection(connection net.Conn){
	// cleaning resources after usage
	defer connection.Close()

	for {
		buf := make([]byte,128)
		n, err := connection.Read(buf)
		
		if(err != nil){
			// EOF is returned by read when no more input is available
			if err == io.EOF {
				break
			}
			fmt.Println("Error reading from the connection: ", err.Error())
			break
		}
		
		lines := strings.Split(string(buf[:n]), "\r\n")
		fmt.Println("lines", lines)

		switch strings.ToUpper(lines[2]){
		case "PING":
			connection.Write([]byte("+PONG\r\n"))
		case "ECHO" :
			fmt.Fprintf(connection, "$%d\r\n%v\r\n", len(lines[4]),lines[4])
		}
	
	}
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment the code below to pass the first stage
	
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	defer l.Close()
	
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}
		go handleConnection(conn)
		
	}


}
