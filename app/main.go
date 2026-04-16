package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func handleConnection(connection net.Conn, redisStore map[string]string){
	// cleaning resources after usage
	defer connection.Close()

	for {
		buf := make([]byte,4096)
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
		case "SET" :
			redisStore[lines[4]] = lines[6]
			argCount, err := strconv.Atoi(lines[0][1:])
			if(err != nil){
				fmt.Println("error converting string to in using Atoi", err.Error())
			}
			if(argCount > 3){
				switch strings.ToUpper(lines[8]){
				case "EX" :
					setTime, err := strconv.Atoi(lines[10])
					if err != nil{
						fmt.Println("invalid expiry time: ", err.Error())
					}
					time.AfterFunc(time.Duration(setTime) * time.Second,func(){
						delete(redisStore,lines[4])
					})

				case "PX" :
					setTime, err := strconv.Atoi(lines[10])
					if err != nil{
						fmt.Println("invalid expiry time: ", err.Error())
					}
					time.AfterFunc(time.Duration(setTime) * time.Millisecond,func(){
						delete(redisStore,lines[4])
					})

				}
			}
			connection.Write([]byte("+OK\r\n"))
		case "GET" :
			val, ok := redisStore[lines[4]]
			if ok {
				fmt.Fprintf(connection, "$%d\r\n%s\r\n", len(val), val)
			}else {
				fmt.Fprintf(connection, "$-1\r\n")
			}
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

	var redisStore map[string]string
	redisStore = make(map[string]string)
	
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}

		go handleConnection(conn,redisStore)
		
	}


}
