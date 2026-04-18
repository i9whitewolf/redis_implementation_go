package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func handleConnection(connection net.Conn){
	// cleaning resources after usage
	defer connection.Close()

	redisStore := make(map[string]any)

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
				fmt.Fprintf(connection, "$%d\r\n%s\r\n", len(val.(string)), val)
			}else {
				fmt.Fprintf(connection, "$-1\r\n")
			}
		case "RPUSH" :
			key := lines[4]
			if existingArr, ok := redisStore[key]; ok {
				arr := existingArr.([]string)
				for i := 6; i < len(lines); i+=2 {
					arr = append(arr, lines[i])
				}
				redisStore[key] = arr
			}else{
				redisStore[key] = make([]string,0)
				arr := redisStore[key].([]string)
				for i := 6; i < len(lines); i+=2 {
					arr = append(arr, lines[i])
				}
				redisStore[key] = arr
			}
			fmt.Fprintf(connection, ":%d\r\n", len(redisStore[key].([]string)))
		case "LRANGE" :
			key := lines[4]
			arr, ok := redisStore[key].([]string)
			if !ok {
				fmt.Fprintf(connection, "*0\r\n")
				continue
			}
			n := len(arr)
			start, err1 := strconv.Atoi(lines[6])
			if err1 != nil {
				fmt.Println("invalid start index: ", err1.Error())
			}
			if start < -n {
				start = 0
			}
			start = (start%n + n)%n
			end, err2 := strconv.Atoi(lines[8])
			if err2 != nil {
				fmt.Println("invalid end index: ", err2.Error())
			}
			if end > n-1 {
				end = n-1
			}
			end = (end%n + n)%n
			if existingArr, ok := redisStore[key]; ok {
				arr := existingArr.([]string)
				if start > end || start >= len(arr) {
					fmt.Fprintf(connection, "*0\r\n")
				}else{
					fmt.Fprintf(connection, "*%d\r\n", end-start+1)
					for i := start; i <= end; i++ {
						fmt.Fprintf(connection, "$%d\r\n%s\r\n", len(arr[i]), arr[i])
					}
				}

			}else{
				fmt.Fprintf(connection, "*0\r\n")
			}
		case "LPUSH" :
			key := lines[4]
			existingArr, ok := redisStore[key].([]string)
			if !ok{
				existingArr = make([]string,0)
			}
			slices.Reverse(existingArr)
			for i := 6; i < len(lines); i+=2 {
				existingArr = append(existingArr, lines[i])
			}
			slices.Reverse(redisStore[key].([]string))
			redisStore[key] = existingArr
			fmt.Fprintf(connection, ":%d\r\n", len(existingArr))
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
