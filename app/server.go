package main

import (
	"flag"
	"fmt"
	"net"
	"strings"
	"time"
)

func main() {
	var port int

	// Define the port flag
	flag.IntVar(&port, "port", 6379, "Port number to listen on")
	flag.Parse()

	// Listen for incoming connections
	listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	// Ensure we teardown the server when the program exits
	defer listener.Close()

	fmt.Println(fmt.Sprintf("Server is listening on port %d", port))

	for {
		// Block until we receive an incoming connection
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error:", err)
			continue
		}

		// Handle client connection
		go handleClient(conn)
	}
}

func handleClient(conn net.Conn) {
	for {
		// Read data
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			fmt.Printf("failed to read data")
			return
		}
		now := time.Now()

		rawStr := string(buf[:n])

		strs, err := parseString(rawStr)
		if err != nil {
			fmt.Printf("failed to read data %+v", err)
			return
		}
		fmt.Printf("got %q", strs)

		command := strings.ToLower(strs[0])

		var reply string
		switch command {
		case "ping":
			reply = "PONG"
			conn.Write([]byte(fmt.Sprintf("+%s\r\n", reply)))
			break
		case "echo":
			reply = strs[1]
			conn.Write([]byte(fmt.Sprintf("+%s\r\n", reply)))
			break
		case "set":
			handleSet(now, strs[1:])
			reply = "OK"
			conn.Write([]byte(fmt.Sprintf("+%s\r\n", reply)))
			break
		case "get":
			resp, ok := handleGet(now, strs[1])
			if ok {
				reply = resp
				conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(reply), reply)))
			} else {
				reply = "-1"
				conn.Write([]byte(fmt.Sprintf("$%s\r\n", reply)))
			}
			break
		}
	}

}
