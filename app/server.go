package main

import (
	"fmt"
	"net"
	"strings"
	"time"
)

func main() {
	// Listen for incoming connections
	listener, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	// Ensure we teardown the server when the program exits
	defer listener.Close()

	fmt.Println("Server is listening on port 6379")

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
			break
		case "echo":
			reply = strs[1]
			break
		case "set":
			handleSet(now, strs[1:])
			reply = "OK"
		case "get":
			resp, ok := handleGet(now, strs[1])
			if ok {
				reply = resp
			} else {
				reply = "-1"
			}
		}

		conn.Write([]byte(fmt.Sprintf("+%s\r\n", reply)))
	}

}
