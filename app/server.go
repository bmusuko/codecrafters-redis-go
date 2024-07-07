package main

import (
	"fmt"
	"net"
	"strings"
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

		trimmed := strings.TrimSuffix(string(buf[:n]), "\n")
		strs := strings.Split(trimmed, " ")
		command := strings.ToLower(strs[0])

		var reply string
		switch command {
		case "ping":
			reply = "PONG"
			break
		case "echo":
			reply = strs[1]
			break
		}

		conn.Write([]byte(fmt.Sprintf("+%s\r\n", reply)))
	}

}
