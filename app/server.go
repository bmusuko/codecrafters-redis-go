package main

import (
	"fmt"
	"log"
	"net"
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
		handleClient(conn)
	}
}

func handleClient(conn net.Conn) {
	// Read data
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		return
	}

	log.Println("Received data", buf[:n])

	// Write the same data back
	conn.Write([]byte("+PONG\r\n"))
}
