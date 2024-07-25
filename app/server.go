package main

import (
	"fmt"
	"net"
	"strings"
	"sync"
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

var _map sync.Map

func handleSet(key, value string) {
	_map.Store(key, value)
}

func handleGet(key string) (string, bool) {
	value, ok := _map.Load(key)
	if !ok {
		return "", false
	}
	str, ok := value.(string)
	if !ok {
		return "", false
	}
	return str, true
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

		rawStr := string(buf[:n])

		fmt.Printf("got %s", rawStr)
		strs, err := parseString(rawStr)
		if err != nil {
			fmt.Printf("failed to read data %+v", err)
			return
		}
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
			handleSet(strs[1], strs[2])
			reply = "OK"
		case "get":
			resp, ok := handleGet(strs[1])
			if ok {
				reply = resp
			} else {
				reply = "-1"
			}
		}

		conn.Write([]byte(fmt.Sprintf("+%s\r\n", reply)))
	}

}
