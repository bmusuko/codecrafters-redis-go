package main

import (
	"fmt"
	"net"
	"strconv"
	"strings"
)

func handleClient(conn net.Conn) {
	for {
		// Read data
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			fmt.Printf("failed to read data\n")
			return
		}

		rawStr := string(buf[:n])
		fmt.Printf("raw str %s\n", strconv.Quote(rawStr))

		// can be multiple command
		commands := splitCommand(rawStr)
		for _, command := range commands {
			handleCommand(conn, command)
		}
	}
}

func splitCommand(rawStr string) []string {
	if !strings.Contains(rawStr, "*") {
		return []string{rawStr}
	}
	commands := strings.Split(rawStr, "*")
	for i, command := range commands {
		commands[i] = "*" + command
	}
	return commands[1:]
}
