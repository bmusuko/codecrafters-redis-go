package main

import (
	"fmt"
	"net"
	"regexp"
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
			fmt.Printf("parsed command %q\n", strconv.Quote(command))
			handleCommand(conn, command)
		}
	}
}

// splitCommand splits the input string only if '*' is followed by a number
func splitCommand(rawStr string) []string {
	var result []string

	// Regular expression to match "*<number>" pattern
	re := regexp.MustCompile(`\*(\d+)`)

	// Find all matches of the pattern
	matches := re.FindAllStringIndex(rawStr, -1)

	if len(matches) == 0 {
		// No valid '*' followed by a number; return the original string
		return []string{rawStr}
	}

	// Split the rawStr into parts based on the positions of the valid '*' patterns
	start := 0
	for _, match := range matches {
		// Extract the part between the last match and the current match
		if start < match[0] {
			result = append(result, rawStr[start:match[0]])
		}
		// Include the match itself (i.e., "*<number>")
		result = append(result, rawStr[match[0]:match[1]])
		start = match[1]
	}
	// Append the last part after the last match
	if start < len(rawStr) {
		result = append(result, rawStr[start:])
	}

	// Remove any empty strings from the result
	for i, part := range result {
		result[i] = strings.TrimSpace(part)
	}

	// Clean up any empty strings that might be the result of trimming
	var cleanedResult []string
	for _, part := range result {
		if len(part) > 0 {
			cleanedResult = append(cleanedResult, part)
		}
	}

	return cleanedResult
}
