package main

import (
	"fmt"
	"net"
	"strconv"
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

		rawBuf := buf[:n]
		rawStr := string(rawBuf)
		fmt.Printf("raw str %s\n", strconv.Quote(rawStr))

		strs, err := parseString(rawStr)
		if err != nil {
			fmt.Printf("failed to read data %+v", err)
			continue
		}
		fmt.Printf("localhost:%d got %q\n", _metaInfo.port, strs)

		handleCommand(conn, strs, buf)
	}
}
