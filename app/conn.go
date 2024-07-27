package main

import (
	"fmt"
	"net"
	"strings"
	"time"
)

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
		case "info":
			replies := handleInfo()
			sendBulkString(conn, replies)
			break
		case "replconf":
			reply = "OK"
			conn.Write([]byte(fmt.Sprintf("+%s\r\n", reply)))
			break
		case "psync":
			conn.Write([]byte(fmt.Sprintf("+FULLRESYNC %s 0\r\n", _metaInfo.masterReplID)))
		}
	}

}
