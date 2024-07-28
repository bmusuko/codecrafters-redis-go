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

		rawBuf := buf[:n]
		strs, err := parseString(string(rawBuf))
		if err != nil {
			fmt.Printf("failed to read data %+v", err)
			return
		}
		fmt.Printf("localhost:%d got %q", _metaInfo.port, strs)

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
			if _metaInfo.isMaster() {
				reply = "OK"
				conn.Write([]byte(fmt.Sprintf("+%s\r\n", reply)))
				handleBroadcast(rawBuf)
			}
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
			conn.Write([]byte(fmt.Sprintf("+FULLRESYNC %s %d\r\n", _metaInfo.masterReplID, *_metaInfo.masterReplOffset)))
			time.Sleep(100 * time.Millisecond)
			fullByte := getEmptyRDBByte()
			conn.Write([]byte(fmt.Sprintf("$%d\r\n%s", len(fullByte), fullByte)))

			_metaInfo.addSlave(conn)
		default:
			continue
		}
	}
}
