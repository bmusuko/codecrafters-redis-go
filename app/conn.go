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
		rawStr := string(rawBuf)

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
			if strs[1] == "listening-port" {
				slaveAddr := fmt.Sprintf("0.0.0.0:%s", strs[2])
				_metaInfo.addSlave(slaveAddr)
				fmt.Printf("add slave %s\n", slaveAddr)
			}

			break
		case "psync":
			conn.Write([]byte(fmt.Sprintf("+FULLRESYNC %s %d\r\n", _metaInfo.masterReplID, *_metaInfo.masterReplOffset)))
			time.Sleep(100 * time.Millisecond)
			fullByte := getEmptyRDBByte()
			conn.Write([]byte(fmt.Sprintf("$%d\r\n%s", len(fullByte), fullByte)))
		}
	}
}
