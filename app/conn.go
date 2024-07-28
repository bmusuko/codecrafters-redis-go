package main

import (
	"fmt"
	"net"
	"os"
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

		if command == "set" {
			defer func() {
				handleBroadcast(rawBuf)
			}()
		}

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

			slaveAddr := getClientAddress(conn)
			_metaInfo.addSlave(slaveAddr)
			fmt.Printf("add slave %s", slaveAddr)
		}
	}
}

// getClientAddress takes a net.Conn and returns the client's IP address and port as a formatted string.
func getClientAddress(conn net.Conn) string {
	// Get the remote address from the connection
	remoteAddr := conn.RemoteAddr()

	// Convert the remote address to a TCP address
	tcpAddr, ok := remoteAddr.(*net.TCPAddr)
	if !ok {
		os.Exit(-1)
	}

	// Extract the IP address and port from the TCP address
	clientIP := tcpAddr.IP.String()
	clientPort := tcpAddr.Port

	// Format the IP address and port into a string
	address := fmt.Sprintf("%s:%d", clientIP, clientPort)
	return address
}
