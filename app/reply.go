package main

import (
	"fmt"
	"net"
)

func sendBulkString(conn net.Conn, strs []string) {
	for _, str := range strs {
		conn.Write([]byte(fmt.Sprintf("+%s\r\n", str)))
	}
}
