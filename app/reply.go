package main

import (
	"fmt"
	"net"
	"strings"
)

func sendBulkString(conn net.Conn, strs []string) {
	str := strings.Join(strs, "\r\n")
	conn.Write([]byte(fmt.Sprintf("+%s\r\n", str)))
}
