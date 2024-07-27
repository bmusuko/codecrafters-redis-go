package main

import (
	"fmt"
	"net"
	"os"
)

func handshake() {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", _metaInfo.masterHost, _metaInfo.masterPort))
	if err != nil {
		os.Exit(-1)
	}

	// send PING
	_, err = conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	if err != nil {
		os.Exit(-1)
	}
}
