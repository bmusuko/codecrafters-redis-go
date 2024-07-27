package main

import (
	"fmt"
	"net"
	"os"
)

func handshake() {
	fmt.Printf("master_host=%s", fmt.Sprintf("%s:%d", _metaInfo.masterHost, _metaInfo.masterPort))
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", _metaInfo.masterHost, _metaInfo.masterPort))
	if err != nil {
		fmt.Printf("failed to dial master")
		os.Exit(-1)
	}

	defer func() {
		conn.Close()
	}()

	// send PING
	_, err = conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	if err != nil {
		fmt.Printf("failed to send ping")
		os.Exit(-1)
	}
}
