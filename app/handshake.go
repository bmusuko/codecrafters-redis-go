package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
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

	// send REPLCONF
	_, err = conn.Write([]byte(fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$%d\r\n%d\r\n", len(strconv.Itoa(_metaInfo.masterPort)), _metaInfo.port)))
	if err != nil {
		fmt.Printf("failed to send first REPLCONF")
		os.Exit(-1)
	}

	_, err = conn.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"))
	if err != nil {
		fmt.Printf("failed to send second REPLCONF")
		os.Exit(-1)
	}
}
