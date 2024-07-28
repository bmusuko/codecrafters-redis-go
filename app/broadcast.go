package main

import (
	"fmt"
	"net"
	"os"
	"time"
)

func handleBroadcast(rawBuf []byte) {
	for _, slave := range _metaInfo.slaves {
		conn, err := net.Dial("tcp", slave)
		if err != nil {
			fmt.Printf("failed to dial slave")
			os.Exit(-1)
		}

		// send PING
		_, err = conn.Write(rawBuf)
		if err != nil {
			fmt.Printf("failed to send ping")
			os.Exit(-1)
		}

		time.Sleep(100 * time.Millisecond)
		conn.Close()
	}
}
