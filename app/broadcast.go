package main

import (
	"fmt"
	"net"
	"os"
	"time"
)

func handleBroadcast(rawBuf []byte) {
	for _, slave := range _metaInfo.slaves {
		fmt.Printf("send %s to slave=%s\n", string(rawBuf), slave)
		conn, err := net.Dial("tcp", slave)
		if err != nil {
			fmt.Printf("failed to dial slave")
			os.Exit(-1)
		}
		time.Sleep(100 * time.Millisecond)

		// send raw buf
		_, err = conn.Write(rawBuf)
		if err != nil {
			fmt.Printf("failed to send broadcast %s", string(rawBuf))
			os.Exit(-1)
		}

		time.Sleep(100 * time.Millisecond)
		conn.Close()
	}
}
