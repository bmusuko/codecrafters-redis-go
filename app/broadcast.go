package main

import (
	"fmt"
	"os"
	"time"
)

func handleBroadcast(rawBuf []byte) {
	for _, slave := range _metaInfo.slaves {
		// send raw buf
		_, err := slave.Write(rawBuf)
		if err != nil {
			fmt.Printf("failed to send broadcast %s", string(rawBuf))
			os.Exit(-1)
		}

		time.Sleep(100 * time.Millisecond)
	}
}
