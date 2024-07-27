package main

import (
	"flag"
	"os"
	"strconv"
	"strings"
)

type metaInfo struct {
	port             int
	masterHost       string
	masterPort       int
	masterReplID     string
	masterReplOffset *int
}

var (
	_metaInfo metaInfo
)

func initMeta() {
	// Define the port flag
	flag.IntVar(&_metaInfo.port, "port", 6379, "Port number to listen on")

	replicaOf := ""
	flag.StringVar(&replicaOf, "replicaof", "", "Replication info")
	if len(replicaOf) > 0 {
		replicaInfo := strings.Split(replicaOf, " ")
		_metaInfo.masterHost = replicaInfo[0]
		masterPort, err := strconv.Atoi(replicaInfo[1])
		if err != nil {
			os.Exit(-1)
		}
		_metaInfo.masterPort = masterPort
	} else {
		_metaInfo.masterReplID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
		_metaInfo.masterReplOffset = ptr(0)
	}

	flag.Parse()
}
