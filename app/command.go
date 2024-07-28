package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type store struct {
	value    string
	expireAt time.Time
}

var _map sync.Map

func handleCommand(conn net.Conn, strs []string, rawBuf []byte) {
	command := strings.ToLower(strs[0])

	now := time.Now()
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
			handleBroadcast(rawBuf)
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

		_metaInfo.addSlave(conn)
	}
}

func handleSet(now time.Time, strs []string) {
	key := strs[0]
	value := strs[1]

	stored := store{
		value: value,
	}

	if len(strs) > 2 {
		switch strings.ToLower(strs[2]) {
		case "px":
			ms, err := strconv.Atoi(strs[3])
			if err != nil {
				os.Exit(-1)
			}
			stored.expireAt = now.Add(time.Millisecond * time.Duration(ms))
		}
	}

	_map.Store(key, stored)
}

func handleGet(now time.Time, key string) (string, bool) {
	value, ok := _map.Load(key)
	if !ok {
		return "", false
	}
	stored, ok := value.(store)
	if !ok {
		return "", false
	}
	if expireAt := stored.expireAt; !expireAt.IsZero() && expireAt.Before(now) {
		return "", false
	}

	return stored.value, true
}

func handleInfo() []string {
	var reply []string

	if _metaInfo.port == 6379 {
		reply = append(reply, "role:master")
	} else {
		reply = append(reply, "role:slave")
	}

	if len(_metaInfo.masterReplID) > 0 {
		reply = append(reply, fmt.Sprintf("master_replid:%s", _metaInfo.masterReplID))
	}
	if _metaInfo.masterReplOffset != nil {
		reply = append(reply, fmt.Sprintf("master_repl_offset:%d", *_metaInfo.masterReplOffset))
	}

	return reply
}
