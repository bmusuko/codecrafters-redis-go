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
	value      string
	withExpire bool
	expireAt   time.Time
}

var _map sync.Map
var (
	ackReceived = make(chan bool)
)

func handleCommand(conn net.Conn, rawStr string) {
	rawBuf := []byte(rawStr)
	strs, err := parseString(rawStr)
	if err != nil {
		fmt.Printf("failed to read data %+v\n", err)
		return
	}
	fmt.Printf("localhost:%d got %q\n", _metaInfo.port, strs)

	command := strings.ToLower(strs[0])
	byteLen := len(rawBuf)

	now := time.Now()
	var reply string
	var shouldUpdateByte bool
	switch command {
	case "ping":
		if _metaInfo.isMaster() {
			reply = "PONG"
			conn.Write([]byte(fmt.Sprintf("+%s\r\n", reply)))
		}
		shouldUpdateByte = true
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

			handleBroadcast(rawBuf, now.UnixMilli())
		}
		shouldUpdateByte = true
		_metaInfo.startSet.Store(true)
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
		if len(strs) == 3 && strs[1] == "GETACK" && strs[2] == "*" {
			length := fmt.Sprintf("%d", _metaInfo.processedBytes.Load())
			conn.Write([]byte(fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$%d\r\n%s\r\n", len(length), length)))
		} else if len(strs) == 3 && strs[1] == "ACK" {
			fmt.Printf("thx for ack %s \n", conn.RemoteAddr().String())
			ackReceived <- true
		} else {
			reply = "OK"
			conn.Write([]byte(fmt.Sprintf("+%s\r\n", reply)))
		}
		shouldUpdateByte = true
		break
	case "psync":
		conn.Write([]byte(fmt.Sprintf("+FULLRESYNC %s %d\r\n", _metaInfo.masterReplID, *_metaInfo.masterReplOffset)))
		time.Sleep(100 * time.Millisecond)
		fullByte := getEmptyRDBByte()
		conn.Write([]byte(fmt.Sprintf("$%d\r\n%s", len(fullByte), fullByte)))

		_metaInfo.addSlave(conn)
	case "wait":
		go handleWait(conn, strs[1], strs[2])
	case "config":
		if strs[2] == "dir" {
			conn.Write([]byte(fmt.Sprintf("*2\r\n$3\r\ndir\r\n$%d\r\n%s\r\n", len(_metaInfo.dir), _metaInfo.dir)))
		} else if strs[2] == "dbfilename" {
			conn.Write([]byte(fmt.Sprintf("*2\r\n$10\r\ndbfilename\r\n$%d\r\n%s\r\n", len(_metaInfo.dbFileName), _metaInfo.dbFileName)))
		}
	case "keys":
		_keys := handleKeys()
		response := fmt.Sprintf("*%d\r\n", len(_keys))
		for _, k := range _keys {
			response = fmt.Sprintf("%s$%d\r\n%s\r\n", response, len(k), k)
		}
		conn.Write([]byte(response))
	case "incr":
		res, ok := handleIncr(strs[1])
		if !ok {
			conn.Write([]byte("-ERR value is not an integer or out of range\r\n"))
		} else {
			response := fmt.Sprintf(":%d\r\n", res)
			conn.Write([]byte(response))
		}
	case "multi":
		response := fmt.Sprintf("+OK\r\n")
		conn.Write([]byte(response))
	}
	if !_metaInfo.isMaster() && shouldUpdateByte {
		_metaInfo.processedBytes.Add(int32(byteLen))
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
			stored.withExpire = true
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
	if expireAt := stored.expireAt; stored.withExpire && expireAt.Before(now) {
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

func handleWait(conn net.Conn, replicaStr, waitMSStr string) {
	for _, slave := range _metaInfo.slaves {
		go func(_slave net.Conn) {
			_slave.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"))
		}(slave)
	}

	replica, _ := strconv.Atoi(replicaStr)
	waitMS, _ := strconv.Atoi(waitMSStr)

	timer := time.After(time.Duration(waitMS) * time.Millisecond)
	ackNum := 0
	if !_metaInfo.startSet.Load() {
		conn.Write([]byte(fmt.Sprintf(":%d\r\n", len(_metaInfo.slaves))))
		return
	}
	for ackNum < replica {
		select {
		case <-ackReceived:
			fmt.Printf("received ack\n")
			ackNum++
		case <-timer:
			fmt.Printf("timeout reached %d\n", waitMS)
			conn.Write([]byte(fmt.Sprintf(":%d\r\n", ackNum)))
			return
		}
	}
	conn.Write([]byte(fmt.Sprintf(":%d\r\n", ackNum)))
	return
}

func handleKeys() []string {
	_keys := make([]string, 0)
	_map.Range(func(k, v interface{}) bool {
		key, ok := k.(string)
		if !ok {
			// Handle the case where the key is not a string
			return true
		}
		value, ok := v.(store)
		if !ok {
			return true
		}
		if value.withExpire && value.expireAt.Before(time.Now()) {
			return true
		}

		_keys = append(_keys, key)
		return true
	})
	return _keys
}

func handleIncr(key string) (int64, bool) {
	res, ok := _map.Load(key)
	if !ok {
		_map.Store(key, store{
			value: "1",
		})

		return 1, true
	}

	value, ok := res.(store)
	if !ok {
		return 0, false
	}

	if value.withExpire && value.expireAt.Before(time.Now()) {
		return 0, false
	}

	intValue, err := strconv.Atoi(value.value)
	if err != nil {
		fmt.Printf("err := %+v\n", err)
		return 0, false
	}

	newValue := value
	newValue.value = strconv.Itoa(intValue + 1)
	_map.Store(key, newValue)
	return int64(intValue + 1), true
}
