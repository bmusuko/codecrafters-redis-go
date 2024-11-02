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
		if _metaInfo.isMulti[conn.RemoteAddr().String()] {
			_metaInfo.pendingTxn[conn.RemoteAddr().String()] = append(_metaInfo.pendingTxn[conn.RemoteAddr().String()], strings.Join(strs, " "))
			conn.Write([]byte(fmt.Sprintf("+QUEUED\r\n")))
			return
		}
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
		if _metaInfo.isMulti[conn.RemoteAddr().String()] {
			_metaInfo.pendingTxn[conn.RemoteAddr().String()] = append(_metaInfo.pendingTxn[conn.RemoteAddr().String()], strings.Join(strs, " "))
			conn.Write([]byte(fmt.Sprintf("+QUEUED\r\n")))
			return
		}
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
		if _metaInfo.isMulti[conn.RemoteAddr().String()] {
			_metaInfo.pendingTxn[conn.RemoteAddr().String()] = append(_metaInfo.pendingTxn[conn.RemoteAddr().String()], strings.Join(strs, " "))
			conn.Write([]byte(fmt.Sprintf("+QUEUED\r\n")))
			return
		}
		res, ok := handleIncr(strs[1])
		if !ok {
			conn.Write([]byte("-ERR value is not an integer or out of range\r\n"))
		} else {
			response := fmt.Sprintf(":%d\r\n", res)
			conn.Write([]byte(response))
		}
	case "multi":
		_metaInfo.isMulti[conn.RemoteAddr().String()] = true
		response := fmt.Sprintf("+OK\r\n")
		conn.Write([]byte(response))
	case "discard":
		if !_metaInfo.isMulti[conn.RemoteAddr().String()] {
			response := fmt.Sprintf("-ERR DISCARD without MULTI\r\n")
			conn.Write([]byte(response))
			return
		}
		conn.Write([]byte("+OK\r\n"))
		_metaInfo.isMulti[conn.RemoteAddr().String()] = false
		_metaInfo.pendingTxn[conn.RemoteAddr().String()] = nil
	case "exec":
		if !_metaInfo.isMulti[conn.RemoteAddr().String()] {
			response := fmt.Sprintf("-ERR EXEC without MULTI\r\n")
			conn.Write([]byte(response))
			return
		}
		res := handleExec(conn)
		conn.Write([]byte(fmt.Sprintf("%s", res)))
		_metaInfo.isMulti[conn.RemoteAddr().String()] = false
		_metaInfo.pendingTxn[conn.RemoteAddr().String()] = nil
	case "type":
		res := handleType(strs[1])
		conn.Write([]byte(fmt.Sprintf("+%s\r\n", res)))
	case "xadd":
		val, res := handleXAdd(strs[1], strs[2], strs[3:])
		if !val {
			conn.Write([]byte(fmt.Sprintf("%s\r\n", res)))
			return
		}
		if strs[2] == "*" {
			conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(res), res)))
			return
		}
		conn.Write([]byte(fmt.Sprintf("+%s\r\n", res)))
	case "xrange":
		resp := handleXRange(strs[1], strs[2], strs[3])
		conn.Write([]byte(fmt.Sprintf("%s", resp)))
	case "xread":
		resp := handleXRead(strs[1:])
		conn.Write([]byte(fmt.Sprintf("%s", resp)))
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

func handleExec(conn net.Conn) string {
	var respArr []string

	pendingTxns := _metaInfo.pendingTxn[conn.RemoteAddr().String()]
	for _, pendingTxn := range pendingTxns {
		strs := strings.Split(pendingTxn, " ")

		// very redundant, but that's non-prod code works
		// only refactor if needed, and worth the effort

		command := strings.ToLower(strs[0])

		switch command {
		case "get":
			resp, ok := handleGet(time.Now(), strs[1])
			if ok {
				respArr = append(respArr, fmt.Sprintf("$%d\r\n%s\r\n", len(resp), resp))
			} else {
				reply := "-1"
				respArr = append(respArr, fmt.Sprintf("$%s\r\n", reply))
			}
		case "set":
			handleSet(time.Now(), strs[1:])
			respArr = append(respArr, "+OK\r\n")
		case "incr":
			res, ok := handleIncr(strs[1])
			if !ok {
				respArr = append(respArr, "-ERR value is not an integer or out of range\r\n")
			} else {
				response := fmt.Sprintf(":%d\r\n", res)
				respArr = append(respArr, response)
			}
		default:
			fmt.Printf("unhandled %s\n", command)
		}

	}

	ans := fmt.Sprintf("*%d\r\n", len(respArr))

	return ans + strings.Join(respArr, "")
}

func handleType(key string) string {
	_, ok := handleGet(time.Now(), key)
	if ok {
		return "string"
	}

	_, ok = _metaInfo.stream[key]
	if ok {
		return "stream"
	}

	return "none"
}

func handleXAdd(key string, id string, vals []string) (bool, string) {
	// validate key
	if id == "0-0" {
		return false, "-ERR The ID specified in XADD must be greater than 0-0"
	}

	var sequence int
	var timestamp int64
	var err error

	if id == "*" {
		timestamp = time.Now().UnixMilli()
		sequence = 0
	} else {
		parts := strings.Split(id, "-")
		if len(parts) != 2 {
			return false, "invalid length"
		}
		timestamp, err = strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			return false, err.Error()
		}
		if parts[1] == "*" {
			if timestamp == _metaInfo.lastStreamMS {
				sequence = _metaInfo.lastStreamSequence + 1
			} else {
				sequence = 0
			}
		} else {
			sequence, err = strconv.Atoi(parts[1])
			if err != nil {
				return false, err.Error()
			}
		}
	}

	if (timestamp < _metaInfo.lastStreamMS) || (timestamp == _metaInfo.lastStreamMS && sequence <= _metaInfo.lastStreamSequence) {
		return false, "-ERR The ID specified in XADD is equal or smaller than the target stream top item"
	}
	_metaInfo.lastStreamMS = timestamp
	_metaInfo.lastStreamSequence = sequence

	id = fmt.Sprintf("%d-%d", timestamp, sequence)

	_metaInfo.stream[key] = append(_metaInfo.stream[key], stream{
		id:    id,
		value: vals,
	})

	return true, id
}

type xRead struct {
	key  string
	from string
}

func handleXRead(args []string) string {
	var blockTime time.Duration
	var isBlock bool

	if args[0] == "block" {
		slp, err := strconv.Atoi(args[1])
		if err != nil {
			return ""
		}
		blockTime = time.Duration(slp) * time.Millisecond
		args = args[3:]
		isBlock = true
	} else {
		args = args[1:]
	}
	data := getXReadArg(args)

	if isBlock {
		fmt.Printf("blocking")
		if blockTime != 0 {
			fmt.Printf("non-zero blocking")
			time.Sleep(blockTime)
		} else {
			fmt.Printf("zero blocking")
			for {
				var isExist bool

				for _, d := range data {
					timestamp, sequence := parseID(d.from)
					newFrom := fmt.Sprintf("%d-%d", timestamp, sequence+1)

					arr := getStreamData(d.key, newFrom, "+")
					fmt.Printf("get key=%s, from=%s, res=%d", d.key, d.from, len(arr))
					if len(arr) > 0 {
						isExist = true
						break
					}
				}

				if isExist {
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
		}

	}

	var resps []string
	for _, d := range data {
		key := d.key
		from := d.from

		var ans string

		timestamp, sequence := parseID(from)
		newFrom := fmt.Sprintf("%d-%d", timestamp, sequence+1)
		arr := getStreamData(key, newFrom, "+")
		if len(arr) == 0 {
			continue
		}

		ans += fmt.Sprintf("*2\r\n")
		ans += fmt.Sprintf("$%d\r\n%s\r\n", len(key), key)
		ans += fmt.Sprintf("*%d\r\n", len(arr))
		for _, item := range arr {
			ans += "*2\r\n"
			ans += fmt.Sprintf("$%d\r\n%s\r\n", len(item.id), item.id)
			ans += fmt.Sprintf("*%d\r\n", len(item.value))

			for _, val := range item.value {
				ans += fmt.Sprintf("$%d\r\n%s\r\n", len(val), val)
			}
		}
		resps = append(resps, ans)
	}
	if len(resps) == 0 {
		return "$-1\r\n"
	}
	ans := fmt.Sprintf("*%d\r\n", len(resps))
	for _, resp := range resps {
		ans += resp
	}

	fmt.Printf("ans: %s\n", strconv.Quote(ans))

	return ans
}

func getXReadArg(args []string) []xRead {
	var data []xRead

	for i := 1; i <= len(args)/2; i++ {
		idx := i - 1
		key := args[idx]
		from := args[idx+(len(args)/2)]
		fmt.Printf("key: %s, from: %s\n", key, from)

		arg := xRead{
			key: key,
		}

		if from != "$" {
			arg.from = from
		} else {
			newFrom := "0-0"
			arr := getStreamData(key, "-", "+")
			if len(arr) > 0 {
				newFrom = arr[len(arr)-1].id
			}
			arg.from = newFrom

		}
		data = append(data, arg)
		fmt.Printf("parse: %+v\n", arg)
	}
	return data
}

func handleXRange(key, from, to string) string {
	arr := getStreamData(key, from, to)
	ans := fmt.Sprintf("*%d\r\n", len(arr))
	for _, item := range arr {
		ans += "*2\r\n"
		ans += fmt.Sprintf("$%d\r\n%s\r\n", len(item.id), item.id)
		ans += fmt.Sprintf("*%d\r\n", len(item.value))

		for _, val := range item.value {
			ans += fmt.Sprintf("$%d\r\n%s\r\n", len(val), val)
		}

	}

	return ans
}

func getStreamData(key, from, to string) []stream {
	arr := make([]stream, 0)

	all := _metaInfo.stream[key]
	for _, item := range all {
		if isInRange(item.id, from, to) {
			arr = append(arr, item)
		}
	}

	return arr
}

func isInRange(id, from, to string) bool {
	timestamp, sequence := parseID(id)
	fromTimestamp, fromSequence := parseID(from)
	toTimestamp, toSequence := parseID(to)

	// from
	if (from != "-") && ((fromTimestamp > timestamp) || (fromTimestamp == timestamp && fromSequence > sequence)) {
		return false
	}

	// to
	if (to != "+") && ((toTimestamp < timestamp) || (toTimestamp == timestamp && toSequence < sequence)) {
		return false
	}

	return true
}

func parseID(id string) (timestamp int64, sequence int) {
	var err error

	if !strings.Contains(id, "-") {
		timestamp, err = strconv.ParseInt(id, 10, 64)
		if err != nil {
			return 0, 0
		}
		return timestamp, 0
	}

	parts := strings.Split(id, "-")
	if len(parts) != 2 {
		return 0, 0
	}

	timestamp, err = strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0
	}

	sequence, err = strconv.Atoi(parts[1])
	if err != nil {
		return 0, 0
	}
	return timestamp, sequence
}
