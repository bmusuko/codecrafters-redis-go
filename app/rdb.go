package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"time"

	"golang.org/x/exp/slices"
)

type keys struct {
	key        string
	valueType  int
	value      string
	expireTime uint64
	isMS       bool
	withExpire bool
}

func initRDB(path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		fmt.Printf("err reading file")
		return
	}
	startData := slices.Index(data, byte('\xfe'))
	endData := slices.Index(data, byte('\xff'))

	// parse database
	database := data[startData:endData]
	fmt.Printf("%q\n", database)
	dbs := parseDB(database)

	for _, k := range dbs {
		key := k.key
		stored := store{
			value: k.value,
		}

		if k.expireTime != 0 {
			if k.isMS {
				stored.expireAt = time.UnixMilli(int64(k.expireTime))
			} else {
				stored.expireAt = time.Unix(int64(k.expireTime), 0)
			}
			stored.withExpire = true
		}

		_map.Store(key, stored)
	}

	return
}

func parseDB(database []byte) []keys {
	length := int(database[3])
	fmt.Printf("num of keys: %d\n", length)

	i := 5 // start to get data
	var ans []keys
	for i < len(database) {
		curr := keys{}
		if database[i] == byte('\xFD') {
			i += 1
			expiry := binary.LittleEndian.Uint32(database[i : i+4])
			curr.isMS = false
			curr.expireTime = uint64(expiry)
			curr.withExpire = true
			i += 4
		} else if database[i] == byte('\xFC') {
			i += 1
			expiry := binary.LittleEndian.Uint64(database[i : i+8])
			curr.isMS = true
			curr.expireTime = expiry
			curr.withExpire = true
			i += 8
		}

		// type
		if database[i] == 0 {
			i += 1
			curr.valueType = 0
		}

		// key
		keyLen := int(database[i])
		i += 1
		key := string(database[i : i+keyLen])
		i += keyLen
		curr.key = key

		// value
		valueLen := int(database[i])
		i += 1
		value := string(database[i : i+valueLen])
		i += valueLen
		curr.value = value

		ans = append(ans, curr)
	}
	return ans
}
