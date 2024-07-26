package main

import (
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

func handleSet(now time.Time, strs []string) {
	key := strs[0]
	value := strs[1]

	stored := store{
		value: value,
	}

	if len(strs) > 2 {
		switch strings.ToLower(strs[3]) {
		case "px":
			ms, err := strconv.Atoi(strs[4])
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
