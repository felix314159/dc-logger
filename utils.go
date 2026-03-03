package main

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

func normalizeTimestamp(ts any) string {
	switch v := ts.(type) {
	case time.Time:
		if v.IsZero() {
			return time.Now().UTC().Format(time.RFC3339Nano)
		}
		return v.UTC().Format(time.RFC3339Nano)
	case string:
		if v == "" {
			return time.Now().UTC().Format(time.RFC3339Nano)
		}
		return v
	default:
		s := fmt.Sprint(ts)
		if s == "" || s == "<nil>" {
			return time.Now().UTC().Format(time.RFC3339Nano)
		}
		return s
	}
}

func snowflakeGreater(a, b string) bool {
	if b == "" {
		return a != ""
	}
	au, errA := strconv.ParseUint(a, 10, 64)
	bu, errB := strconv.ParseUint(b, 10, 64)
	if errA != nil || errB != nil {
		return a > b
	}
	return au > bu
}

func getenvDefault(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
