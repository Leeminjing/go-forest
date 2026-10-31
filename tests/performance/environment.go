package performance

import (
	"os"
	"strconv"
)

func dimension(key string, fallback int) int {
	if v := os.Getenv("FOREST_PERF_" + key); v != "" {
		n, e := strconv.Atoi(v)
		if e != nil {
			panic(e)
		}
		return n
	}
	return fallback
}
func config() Config {
	return Config{dimension("N", 32), dimension("E", 16), dimension("D", 1), dimension("PORTS", 1), os.Getenv("FOREST_PERF_MANY") == "1"}
}
