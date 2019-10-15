package main

import (
	"bufio"
	"os"
	"strings"
)

func readWorkerID() string {
	f, err := os.Open("/edge/init")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		s := scanner.Text()
		if strings.HasPrefix(s, "workid:") {
			return strings.TrimSpace(s[7:])
		}
	}

	return ""
}
