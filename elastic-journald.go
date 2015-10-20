package main

import (
	"flag"
)

var (
	host   string
	prefix string
	state  string
)

func init() {
	flag.StringVar(&host, "host", "http://localhost:9200", "comma-separated list of elastic (target) hosts")
	flag.StringVar(&prefix, "prefix", "journald", "The index prefix to use")
	flag.StringVar(&state, "state", ".elastic_journal_cursor", "The file to keep cursor state between runs")
	flag.Parse()
}

func main() {
	NewService(host, prefix, state).Run()
}
