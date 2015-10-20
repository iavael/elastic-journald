package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os/exec"
	"strings"
	"time"
	"unsafe"

	"github.com/mattbaird/elastigo/lib"
)

// #include <stdio.h>
// #include <string.h>
// #include <systemd/sd-journal.h>
// #cgo LDFLAGS: -lsystemd-journal
import "C"

type Config struct {
	Host   string
	Prefix string
	State  string
}

type Service struct {
	Config  *Config
	Journal *C.sd_journal
	Cursor  string
	Elastic *elastigo.Conn
	Indexer *elastigo.BulkIndexer
}

func NewService(host string, prefix string, state string) *Service {
	config := &Config{
		Host:   host,
		Prefix: prefix,
		State:	state,
	}

	elastic := elastigo.NewConn()
	indexer := elastic.NewBulkIndexerErrors(2, 30)
	indexer.BufferDelayMax = time.Duration(30) * time.Second
	indexer.BulkMaxDocs = 1000
	indexer.BulkMaxBuffer = 65536
	indexer.Sender = func(buf *bytes.Buffer) error {
		respJson, err := elastic.DoCommand("POST", "/_bulk", nil, buf)
		if err != nil {
			// TODO
			log.Fatalf("Bulk error: \n%v\n", err)
		} else {
			response := struct {
				Took   int64 `json:"took"`
				Errors bool  `json:"errors"`
				Items  []struct {
					Index struct {
						Id string `json:"_id"`
					} `json:"index"`
				} `json:"items"`
			}{}

			jsonErr := json.Unmarshal(respJson, &response)
			if jsonErr != nil {
				// TODO
				log.Fatalln(jsonErr)
			}
			if response.Errors {
				// TODO
				log.Fatalf("elasticsearch reported errors on intake: %s\n", respJson)
			}

			messagesStored := len(response.Items)
			lastStoredCursor := response.Items[messagesStored-1].Index.Id
			ioutil.WriteFile(config.State, []byte(lastStoredCursor), 0644)

			// this will cause a busy loop, don't do it
			// fmt.Printf("Sent %v entries\n", messagesStored)
		}
		return err
	}

	service := &Service{
		Config:  config,
		Elastic: elastic,
		Indexer: indexer,
	}
	return service
}

func (s *Service) Run() {
	s.Elastic.SetFromUrl(s.Config.Host)

	s.InitJournal()
	s.ProcessStream(GetFQDN())
}

func (s *Service) ProcessStream(hostname *string) {
	s.Indexer.Start()
	defer s.Indexer.Stop()

	for {
		r := C.sd_journal_next(s.Journal)
		if r < 0 {
			log.Fatalf("failed to iterate to next entry: %s\n", C.strerror(-r))
		}
		if r == 0 {
			r = C.sd_journal_wait(s.Journal, 1000000)
			if r < 0 {
				log.Fatalf("failed to wait for changes: %s\n", C.strerror(-r))
			}
			continue
		}
		s.ProcessEntry(hostname)
	}
}

func (s *Service) ProcessEntry(hostname *string) {
	var realtime C.uint64_t
	r := C.sd_journal_get_realtime_usec(s.Journal, &realtime)
	if r < 0 {
		log.Fatalf("failed to get realtime timestamp: %s\n", C.strerror(-r))
	}

	var cursor *C.char
	r = C.sd_journal_get_cursor(s.Journal, &cursor)
	if r < 0 {
		log.Fatalf("failed to get cursor: %s\n", C.strerror(-r))
	}

	row := make(map[string]interface{})

	timestamp := time.Unix(int64(realtime/1000000), int64(realtime%1000000)).UTC()

	row["timestamp"] = timestamp.Format("2006-01-02T15:04:05Z")
	s.ProcessEntryFields(row)

	message, _ := json.Marshal(row)
	indexName := fmt.Sprintf("%v-%v", s.Config.Prefix, timestamp.Format("2006-01-02"))
	cursorId := C.GoString(cursor)

	s.Indexer.Index(
		indexName,       // index
		"journal",       // type
		cursorId,        // id
		"",              // parent
		"",              // ttl
		nil,             // date
		string(message), // content
	)
}

func (s *Service) ProcessEntryFields(row map[string]interface{}) {
	var length C.size_t
	var cData *C.char

	for C.sd_journal_restart_data(s.Journal); C.sd_journal_enumerate_data(s.Journal, (*unsafe.Pointer)(unsafe.Pointer(&cData)), &length) > 0; {
		data := C.GoString(cData)

		parts := strings.SplitN(data, "=", 2)

		key := strings.ToLower(parts[0])
		value := parts[1]

		row[strings.TrimPrefix(key, "_")] = value
	}
}

func (s *Service) InitJournal() {
	r := C.sd_journal_open(&s.Journal, C.SD_JOURNAL_LOCAL_ONLY)
	if r < 0 {
		log.Fatalf("failed to open journal: %s\n", C.strerror(-r))
	}

	bytes, err := ioutil.ReadFile(s.Config.State)
	if err == nil {
		s.Cursor = string(bytes)
	}

	if s.Cursor != "" {
		r = C.sd_journal_seek_cursor(s.Journal, C.CString(s.Cursor))
		if r < 0 {
			log.Fatalf("failed to seek journal: %s\n", C.strerror(-r))
		}
		r = C.sd_journal_next_skip(s.Journal, 1)
		if r < 0 {
			log.Fatalf("failed to skip current journal entry: %s\n", C.strerror(-r))
		}
	}
}

func GetFQDN() *string {
	cmd := exec.Command("hostname", "-f")
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		return nil
	}
	fqdn := string(bytes.TrimSpace(out.Bytes()))
	return &fqdn
}
