package main

import (
	"dag-poll/pkg/dag"
	"dag-poll/pkg/protocol"
	"dag-poll/pkg/utils"
	"flag"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	mkdag "dag-poll/pkg/merkledag"

	"github.com/fsnotify/fsnotify"
)

var (
	state State
)

func main() {
	source := flag.String("path", "./.dag/from.json", "path to load the DAG")
	port := flag.String("port", "3633", "port to listen")

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}
	defer watcher.Close()

	go func() {
		abort := make(chan struct{})
		loadDAG := func() {
			close(abort)
			abort = make(chan struct{})

			d, err := utils.ReadDAG(*source)
			if err != nil {
				log.Fatalf("failed to load DAG from %s, err: %s\n", *source, err)
			}

			go state.Apply(d, abort)
		}

		loadDAG()

		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				if event.Has(fsnotify.Write) {
					loadDAG()
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				log.Println("error:", err)
			}
		}
	}()

	err = watcher.Add(*source)
	if err != nil {
		log.Fatal(err)
	}

	http.HandleFunc("/root", root)
	http.HandleFunc("/sources", sources)
	http.HandleFunc("/query", query)
	http.HandleFunc("/payload", payload)

	addr := "0.0.0.0:" + string(*port)
	fmt.Println("Listening on addr: " + addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}

func root(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if r.Method != "GET" {
		http.Error(w, protocol.Error("Method not allowed"), http.StatusMethodNotAllowed)
		return
	}

	root := state.Root()
	if root == "" {
		http.Error(w, protocol.Error(`Root not found`), http.StatusNotFound)
		return
	}

	version := time.Now().Unix()
	resp := protocol.RootResponse{
		ID:      root,
		Version: version,
	}

	err := resp.Pipe(w)
	if err != nil {
		http.Error(w, protocol.Error("Failed to encode data"), http.StatusInternalServerError)
		return
	}
}

func sources(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if r.Method != "GET" {
		http.Error(w, protocol.Error("Method not allowed"), http.StatusMethodNotAllowed)
		return
	}

	var sourceReq protocol.SourceRequest
	err := sourceReq.Load(r)
	if err != nil {
		http.Error(w, protocol.Error("Invalid http.Body"), http.StatusBadRequest)
		return
	}

	root := state.Root()
	if root != sourceReq.ID {
		msg := fmt.Sprintf("Root not match, current root: %v", root)
		http.Error(w, protocol.Error(msg), http.StatusNotFound)
		return
	}

	sources := state.Sources()
	if sources == nil {
		http.Error(w, protocol.Error("Sources not found"), http.StatusNotFound)
		return
	}

	v := make([]protocol.Source, len(sources))
	for index, source := range sources {
		v[index] = protocol.Source{
			Name:      source.Name,
			ID:        source.MerkleID,
			PayloadID: source.PayloadID,
		}
	}

	resp := &protocol.SourcesResponse{
		Size:    len(sources),
		Sources: v,
	}

	if err := resp.Pipe(w); err != nil {
		http.Error(w, protocol.Error("Failed to encode data"), http.StatusInternalServerError)
		return
	}
}

func query(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if r.Method != "GET" {
		http.Error(w, protocol.Error("Method not allowed"), http.StatusMethodNotAllowed)
		return
	}

	var queryReq protocol.QueryRequest
	err := queryReq.Load(r)
	if err != nil {
		http.Error(w, protocol.Error("Invalid http.Body"), http.StatusBadRequest)
		return
	}

	v := state.Query(queryReq)
	m := make(protocol.QueryResponse, len(v))
	for merkleID, items := range v {
		m[merkleID] = make([]protocol.QueryItem, len(items))
		for index, item := range items {
			m[merkleID][index] = protocol.QueryItem{
				MerkleID:  item.MerkleID,
				PayloadID: item.PayloadID,
			}
		}
	}

	err = m.Pipe(w)
	if err != nil {
		http.Error(w, protocol.Error("Failed to encode data"), http.StatusInternalServerError)
		return
	}
}

func payload(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, protocol.Error("Method not allowed"), http.StatusMethodNotAllowed)
		return
	}

	var payloadRequest protocol.PayloadRequest
	err := payloadRequest.Load(r.Body)
	if err != nil {
		http.Error(w, protocol.Error("Invalid http.Body"), http.StatusBadRequest)
		return
	}

	payload := state.Payload(payloadRequest.PayloadID)
	if payload == nil {
		http.Error(w, protocol.Error("Payload not found"), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	var payloadResponse protocol.PayloadResponse
	payloadResponse.Payload = *payload

	err = payloadResponse.Pipe(w)
	if err != nil {
		http.Error(w, protocol.Error("Failed to encode data"), http.StatusInternalServerError)
		return
	}
}

type State struct {
	rw sync.RWMutex
	*mkdag.MerkleDAG
}

func (m *State) Apply(d *dag.DAG, abort chan struct{}) {
	v := mkdag.GenerateMerkleDAG(d, abort)

	select {
	case <-abort:
		return
	default:
		m.rw.Lock()
		defer m.rw.Unlock()

		m.MerkleDAG = v
		fmt.Println("MerkleDAG loaded, root: ", v.RootMerkleID)

		// For debug
		d := v.ToDAG()
		if err := d.IsDAG(); err != nil {
			fmt.Println("MerkleDAG is not valid, err: ", err)
			return
		}
	}
}

func (m *State) Root() string {
	m.rw.RLock()
	defer m.rw.RUnlock()

	return m.RootMerkleID
}

type QueryItem struct {
	MerkleID  string
	PayloadID string
}

func (m *State) Query(merkleIDs []string) (r map[string][]QueryItem) {
	m.rw.RLock()
	defer m.rw.RUnlock()

	r = make(map[string][]QueryItem, len(merkleIDs))
	if m.MerkleGraph == nil {
		return
	}

	for _, merkleID := range merkleIDs {
		nodes, ok := m.MerkleGraph[merkleID]
		if !ok {
			r[merkleID] = []QueryItem{}
			continue
		}

		v := []QueryItem{}
		for _, node := range nodes {
			item := QueryItem{
				MerkleID:  node.MerkleID,
				PayloadID: node.PayloadID,
			}
			v = append(v, item)
		}

		r[merkleID] = v
	}

	return
}

func (s *State) Sources() []mkdag.Source {
	s.rw.RLock()
	defer s.rw.RUnlock()

	if len(s.MerkleDAG.Sources) == 0 {
		return nil
	}

	return s.MerkleDAG.Sources
}

func (m *State) Payload(payloadID string) (r *string) {
	m.rw.RLock()
	defer m.rw.RUnlock()

	if m.PayloadMap == nil {
		return
	}

	payload, ok := m.PayloadMap[payloadID]
	if !ok {
		return
	}

	return &payload
}
