package main

import (
	"dag-poll/pkg/dag"
	"dag-poll/pkg/protocol"
	"dag-poll/pkg/utils"
	"flag"
	"fmt"
	"log"
	"net/http"
	"sort"
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

type MerkleDAG struct {
	Version      int64
	RootMerkleID string
	MerkleGraph  mkdag.MerkleGraph
	PayloadMap   mkdag.PayloadMap
	Sources      []mkdag.Source
}

func (m *MerkleDAG) ToDAG() *dag.DAG {
	var sources []dag.Source
	for _, source := range m.Sources {
		s := dag.Source{
			Name: source.Name,
			ID:   source.PayloadID,
		}
		sources = append(sources, s)
	}

	nodeMap := make(map[mkdag.PayloadID]dag.Node)
	merkleIDToPayloadID := make(map[mkdag.MerkleID]mkdag.PayloadID, len(m.MerkleGraph))
	for _, items := range m.MerkleGraph {
		for _, item := range items {
			nodeMap[item.PayloadID] = dag.Node{
				ID:      item.PayloadID,
				Payload: m.PayloadMap[item.PayloadID],
			}
			merkleIDToPayloadID[item.MerkleID] = item.PayloadID
		}
	}

	for _, source := range m.Sources {
		nodeMap[source.PayloadID] = dag.Node{
			ID:      source.PayloadID,
			Payload: m.PayloadMap[source.PayloadID],
		}
		merkleIDToPayloadID[source.MerkleID] = source.PayloadID
	}

	var nodes []dag.Node
	for _, node := range nodeMap {
		nodes = append(nodes, node)
	}

	var edges []dag.Edge
	for merkleID, items := range m.MerkleGraph {
		for _, item := range items {
			edges = append(edges, dag.Edge{
				From: merkleIDToPayloadID[merkleID],
				To:   item.PayloadID,
			})
		}
	}

	d := &dag.DAG{
		Nodes:   nodes,
		Sources: sources,
		Edges:   edges,
	}

	return d
}

type State struct {
	rw sync.RWMutex
	*MerkleDAG
}

func (m *State) Apply(d *dag.DAG, abort chan struct{}) {
	v := generateMerkleDAG(d, abort)

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

type StackFrame struct {
	done      map[mkdag.MerkleID]mkdag.PayloadID
	payloadID mkdag.PayloadID
	pending   []mkdag.PayloadID
}

func (s *StackFrame) getMerkleID() mkdag.MerkleID {
	var merkleIDs []string
	// Current PayloadID
	merkleIDs = append(merkleIDs, s.payloadID)
	// All MerkleIDs
	for merkleID := range s.done {
		merkleIDs = append(merkleIDs, merkleID)
	}

	sort.Strings(merkleIDs)
	return utils.GenerateMD5(merkleIDs)
}

func (s *StackFrame) getNode() *mkdag.Node {
	return &mkdag.Node{
		MerkleID:  s.getMerkleID(),
		PayloadID: s.payloadID,
	}
}

func generateMerkleDAG(d *dag.DAG, abort chan struct{}) (r *MerkleDAG) {
	payloadGraph := make(map[mkdag.PayloadID][]mkdag.PayloadID, len(d.Nodes))
	payloadMap := make(mkdag.PayloadMap, len(d.Nodes))

	for _, node := range d.Nodes {
		payloadMap[node.ID] = node.Payload
	}
	for _, edge := range d.Edges {
		payloadGraph[edge.From] = append(payloadGraph[edge.From], edge.To)
	}

	sources := make([]mkdag.Source, 0, len(d.Sources))
	visited := make(map[mkdag.PayloadID]mkdag.MerkleID, len(d.Nodes))
	merkleGraph := make(mkdag.MerkleGraph, len(d.Nodes))

	for _, source := range d.Sources {
		var stack []*StackFrame
		stack = append(stack, &StackFrame{
			done:      make(map[mkdag.MerkleID]mkdag.PayloadID),
			payloadID: source.ID,
			pending:   payloadGraph[source.ID],
		})

		var sourceMerkleID mkdag.MerkleID
		step := func() {
			frame := stack[len(stack)-1]

			if len(frame.pending) == 0 {
				node := frame.getNode()
				visited[node.PayloadID] = node.MerkleID
				nodes := make([]*mkdag.Node, 0, len(frame.done))
				for merkleID, payloadID := range frame.done {
					nodes = append(nodes, &mkdag.Node{
						MerkleID:  merkleID,
						PayloadID: payloadID,
					})
				}
				merkleGraph[node.MerkleID] = nodes

				if len(stack) == 1 {
					stack = stack[:0]
					sourceMerkleID = node.MerkleID
					return
				}

				stack = stack[:len(stack)-1]
				prev := stack[len(stack)-1]
				prev.done[node.MerkleID] = node.PayloadID
				return
			}

			lastIndex := len(frame.pending) - 1
			nextPayloadID := frame.pending[lastIndex]
			frame.pending = frame.pending[:lastIndex]
			if merkleID, ok := visited[nextPayloadID]; ok {
				frame.done[merkleID] = nextPayloadID
				return
			}

			nextFrame := &StackFrame{
				done:      make(map[mkdag.MerkleID]mkdag.PayloadID),
				payloadID: nextPayloadID,
				pending:   payloadGraph[nextPayloadID],
			}

			stack = append(stack, nextFrame)
		}

		for len(stack) > 0 {
			select {
			case <-abort:
				fmt.Println("Stop generating MerkleDAG")
				return
			default:
				step()
			}
		}

		sources = append(sources, mkdag.Source{
			Name:      source.Name,
			MerkleID:  sourceMerkleID,
			PayloadID: source.ID,
		})
	}

	var merkleIDs []string
	for _, source := range sources {
		merkleIDs = append(merkleIDs, source.MerkleID)
	}
	sort.Strings(merkleIDs)
	rootMerkleID := utils.GenerateMD5(merkleIDs)

	r = &MerkleDAG{
		Version:      time.Now().Unix(),
		RootMerkleID: rootMerkleID,
		MerkleGraph:  merkleGraph,
		PayloadMap:   payloadMap,
		Sources:      sources,
	}
	return
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
