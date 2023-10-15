package main

import (
	"bytes"
	"context"
	"dag-poll/pkg/dag"
	mkdag "dag-poll/pkg/merkledag"
	"dag-poll/pkg/protocol"
	"dag-poll/pkg/utils"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"time"
)

var (
	endpoint string
	from     string
	to       string
	root     string
	sources  string
	query    string
	payload  string

	state State
	task  Task
)

func init() {
	flag.StringVar(&endpoint, "observable-endpoint", "http://127.0.0.1:3633", "observable endpoint")
	flag.StringVar(&from, "from", "./.dag/from.json", "path to load the DAG")
	flag.StringVar(&to, "to", "./.dag/to.json", "path to save the DAG")
	flag.Parse()

	root = endpoint + "/root"
	sources = endpoint + "/sources"
	query = endpoint + "/query"
	payload = endpoint + "/payload"

	task.OnDone(onTaskDone)
}

func main() {
	for {
		resp, err := peekRoot()
		if err != nil {
			fmt.Println("Peek root failed, err: ", err)
			time.Sleep(time.Second)
			continue
		}

		rootMerkleID := state.GetRootMerkleID()
		taskStatus := task.GetTaskStatus()
		taskRootMerkleID := task.GetRootMerkleID()
		taskVersion := task.GetTaskVersion()

		if rootMerkleID == resp.ID {
			fmt.Printf("Root not changed\n")
			goto next
		}

		if taskStatus == TaskStatusNone {
			fmt.Printf("Start initial task\n  - task id: %s\n", resp.ID)
			task.StartTask(resp.ID, resp.Version)
			goto next
		}

		if taskRootMerkleID != resp.ID && taskVersion <= resp.Version {
			fmt.Printf("Root changed, start new task\n  - root id: %s\n  - task id: %s\n", resp.ID, resp.ID)
			task.StartTask(resp.ID, resp.Version)
			goto next
		}

		if taskStatus == TaskStatusInProgress {
			fmt.Printf("The task is in progress\n  - root id: %s\n", taskRootMerkleID)
		}

		if taskStatus == TaskStatusFailed {
			fmt.Println("The task is failed")
		}

	next:
		time.Sleep(time.Second)
	}
}

func peekRoot() (*protocol.RootResponse, error) {
	resp, err := http.Get(root)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("root not found")
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("root request failed, status: %v", resp.StatusCode)
	}

	defer resp.Body.Close()

	var r protocol.RootResponse
	err = r.Load(resp.Body)
	if err != nil {
		return nil, err
	}

	return &r, nil
}

func getSources(ctx context.Context, rootMerkleID mkdag.MerkleID) (*protocol.SourcesResponse, error) {
	v := protocol.SourceRequest{ID: rootMerkleID}
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}

	r := bytes.NewReader(b)

	req, err := http.NewRequest(http.MethodGet, sources, r)
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("sources not found, root_id: %v", rootMerkleID)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("sources request failed, status: %v", resp.StatusCode)
	}

	defer resp.Body.Close()
	var sources protocol.SourcesResponse
	err = sources.Load(resp.Body)
	if err != nil {
		return nil, err
	}

	return &sources, nil
}

func doQuery(ctx context.Context, merkleIDs []mkdag.MerkleID) (*protocol.QueryResponse, error) {
	v := protocol.QueryRequest(merkleIDs)
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}

	r := bytes.NewReader(b)
	req, err := http.NewRequest(http.MethodGet, query, r)
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("query request failed, status: %v", resp.StatusCode)
	}

	defer resp.Body.Close()
	var queryResp protocol.QueryResponse
	err = queryResp.Load(resp.Body)
	if err != nil {
		return nil, err
	}

	return &queryResp, nil
}

func getPayload(ctx context.Context, payloadID mkdag.PayloadID) (*protocol.PayloadResponse, error) {
	v := protocol.PayloadRequest{PayloadID: payloadID}
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}

	r := bytes.NewReader(b)
	req, err := http.NewRequest(http.MethodGet, payload, r)
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("payload not found, payload_id: %v", payloadID)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("payload request failed, status: %v", resp.StatusCode)
	}

	defer resp.Body.Close()

	var payloadResp protocol.PayloadResponse
	err = payloadResp.Load(resp.Body)
	if err != nil {
		return nil, err
	}

	return &payloadResp, nil
}

func onTaskDone(m *mkdag.MerkleDAG) {
	d := m.ToDAG()

	if err := d.IsDAG(); err != nil {
		fmt.Println("DAG is not valid, err: ", err)
		return
	}

	err := utils.WriteDAG(to, m.ToDAG())
	if err != nil {
		fmt.Println("Failed to write DAG, err: ", err)
	}

	v, err := utils.ReadDAG(from)
	if err != nil {
		fmt.Println("Failed to read DAG, err: ", err)
		return
	}

	dag.SortDAG(d)
	dag.SortDAG(v)
	if !dag.IsEquals(d, v) {
		fmt.Println("DAG is not equal")
	} else {
		fmt.Println("DAG is equal")
	}

	// For debug
	// for k, v := range m.merkleGraph {
	// 	fmt.Println(k)
	// 	for _, edge := range v {
	// 		fmt.Println("  -", edge.merkleID)
	// 	}
	// }

	// var f func(MerkleID)
	// f = func(merkleID MerkleID) {
	// 	fmt.Println(merkleID)
	// 	edges, ok := m.merkleGraph[merkleID]
	// 	if !ok {
	// 		panic("not found")
	// 	}
	// 	for _, edge := range edges {
	// 		f(edge.merkleID)
	// 	}
	// }

	// for _, source := range state.sources {
	// 	f(source.merkleID)
	// }
}
