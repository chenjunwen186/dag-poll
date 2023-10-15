package main

import (
	"context"
	mkdag "dag-poll/pkg/merkledag"
	"dag-poll/pkg/protocol"
	"dag-poll/pkg/utils"
	"fmt"
	"sync"

	"golang.org/x/sync/semaphore"
)

type Task struct {
	status    TaskStatus
	rw        sync.RWMutex
	merkleDAG *mkdag.MerkleDAG
	ctx       context.Context
	cancel    context.CancelFunc

	visitedMerkleIDs  utils.Set[mkdag.MerkleID]
	visitedPayloadIDs utils.Set[mkdag.PayloadID]

	wg sync.WaitGroup

	sem *semaphore.Weighted

	onDone []func(*mkdag.MerkleDAG)
}

type TaskStatus string

const (
	TaskStatusNone       TaskStatus = "none"
	TaskStatusInProgress TaskStatus = "in_progress"
	TaskStatusDone       TaskStatus = "done"
	TaskStatusAborted    TaskStatus = "aborted"
	TaskStatusFailed     TaskStatus = "failed"
)

func (t *Task) GetTaskStatus() TaskStatus {
	if t == nil {
		return TaskStatusNone
	}

	t.rw.RLock()
	defer t.rw.RUnlock()

	return t.status
}

func (t *Task) OnDone(f func(*mkdag.MerkleDAG)) *Task {
	t.rw.Lock()
	defer t.rw.Unlock()

	t.onDone = append(t.onDone, f)
	return t
}

func (t *Task) runAllOnDone() {
	t.rw.Lock()
	defer t.rw.Unlock()

	for _, f := range t.onDone {
		f(t.merkleDAG)
	}
}

func (t *Task) GetRootMerkleID() string {
	if t == nil {
		return ""
	}

	t.rw.RLock()
	defer t.rw.RUnlock()

	if t.merkleDAG == nil {
		return ""
	}

	return t.merkleDAG.RootMerkleID
}

func (t *Task) GetTaskVersion() int64 {
	if t == nil {
		return 0
	}

	t.rw.RLock()
	defer t.rw.RUnlock()

	if t.merkleDAG == nil {
		return 0
	}

	return t.merkleDAG.Version
}

func (t *Task) Apply() {
	if t == nil {
		return
	}

	t.rw.RLock()
	defer t.rw.RUnlock()

	if t.status == TaskStatusDone {
		state.rw.Lock()
		defer state.rw.Unlock()

		state.MerkleDAG = t.merkleDAG
	}
}

func (t *Task) Abort() {
	t.rw.Lock()
	defer t.rw.Unlock()

	t.cancel()
	t.status = TaskStatusAborted
}

func (t *Task) setupTask() {
	t.rw.Lock()
	defer t.rw.Unlock()

	if t.status == TaskStatusInProgress {
		// Abort previous task
		t.cancel()
	}

	t.ctx, t.cancel = context.WithCancel(context.Background())

	if t.sem == nil {
		t.sem = semaphore.NewWeighted(100)
	}

	t.status = TaskStatusInProgress
}

func (t *Task) setFailed(err error) {
	t.rw.Lock()
	defer t.rw.Unlock()

	t.cancel()
	t.status = TaskStatusFailed
	fmt.Println("Task failed, err: ", err)
}

func (t *Task) setDone() {
	t.rw.Lock()
	defer t.rw.Unlock()

	t.cancel()
	t.status = TaskStatusDone
}

func (t *Task) isVisitedMerkleID(merkleID mkdag.MerkleID) bool {
	t.rw.RLock()
	defer t.rw.RUnlock()

	return t.visitedMerkleIDs.Contains(merkleID)
}

func (t *Task) isVisitedPayloadID(payloadID mkdag.PayloadID) bool {
	t.rw.RLock()
	defer t.rw.RUnlock()

	return t.visitedPayloadIDs.Contains(payloadID)
}

func (t *Task) setMerkleGraph(id mkdag.MerkleID, edges []*mkdag.Node) {
	t.rw.Lock()
	defer t.rw.Unlock()

	t.visitedMerkleIDs.Add(id)
	t.merkleDAG.MerkleGraph[id] = edges
}

func (t *Task) migrate(merkleID mkdag.MerkleID, payloadID mkdag.PayloadID) bool {
	state.rw.RLock()
	defer state.rw.RUnlock()

	if state.MerkleDAG == nil {
		return false
	}

	_, ok := state.MerkleGraph[merkleID]
	if !ok {
		return false
	}

	t.rw.Lock()
	defer t.rw.Unlock()

	var f func(mkdag.MerkleID, mkdag.PayloadID)
	f = func(merkleID mkdag.MerkleID, payloadID mkdag.PayloadID) {
		if t.visitedMerkleIDs.Contains(merkleID) {
			return
		}

		// fmt.Println(len(state.merkleGraph))
		edges, ok := state.MerkleGraph[merkleID]
		if !ok {
			// TODO
			panic("merkleID not found")
		}

		payload, ok := state.PayloadMap[payloadID]
		if !ok {
			// TODO
			panic("payloadID not found")
		}

		t.visitedMerkleIDs.Add(merkleID)
		t.merkleDAG.MerkleGraph[merkleID] = edges

		t.visitedPayloadIDs.Add(payloadID)
		t.merkleDAG.PayloadMap[payloadID] = payload

		for _, edge := range edges {
			f(edge.MerkleID, edge.PayloadID)
		}
	}

	f(merkleID, payloadID)

	return true
}

func (t *Task) getPayload(payloadID mkdag.PayloadID) (mkdag.Payload, bool) {
	t.rw.RLock()
	defer t.rw.RUnlock()

	payload, ok := t.merkleDAG.PayloadMap[payloadID]
	return payload, ok
}

func (t *Task) setPayload(payloadID mkdag.PayloadID, payload mkdag.Payload) {
	t.rw.Lock()
	defer t.rw.Unlock()

	if payload == "" {
		// TODO
		panic("payload is empty")
	}

	t.visitedPayloadIDs.Add(payloadID)
	t.merkleDAG.PayloadMap[payloadID] = payload
}

func (t *Task) StartTask(rootMerkleID mkdag.MerkleID, version int64) {
	t.setupTask()

	sourcesResp, err := getSources(t.ctx, rootMerkleID)
	if err != nil {
		t.setFailed(err)
		return
	}

	var sources []mkdag.Source
	for _, source := range sourcesResp.Sources {
		sources = append(sources, mkdag.Source{
			Name:      source.Name,
			MerkleID:  source.ID,
			PayloadID: source.PayloadID,
		})
	}

	prepare := func() {
		t.rw.Lock()
		defer t.rw.Unlock()

		t.merkleDAG = &mkdag.MerkleDAG{
			Version:      version,
			RootMerkleID: rootMerkleID,
			MerkleGraph:  make(mkdag.MerkleGraph, sourcesResp.Size),
			PayloadMap:   make(mkdag.PayloadMap, sourcesResp.Size),
			Sources:      sources,
		}
	}

	prepare()

	var f func(mkdag.MerkleID, []protocol.QueryItem)
	f = func(prev mkdag.MerkleID, items []protocol.QueryItem) {
		defer t.wg.Done()

		var fetchList []protocol.QueryItem
		for _, item := range items {
			if t.isVisitedMerkleID(item.MerkleID) {
				continue
			}

			if t.migrate(item.MerkleID, item.PayloadID) {
				continue
			}

			fetchList = append(fetchList, item)
		}

		for _, item := range fetchList {
			t.wg.Add(1)
			go t.syncPayload(item.PayloadID)
		}

		if prev != "" {
			var nodes []*mkdag.Node
			for _, item := range items {
				nodes = append(nodes, &mkdag.Node{
					MerkleID:  item.MerkleID,
					PayloadID: item.PayloadID,
				})
			}
			t.setMerkleGraph(prev, nodes)
		}

		var edges []mkdag.MerkleID
		for _, item := range fetchList {
			edges = append(edges, item.MerkleID)
		}
		t.sem.Acquire(t.ctx, 1)
		defer t.sem.Release(1)
		resp, err := doQuery(t.ctx, edges)
		if err != nil {
			t.setFailed(err)
			return
		}

		for merkleID, v := range *resp {
			if len(v) == 0 {
				t.setMerkleGraph(merkleID, nil)
				continue
			}

			t.wg.Add(1)
			go f(merkleID, v)
		}
	}

	var items []protocol.QueryItem
	for _, source := range sources {
		items = append(items, protocol.QueryItem{
			MerkleID:  source.MerkleID,
			PayloadID: source.PayloadID,
		})
	}

	t.wg.Add(1)
	go f("", items)

	t.wg.Wait()
	status := t.GetTaskStatus()
	if status == TaskStatusFailed {
		fmt.Println("Task failed")
	}

	if status == TaskStatusAborted {
		fmt.Println("Task aborted")
	}

	if status == TaskStatusInProgress {
		t.setDone()
		t.Apply()
		t.runAllOnDone()
		fmt.Println("Task done")
	}
}

func (t *Task) syncPayload(payloadID mkdag.PayloadID) {
	defer t.wg.Done()

	t.sem.Acquire(t.ctx, 1)
	defer t.sem.Release(1)

	// Maybe on request not done
	if t.isVisitedPayloadID(payloadID) {
		return
	}

	_, exist := t.getPayload(payloadID)
	if exist {
		return
	}

	payload, exist := state.GetPayload(payloadID)
	if exist {
		t.setPayload(payloadID, payload)
		return
	}

	resp, err := getPayload(t.ctx, payloadID)
	if err != nil {
		t.setFailed(err)
		return
	}

	t.setPayload(payloadID, resp.Payload)
}
