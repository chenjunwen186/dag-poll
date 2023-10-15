package main

import (
	mkdag "dag-poll/pkg/merkledag"
	"sync"
)

type State struct {
	rw sync.RWMutex
	*mkdag.MerkleDAG
}

func (s *State) GetRootMerkleID() string {
	s.rw.RLock()
	defer s.rw.RUnlock()

	if s.MerkleDAG == nil {
		return ""
	}

	return s.RootMerkleID
}

func (s *State) GetPayload(payloadID mkdag.PayloadID) (r mkdag.Payload, ok bool) {
	s.rw.RLock()
	defer s.rw.RUnlock()

	if s.MerkleDAG == nil {
		return "", false
	}

	r, ok = s.PayloadMap[payloadID]
	return
}
