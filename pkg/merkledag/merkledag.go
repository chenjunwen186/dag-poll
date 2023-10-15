package merkledag

import (
	"dag-poll/pkg/dag"
)

type MerkleID = string
type PayloadID = string
type Payload = string

type Source struct {
	Name      string
	MerkleID  MerkleID
	PayloadID PayloadID
}

type Node struct {
	MerkleID  MerkleID
	PayloadID PayloadID
}

type MerkleGraph map[MerkleID][]*Node
type PayloadMap map[PayloadID]Payload

type MerkleDAG struct {
	Version      int64
	RootMerkleID string
	MerkleGraph  MerkleGraph
	PayloadMap   PayloadMap
	Sources      []Source
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

	nodeMap := make(map[PayloadID]dag.Node)
	merkleIDToPayloadID := make(map[MerkleID]PayloadID, len(m.MerkleGraph))
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
