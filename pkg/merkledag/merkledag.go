package merkledag

import (
	"dag-poll/pkg/dag"
	"dag-poll/pkg/utils"
	"fmt"
	"sort"
	"time"
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

type StackFrame struct {
	done      map[MerkleID]PayloadID
	payloadID PayloadID
	pending   []PayloadID
}

func (s *StackFrame) getMerkleID() MerkleID {
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

func (s *StackFrame) getNode() *Node {
	return &Node{
		MerkleID:  s.getMerkleID(),
		PayloadID: s.payloadID,
	}
}

func GenerateMerkleDAG(d *dag.DAG, abort chan struct{}) (r *MerkleDAG) {
	payloadGraph := make(map[PayloadID][]PayloadID, len(d.Nodes))
	payloadMap := make(PayloadMap, len(d.Nodes))

	for _, node := range d.Nodes {
		payloadMap[node.ID] = node.Payload
	}
	for _, edge := range d.Edges {
		payloadGraph[edge.From] = append(payloadGraph[edge.From], edge.To)
	}

	sources := make([]Source, 0, len(d.Sources))
	visited := make(map[PayloadID]MerkleID, len(d.Nodes))
	merkleGraph := make(MerkleGraph, len(d.Nodes))

	for _, source := range d.Sources {
		var stack []*StackFrame
		stack = append(stack, &StackFrame{
			done:      make(map[MerkleID]PayloadID),
			payloadID: source.ID,
			pending:   payloadGraph[source.ID],
		})

		var sourceMerkleID MerkleID
		step := func() {
			frame := stack[len(stack)-1]

			if len(frame.pending) == 0 {
				node := frame.getNode()
				visited[node.PayloadID] = node.MerkleID
				nodes := make([]*Node, 0, len(frame.done))
				for merkleID, payloadID := range frame.done {
					nodes = append(nodes, &Node{
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
				done:      make(map[MerkleID]PayloadID),
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

		sources = append(sources, Source{
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
