package dag

import (
	"crypto/md5"
	crand "crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"log"
	mrand "math/rand"
	"sort"
)

type DAG struct {
	Nodes   []Node     `json:"nodes"`
	Edges   []Edge     `json:"edges"`
	Sources []Source   `json:"sources"`
	Config  *DAGConfig `json:"-"`
}

type Node struct {
	ID      string `json:"id"`
	Payload string `json:"payload"`
}

type Edge struct {
	From string `json:"from"`
	To   string `json:"to"`
}

type Source struct {
	Name string `json:"name"`
	ID   string `json:"id"`
}

type DAGConfig struct {
	// Default 10000
	NumNodes int
	// Default 5
	NumSources int
	// Default 5
	RandomDegree int // Random ingoing and outgoing edges per node on generated & inserted nodes
	// Default 100
	PayloadSize int // Size of the payload for each node
}

var defaultDAGConfig = DAGConfig{
	NumNodes:     10000,
	NumSources:   5,
	RandomDegree: 5,
	PayloadSize:  100,
}

func GenerateRandomDAG(config *DAGConfig) *DAG {
	if config == nil {
		config = &DAGConfig{}
	}

	if config.NumNodes < 0 {
		log.Fatal("NumNodes must be greater than 0")
	}
	if config.NumNodes == 0 {
		config.NumNodes = defaultDAGConfig.NumNodes
	}

	if config.NumSources >= config.NumNodes {
		log.Fatal("NumSources must be less than NumNodes")
	}
	if config.NumSources < 0 {
		log.Fatal("NumSources must be greater than 0")
	}
	if config.NumSources == 0 {
		config.NumSources = defaultDAGConfig.NumSources
	}

	if config.RandomDegree < 0 {
		log.Fatal("MaxDegree must be greater than 0")
	}
	if config.RandomDegree == 0 {
		config.RandomDegree = defaultDAGConfig.RandomDegree
	}

	if config.PayloadSize < 0 {
		log.Fatal("PayloadSize must be greater than 0")
	}
	if config.PayloadSize == 0 {
		config.PayloadSize = defaultDAGConfig.PayloadSize
	}

	nodes := make([]Node, config.NumNodes)
	for i := 0; i < config.NumNodes; i++ {
		b := randomBase64Bytes(config.PayloadSize)
		nodes[i] = Node{
			ID:      generateMD5(b),
			Payload: base64.StdEncoding.EncodeToString(b),
		}
	}

	edges := []Edge{}
	// Ensure that all sources have at least one out-degree.
	for i := 0; i < config.NumSources; i++ {
		target := config.NumSources + mrand.Intn(config.NumNodes-config.NumSources)
		edges = append(edges, Edge{
			From: nodes[i].ID,
			To:   nodes[target].ID,
		})
	}

	// Ensure other nodes have at least one in-degree.
	for i := config.NumSources; i < config.NumNodes; i++ {
		source := mrand.Intn(i)
		edges = append(edges, Edge{
			From: nodes[source].ID,
			To:   nodes[i].ID,
		})
	}

	for i := 0; i < len(nodes); i++ {
		numEdges := mrand.Intn(config.RandomDegree)
		for j := 0; j < numEdges; j++ {
			target := mrand.Intn(len(nodes))
			if target < i {
				edges = append(edges, Edge{
					From: nodes[target].ID,
					To:   nodes[i].ID,
				})
			} else if target > i {
				edges = append(edges, Edge{
					From: nodes[i].ID,
					To:   nodes[target].ID,
				})
			}
		}
	}

	dedupEdges(&edges)

	sources := make([]Source, config.NumSources)
	for i := 0; i < config.NumSources; i++ {
		sources[i] = Source{
			Name: "Source-" + nodes[i].ID,
			ID:   nodes[i].ID,
		}
	}

	r := &DAG{
		Nodes:   nodes,
		Edges:   edges,
		Sources: sources,
		Config:  config,
	}

	SortDAG(r)

	return r
}

func dedupEdges(edges *[]Edge) {
	s := map[Edge]struct{}{}
	for _, e := range *edges {
		s[e] = struct{}{}
	}
	r := []Edge{}
	for e := range s {
		r = append(r, e)
	}

	*edges = r
}

func SortDAG(dag *DAG) {
	nodes := dag.Nodes
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].ID < nodes[j].ID
	})
	edges := dag.Edges

	sort.Slice(edges, func(i, j int) bool {
		return edges[i].From+edges[i].To < edges[j].From+edges[j].To
	})

	sources := dag.Sources
	sort.Slice(sources, func(i, j int) bool {
		return sources[i].ID < sources[j].ID
	})
}

func randomBase64Bytes(length int) []byte {
	b := make([]byte, length)
	_, err := crand.Read(b)
	if err != nil {
		log.Fatal(err)
	}

	return b
}

func generateMD5(b []byte) string {
	hasher := md5.New()
	hasher.Write(b)
	return hex.EncodeToString(hasher.Sum(nil))
}

// For debug
func (dag *DAG) IsDAG() error {
	graph := make(map[string][]string)
	inDegree := make(map[string]int)

	for _, node := range dag.Nodes {
		graph[node.ID] = []string{}
		inDegree[node.ID] = 0
	}

	for _, edge := range dag.Edges {
		graph[edge.From] = append(graph[edge.From], edge.To)
		inDegree[edge.To]++
	}

	sourcesSet := map[string]struct{}{}
	for _, source := range dag.Sources {
		sourcesSet[source.ID] = struct{}{}
	}

	queue := []string{}
	for k, v := range inDegree {
		if v == 0 {
			queue = append(queue, k)
		}
	}
	if len(queue) != len(dag.Sources) {
		return fmt.Errorf("sources != len(dag.Sources)")
	}
	for _, nodeID := range queue {
		if _, ok := sourcesSet[nodeID]; !ok {
			return fmt.Errorf("sourceSet does not contain nodeID")
		}
	}

	visited := 0
	for len(queue) > 0 {
		nodeID := queue[0]
		queue = queue[1:]

		for _, v := range graph[nodeID] {
			inDegree[v]--
			if inDegree[v] == 0 {
				queue = append(queue, v)
			}
		}
		visited++
	}

	if visited != len(dag.Nodes) {
		return fmt.Errorf("visited != len(dag.Nodes)")
	}

	return nil
}

// Will SortDAG before compare
func IsEquals(left *DAG, right *DAG) bool {
	SortDAG(left)
	SortDAG(right)

	// Compare Nodes
	if len(left.Nodes) != len(right.Nodes) {
		return false
	}
	for i, node := range left.Nodes {
		if node.ID != right.Nodes[i].ID || node.Payload != right.Nodes[i].Payload {
			return false
		}
	}

	// Compare Edges
	if len(left.Edges) != len(right.Edges) {
		return false
	}
	for i, edge := range left.Edges {
		if edge.From != right.Edges[i].From || edge.To != right.Edges[i].To {
			return false
		}
	}

	// Compare Sources
	if len(left.Sources) != len(right.Sources) {
		return false
	}
	for i, source := range left.Sources {
		if source.Name != right.Sources[i].Name || source.ID != right.Sources[i].ID {
			return false
		}
	}

	return true
}

func topologicalSort(dag *DAG) {
	inDegree := make(map[string]int)
	for _, edge := range dag.Edges {
		inDegree[edge.To]++
	}

	queue := []string{}
	for _, source := range dag.Sources {
		queue = append(queue, source.ID)
	}

	nodes := []Node{}
	nodeMap := make(map[string]Node)
	for _, node := range dag.Nodes {
		nodeMap[node.ID] = node
	}

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		nodes = append(nodes, nodeMap[current])

		for _, edge := range dag.Edges {
			if edge.From == current {
				inDegree[edge.To]--
				if inDegree[edge.To] == 0 {
					queue = append(queue, edge.To)
				}
			}
		}
	}

	if len(nodes) != len(dag.Nodes) {
		log.Fatal("len(nodes) != len(dag.Nodes), the graph has a cycle")
	}

	dag.Nodes = nodes
}

func (dag *DAG) AddRandomNodes(times int) {
	config := dag.Config
	if config == nil {
		config = &defaultDAGConfig
	}

	topologicalSort(dag)

	doInsert := func() {
		// 1. Generate new Node
		b := randomBase64Bytes(config.PayloadSize)
		newNode := Node{
			ID:      generateMD5(b),
			Payload: base64.StdEncoding.EncodeToString(b),
		}

		// 2. Insert the new node at a random position in nodes slice, but not at a source position
		position := config.NumSources + mrand.Intn(len(dag.Nodes)+1-config.NumSources) // +1 to allow inserting at the end
		dag.Nodes = append(dag.Nodes[:position], append([]Node{newNode}, dag.Nodes[position:]...)...)

		// 3. Create connections for the new node based on the probability logic from the previous code
		// Ensure the node has at least one in-degree
		nodeIndex := mrand.Intn(position)
		edgeIn := Edge{
			From: dag.Nodes[nodeIndex].ID,
			To:   newNode.ID,
		}
		dag.Edges = append(dag.Edges, edgeIn)

		// Now create in-degrees and out-degrees based on the RandomDegree
		numEdges := mrand.Intn(config.RandomDegree)
		for i := 0; i < numEdges; i++ {
			target := mrand.Intn(len(dag.Nodes))
			if target < position {
				dag.Edges = append(dag.Edges, Edge{
					From: dag.Nodes[target].ID,
					To:   dag.Nodes[position].ID,
				})
			} else if target > position {
				dag.Edges = append(dag.Edges, Edge{
					From: dag.Nodes[position].ID,
					To:   dag.Nodes[target].ID,
				})
			}
		}
	}

	for i := 0; i < times; i++ {
		doInsert()
	}

	// Dedup edges if needed
	dedupEdges(&dag.Edges)
}

func (dag *DAG) DeleteRandomNodes(times int) {
	doDelete := func(nodeID string) {
		// Remove the node from the DAG
		for i, n := range dag.Nodes {
			if n.ID == nodeID {
				dag.Nodes = append(dag.Nodes[:i], dag.Nodes[i+1:]...)
				break
			}
		}

		var upstreamNodes []string
		var downstreamNodes []string
		// Determine upstream and downstream nodes
		for i := 0; i < len(dag.Edges); {
			edge := dag.Edges[i]
			if edge.To == nodeID {
				upstreamNodes = append(upstreamNodes, edge.From)
				dag.Edges = append(dag.Edges[:i], dag.Edges[i+1:]...)
				continue
			}
			if edge.From == nodeID {
				downstreamNodes = append(downstreamNodes, edge.To)
				dag.Edges = append(dag.Edges[:i], dag.Edges[i+1:]...)
				continue
			}
			i++
		}
		if len(upstreamNodes) == 0 || len(downstreamNodes) == 0 {
			return
		}

		// Shuffle upstreamNodes
		mrand.Shuffle(len(upstreamNodes), func(i, j int) {
			upstreamNodes[i], upstreamNodes[j] = upstreamNodes[j], upstreamNodes[i]
		})
		// Shuffle downstreamNodes
		mrand.Shuffle(len(downstreamNodes), func(i, j int) {
			downstreamNodes[i], downstreamNodes[j] = downstreamNodes[j], downstreamNodes[i]
		})

		// Ensure each upstream node connects to at least one downstream node
		for i, upstream := range upstreamNodes {
			downstream := downstreamNodes[i%len(downstreamNodes)]
			dag.Edges = append(dag.Edges, Edge{From: upstream, To: downstream})
		}

		// If there are more downstream nodes, connect them with the remaining upstream nodes
		for i := len(upstreamNodes); i < len(downstreamNodes); i++ {
			upstream := upstreamNodes[i%len(upstreamNodes)]
			downstream := downstreamNodes[i]
			dag.Edges = append(dag.Edges, Edge{From: upstream, To: downstream})
		}
	}

	if times+len(dag.Sources) >= len(dag.Nodes) {
		log.Fatal("times + len(dag.Sources)  > len(dag.Nodes)")
	}

	topologicalSort(dag)

	s := map[string]struct{}{}
	for _, source := range dag.Sources {
		s[source.ID] = struct{}{}
	}
	IDs := []string{}
	for _, node := range dag.Nodes {
		if _, ok := s[node.ID]; !ok {
			IDs = append(IDs, node.ID)
		}
	}
	mrand.Shuffle(len(IDs), func(i, j int) {
		IDs[i], IDs[j] = IDs[j], IDs[i]
	})

	for _, nodeID := range IDs[:times] {
		doDelete(nodeID)
	}

	dedupEdges(&dag.Edges)
}

func (dag *DAG) UpdateRandomNodes(times int) {
	config := dag.Config
	if config == nil {
		config = &defaultDAGConfig
	}

	if len(dag.Sources) >= len(dag.Nodes) {
		panic("len(dag.Nodes) >= len(dag.Sources)")
	}

	topologicalSort(dag)

	for i := 0; i < times; i++ {
		index := len(dag.Sources) + mrand.Intn(len(dag.Nodes)-len(dag.Sources))
		node := &dag.Nodes[index]

		prevID := node.ID
		v := randomBase64Bytes(config.PayloadSize)
		node.Payload = base64.StdEncoding.EncodeToString(v)
		node.ID = generateMD5(v)

		for i := 0; i < len(dag.Edges); i++ {
			edge := &dag.Edges[i]
			if edge.From == prevID {
				edge.From = node.ID
			}
			if edge.To == prevID {
				edge.To = node.ID
			}
		}
	}
}
