package main

import (
	"dag-poll/pkg/dag"
	"dag-poll/pkg/utils"
	"flag"
	"fmt"
)

func main() {
	numNodes := flag.Int("num-of-nodes", 100, "size of the DAG")
	numSources := flag.Int("num-of-sources", 5, "number of sources")
	maxOutDegree := flag.Int("max-out-degree", 5, "maximum out-degree")
	payloadSize := flag.Int("payload-size", 10, "size of the payload")
	dist := flag.String("dist", "./.dag/from.json", "path to save the DAG")

	flag.Parse()
	config := dag.DAGConfig{
		NumNodes:     *numNodes,
		NumSources:   *numSources,
		RandomDegree: *maxOutDegree,
		PayloadSize:  *payloadSize,
	}

	d := dag.GenerateRandomDAG(&config)
	err := d.IsDAG()
	if err != nil {
		panic(err)
	}

	if err := utils.WriteDAG(*dist, d); err != nil {
		panic(err)
	}

	fmt.Printf("DAG saved to %s\n", *dist)
}
