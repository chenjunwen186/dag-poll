package main

import (
	"dag-poll/pkg/dag"
	"dag-poll/pkg/utils"
	"flag"
)

var (
	fromPath string
	toPath   string
)

func init() {
	flag.StringVar(&fromPath, "from", "./.dag/from.json", "path to load the DAG")
	flag.StringVar(&toPath, "to", "./.dag/to.json", "path to load the DAG")
	flag.Parse()
}

func main() {
	a, err := utils.ReadDAG(fromPath)
	if err != nil {
		panic(err)
	}

	b, err := utils.ReadDAG(toPath)
	if err != nil {
		panic(err)
	}

	if dag.IsEquals(a, b) {
		println("true")
	} else {
		println("false")
	}
}
