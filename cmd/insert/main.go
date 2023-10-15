package main

import (
	"dag-poll/pkg/utils"
	"flag"
	"fmt"
)

var (
	times int
	path  string
)

func init() {
	flag.IntVar(&times, "times", 1, "number of times to random update the DAG")
	flag.StringVar(&path, "path", "./.dag/from.json", "path to load the DAG")
	flag.Parse()
}

func main() {
	d, err := utils.ReadDAG(path)
	if err != nil {
		panic(err)
	}

	d.AddRandomNodes(times)
	err = d.IsDAG()
	if err != nil {
		panic(err)
	}
	err = utils.WriteDAG(path, d)
	if err != nil {
		panic(err)
	}

	fmt.Println("DAG updated")
}
