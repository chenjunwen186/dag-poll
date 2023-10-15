package main

import (
	"dag-poll/pkg/utils"
	"flag"
	"fmt"
	"log"
	"math/rand"
)

var (
	path string
)

func init() {
	flag.StringVar(&path, "path", "./.dag/from.json", "path to load the DAG")
	flag.Parse()
}

func main() {
	d, err := utils.ReadDAG(path)
	if err != nil {
		log.Fatal(err)
	}

	actions := []func(int){
		func(i int) {
			times := getRandomTimes(1, 5)
			d.AddRandomNodes(times)
			fmt.Println("  -", fmt.Sprintf("%d:", i), "Added ", times, "nodes")
		},
		func(i int) {
			times := getRandomTimes(1, 5)
			d.DeleteRandomNodes(times)
			fmt.Println("  -", fmt.Sprintf("%d:", i), "Deleted ", times, "nodes")
		},
		func(i int) {
			times := getRandomTimes(1, 5)
			d.UpdateRandomNodes(times)
			fmt.Println("  -", fmt.Sprintf("%d:", i), "Updated ", times, "nodes")
		},
	}

	fmt.Println("Start random updating DAG...")
	n := rand.Intn(3) + 3
	for i := 0; i < n; i++ {
		action := actions[rand.Intn(len(actions))]
		action(i + 1)
	}

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

func getRandomTimes(from, to int) int {
	return rand.Intn(to-from) + from
}
