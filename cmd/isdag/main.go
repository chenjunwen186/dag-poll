package main

import (
	"dag-poll/pkg/utils"
	"flag"
	"fmt"
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
	err := check(fromPath)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(fromPath, "is DAG")

	err = check(toPath)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(toPath, "is DAG")
}

func check(path string) error {
	d, err := utils.ReadDAG(path)
	if err != nil {
		return fmt.Errorf("failed to load DAG from %s, err: %s", path, err)
	}

	err = d.IsDAG()
	if err != nil {
		return fmt.Errorf("%s is not DAG, err: %s", path, err)
	}

	return nil
}
