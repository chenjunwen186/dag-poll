package utils

import (
	"crypto/md5"
	"dag-poll/pkg/dag"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

func WriteDAG(path string, d *dag.DAG) error {
	dag.SortDAG(d)

	data, err := json.MarshalIndent(d, "", "  ")
	if err != nil {
		log.Fatal(err)
	}

	dir := filepath.Dir(path)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		err = os.MkdirAll(dir, 0644)
		if err != nil {
			return err
		}
	}

	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Write(data)
	return err
}

func ReadDAG(path string) (*dag.DAG, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed reading data from file: %s", err)
	}
	var d dag.DAG
	err = json.Unmarshal(b, &d)
	if err != nil {
		return nil, fmt.Errorf("failed unmarshalling data: %s", err)
	}

	return &d, nil
}

func Last[T any](s []T) *T {
	if len(s) == 0 {
		return nil
	}

	return &s[len(s)-1]
}

type Set[T comparable] map[T]struct{}

func (s Set[T]) Add(v T) {
	if s == nil {
		s = make(map[T]struct{})
	}

	s[v] = struct{}{}
}

func (s Set[T]) Remove(v T) {
	delete(s, v)
}

func (s Set[T]) Contains(v T) bool {
	if s == nil {
		return false
	}

	_, ok := s[v]
	return ok
}

func (s Set[T]) ToSlice() []T {
	var r []T
	for v := range s {
		r = append(r, v)
	}
	return r
}

func GenerateMD5(list []string) string {
	hasher := md5.New()
	for _, v := range list {
		hasher.Write([]byte(v))
	}
	return hex.EncodeToString(hasher.Sum(nil))
}
