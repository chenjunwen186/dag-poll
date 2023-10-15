package dag_test

import (
	"dag-poll/pkg/dag"
	"testing"
)

func TestCreateDAG(t *testing.T) {
	for i := 0; i < 100; i++ {
		d := dag.GenerateRandomDAG(nil)
		if err := d.IsDAG(); err != nil {
			t.Errorf("failed to create DAG, err: %s\n", err)
		}
	}
}

func TestAddRandomNodes(t *testing.T) {
	for i := 0; i < 20; i++ {
		d := dag.GenerateRandomDAG(nil)
		d.AddRandomNodes(1000)
		if err := d.IsDAG(); err != nil {
			t.Errorf("failed to add nodes, err: %s\n", err)
		}
	}
}

func TestDeleteRandomNodes(t *testing.T) {
	for i := 0; i < 20; i++ {
		d := dag.GenerateRandomDAG(nil)
		d.DeleteRandomNodes(1000)
		if err := d.IsDAG(); err != nil {
			t.Errorf("failed to delete nodes, err: %s\n", err)
		}
	}
}

func TestUpdateRandomNodes(t *testing.T) {
	for i := 0; i < 20; i++ {
		d := dag.GenerateRandomDAG(nil)
		d.UpdateRandomNodes(1000)
		if err := d.IsDAG(); err != nil {
			t.Errorf("failed to update nodes, err: %s\n", err)
		}
	}
}
