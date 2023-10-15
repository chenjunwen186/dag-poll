package protocol_test

import (
	"bytes"
	"dag-poll/pkg/protocol"
	"strings"
	"testing"
)

func TestRootLoad(t *testing.T) {
	s := `{"id": "hello world", "version": 1}`
	r := strings.NewReader(s)

	var root protocol.RootResponse
	err := root.Load(r)
	if err != nil {
		t.Fatal(err)
	}

	if root.ID != "hello world" && root.Version != 1 {
		t.Fatal("root is not hello world")
	}
}

func TestRootPipe(t *testing.T) {
	var buf bytes.Buffer

	root := protocol.RootResponse{ID: "1234", Version: 1}

	err := root.Pipe(&buf)
	if err != nil {
		t.Fatalf("Failed to encode Root to JSON: %v", err)
	}

	result := buf.String()
	expected := `{"id":"1234","version":1}` + "\n"

	if result != expected {
		t.Errorf("Expected %s, but got %s", expected, result)
	}
}
