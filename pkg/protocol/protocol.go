package protocol

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

type RootResponse struct {
	ID      string `json:"id"`
	Version int64  `json:"version"`
}

func (root *RootResponse) Pipe(w io.Writer) error {
	return json.NewEncoder(w).Encode(root)
}

func (root *RootResponse) Load(r io.Reader) error {
	return json.NewDecoder(r).Decode(&root)
}

type SourceRequest struct {
	ID string `json:"id"`
}

func (s *SourceRequest) Load(r *http.Request) error {
	return json.NewDecoder(r.Body).Decode(s)
}

type SourcesResponse struct {
	Size    int      `json:"size"`
	Sources []Source `json:"sources"`
}

func (s *SourcesResponse) Load(r io.Reader) error {
	return json.NewDecoder(r).Decode(s)
}

func (s *SourcesResponse) Pipe(w http.ResponseWriter) error {
	return json.NewEncoder(w).Encode(s)
}

type Source struct {
	Name      string `json:"name"`
	ID        string `json:"id"`
	PayloadID string `json:"payload_id"`
}

type QueryItem struct {
	MerkleID  string `json:"merkle_id"`
	PayloadID string `json:"payload_id"`
}

type QueryRequest []string

func (q *QueryRequest) Load(r *http.Request) error {
	return json.NewDecoder(r.Body).Decode(q)
}

type QueryResponse map[string][]QueryItem

func (q QueryResponse) Pipe(w io.Writer) error {
	return json.NewEncoder(w).Encode(q)
}

func (q *QueryResponse) Load(r io.Reader) error {
	return json.NewDecoder(r).Decode(q)
}

type PayloadRequest struct {
	PayloadID string `json:"payload_id"`
}

func (p *PayloadRequest) Load(r io.Reader) (err error) {
	err = json.NewDecoder(r).Decode(p)
	if err != nil {
		return
	}

	if p.PayloadID == "" {
		err = fmt.Errorf("PayloadRequest.ID is empty")
	}

	return
}

func (p *PayloadRequest) Pipe(w io.Writer) error {
	return json.NewEncoder(w).Encode(p)
}

type PayloadResponse struct {
	Payload string `json:"payload"`
}

func (p PayloadResponse) Pipe(w io.Writer) error {
	return json.NewEncoder(w).Encode(p)
}

func (p *PayloadResponse) Load(r io.Reader) error {
	return json.NewDecoder(r).Decode(p)
}

type ErrorMessage struct {
	Message string `json:"message"`
}

func (e *ErrorMessage) ToJson() string {
	b, _ := json.Marshal(e)
	return string(b)
}

func Error(msg string) string {
	v := ErrorMessage{Message: msg}
	return v.ToJson()
}
