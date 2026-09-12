package repository

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
)

func pitClient(t *testing.T, handle http.HandlerFunc) *ESClient {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		handle(w, r)
	}))
	t.Cleanup(srv.Close)
	client, err := elasticsearch.NewClient(elasticsearch.Config{Addresses: []string{srv.URL}, DisableRetry: true})
	if err != nil {
		t.Fatal(err)
	}
	return &ESClient{client: client}
}

func TestPITLifecycleAndExactSort(t *testing.T) {
	opened, closed := false, false
	client := pitClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/events/_pit":
			opened = r.URL.Query().Get("routing") == "tag" && r.URL.Query().Get("keep_alive") == "120000ms"
			_, _ = w.Write([]byte(`{"id":"pit-1"}`))
		case r.Method == http.MethodPost && r.URL.Path == "/_search":
			if r.URL.Query().Get("routing") != "" {
				t.Error("PIT搜索重复传routing")
			}
			_, _ = w.Write([]byte(`{"pit_id":"pit-2","hits":{"hits":[{"_id":"a","_source":{},"sort":["2026-01-01T00:00:00Z",9007199254740993]}]}}`))
		case r.Method == http.MethodDelete && r.URL.Path == "/_pit":
			var body struct {
				ID string `json:"id"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			closed = body.ID == "pit-2"
			_, _ = w.Write([]byte(`{"succeeded":true}`))
		default:
			t.Errorf("意外请求 %s %s", r.Method, r.URL.String())
			w.WriteHeader(400)
		}
	})
	id, err := client.OpenPIT(context.Background(), []string{"events"}, "tag", 2*time.Minute)
	if err != nil || id != "pit-1" {
		t.Fatalf("open=%s %v", id, err)
	}
	result, err := client.SearchPIT(context.Background(), map[string]any{"pit": map[string]any{"id": id}}, 4096)
	if err != nil {
		t.Fatal(err)
	}
	value, err := json.Marshal(result.Hits[0].Sort[1])
	if err != nil || string(value) != "9007199254740993" {
		t.Fatalf("排序值失去精度 %s %v", value, err)
	}
	if err := client.ClosePIT(context.Background(), result.PITID); err != nil {
		t.Fatal(err)
	}
	if !opened || !closed {
		t.Fatalf("open=%v close=%v", opened, closed)
	}
}

func TestSearchPITRejectsPartialAndOversizedResponse(t *testing.T) {
	for _, tc := range []struct {
		name, body, kind string
		limit            int64
	}{
		{"timeout", `{"timed_out":true,"hits":{"hits":[]}}`, "query_timeout", 1024},
		{"shards", `{"_shards":{"failed":1},"hits":{"hits":[]}}`, "partial_search", 1024},
		{"bytes", `{"hits":{"hits":[]}}`, "response_too_large", 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := pitClient(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(tc.body)) })
			_, err := client.SearchPIT(context.Background(), map[string]any{}, tc.limit)
			var backend *SearchError
			if !errors.As(err, &backend) || backend.Kind != tc.kind {
				t.Fatalf("unexpected error %v", err)
			}
		})
	}
}
