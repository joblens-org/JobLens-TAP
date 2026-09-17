package service

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/joblens/tap/internal/model"
)

func TestStreamRawPrefetchesNextPage(t *testing.T) {
	var mu sync.Mutex
	var secondPageAt time.Time
	firstResponseAt := make(chan time.Time, 1)

	es := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeESProduct(w)
		if r.URL.Path == "/" {
			_ = json.NewEncoder(w).Encode(map[string]any{"version": map[string]any{"number": "8.0.0"}})
			return
		}
		if strings.HasSuffix(r.URL.Path, "_pit") {
			_ = json.NewEncoder(w).Encode(map[string]any{"id": "test-pit", "succeeded": true})
			return
		}
		if !strings.HasSuffix(r.URL.Path, "_search") {
			http.NotFound(w, r)
			return
		}
		body, _ := io.ReadAll(r.Body)
		if !strings.Contains(string(body), "search_after") {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"took": 1, "timed_out": false,
				"hits": map[string]any{
					"total": map[string]any{"value": 1, "relation": "eq"},
					"hits":  []map[string]any{rawHit("a", "2026-01-01T00:00:01Z")},
				},
			})
			firstResponseAt <- time.Now()
			return
		}
		mu.Lock()
		secondPageAt = time.Now()
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{
			"took": 1, "timed_out": false,
			"hits": map[string]any{
				"total": map[string]any{"value": 1, "relation": "eq"},
				"hits":  []any{},
			},
		})
	}))
	defer es.Close()

	_, ssvc := newQueryServiceForTest(t, es.URL)

	req := &model.RawStreamRequest{
		Cluster: "c1", Job: "1", From: "now-1h", To: "now", PageSize: 1,
	}
	emit := func(msg model.StreamMessage) error {
		if msg.Type == model.StreamTypeRecords {
			time.Sleep(60 * time.Millisecond)
		}
		return nil
	}

	if _, err := ssvc.StreamRaw(context.Background(), req, []string{"c1"}, emit); err != nil {
		t.Fatalf("StreamRaw failed: %v", err)
	}
	first := <-firstResponseAt
	mu.Lock()
	second := secondPageAt
	mu.Unlock()
	if second.IsZero() {
		t.Fatal("second page was never requested")
	}
	if d := second.Sub(first); d > 50*time.Millisecond {
		t.Fatalf("second page requested %v after first response, want prefetch (<50ms)", d)
	}
}
