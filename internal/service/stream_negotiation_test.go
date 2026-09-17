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

	"github.com/joblens/tap/internal/model"
)

func TestStreamRawAutoPageSizeNegotiation(t *testing.T) {
	var mu sync.Mutex
	var searchSizes []float64

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
		var parsed struct {
			Size float64 `json:"size"`
		}
		_ = json.Unmarshal(body, &parsed)
		mu.Lock()
		searchSizes = append(searchSizes, parsed.Size)
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{
			"took": 1, "timed_out": false,
			"hits": map[string]any{"total": map[string]any{"value": 0, "relation": "eq"}, "hits": []any{}},
		})
	}))
	defer es.Close()

	_, ssvc := newQueryServiceForTest(t, es.URL)

	cases := []struct {
		name     string
		pageSize string
		want     int
	}{
		{name: "auto", pageSize: "auto", want: 2000},
		{name: "explicit", pageSize: "512", want: 512},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mu.Lock()
			searchSizes = nil
			mu.Unlock()

			req := &model.RawStreamRequest{
				Cluster: "c1", Job: "1", Collector: "cpumem",
				From: "now-1h", To: "now", PageSize: tc.pageSize,
			}
			var metaPageSize int
			emit := func(msg model.StreamMessage) error {
				if msg.Type == model.StreamTypeMeta {
					metaPageSize = msg.Data.(model.StreamMeta).PageSize
				}
				return nil
			}
			if _, err := ssvc.StreamRaw(context.Background(), req, []string{"c1"}, emit); err != nil {
				t.Fatalf("StreamRaw failed: %v", err)
			}
			if metaPageSize != tc.want {
				t.Fatalf("meta page_size = %d, want %d", metaPageSize, tc.want)
			}
			mu.Lock()
			defer mu.Unlock()
			if len(searchSizes) == 0 || int(searchSizes[0]) != tc.want {
				t.Fatalf("es search size = %v, want %d", searchSizes, tc.want)
			}
		})
	}
}
