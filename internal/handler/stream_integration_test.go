package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/joblens/tap/internal/cluster"
	"github.com/joblens/tap/internal/config"
	"github.com/joblens/tap/internal/model"
	"github.com/joblens/tap/internal/repository"
	"github.com/joblens/tap/internal/service"
)

func fakeManagement(t *testing.T, esURL string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"clusters": []map[string]any{{
				"cluster_name": "c1",
				"cluster_type": "condor",
				"tags":         []string{"t1"},
				"enabled":      true,
				"extra":        map[string]any{"es_url": esURL},
			}},
			"total": 1,
		})
	}))
	t.Cleanup(srv.Close)
	return srv
}

func buildStreamTestHandler(t *testing.T, esURL string) *StreamHandler {
	t.Helper()
	mgmt := fakeManagement(t, esURL)
	mgr := cluster.NewManager(mgmt.URL, 5*time.Minute)
	if err := mgr.InitialFetch(context.Background()); err != nil {
		t.Fatalf("initial fetch: %v", err)
	}
	esMgr := repository.NewClientManager(mgr)
	cfg := &config.Config{Registry: model.BuildDefaultRegistry()}
	cfg.Normalize()
	cfg.DefaultCollectors = cfg.Registry.GetCollectorNames()
	qsvc := service.NewQueryService(cfg, esMgr, mgr)
	streamSvc := service.NewStreamService(qsvc)
	return NewStreamHandler(streamSvc, qsvc, service.NewLimiter(4), cfg)
}

func fakeRawES(t *testing.T) *httptest.Server {
	t.Helper()
	var mu sync.Mutex
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			_ = json.NewEncoder(w).Encode(map[string]any{"version": map[string]any{"number": "8.0.0"}})
			return
		}
		mu.Lock()
		calls++
		n := calls
		mu.Unlock()
		hits := []map[string]any{}
		if n == 1 {
			hits = []map[string]any{
				{"_id": "a", "_index": "cpumem_collector_2026.01.01", "_score": 1, "sort": []any{"t4"},
					"_source": map[string]any{"hostname": "h", "@timestamp": "2026-01-01T00:00:02Z", "job_info": map[string]any{"NativeJobID": "1"}, "data": map[string]any{}}},
				{"_id": "b", "_index": "cpumem_collector_2026.01.01", "_score": 1, "sort": []any{"t3"},
					"_source": map[string]any{"hostname": "h", "@timestamp": "2026-01-01T00:00:01Z", "job_info": map[string]any{"NativeJobID": "1"}, "data": map[string]any{}}},
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"took": 1, "timed_out": false,
			"hits": map[string]any{"total": map[string]any{"value": 2, "relation": "eq"}, "hits": hits},
		})
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestStreamHandlerRawEndToEnd(t *testing.T) {
	gin.SetMode(gin.TestMode)
	es := fakeRawES(t)
	h := buildStreamTestHandler(t, es.URL)

	r := gin.New()
	r.GET("/data/raw/stream", h.Raw)

	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/data/raw/stream?cluster=c1&job=1&from=now-1h&to=now&page_size=2", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", w.Code, w.Body.String())
	}
	if ct := w.Header().Get("Content-Type"); !strings.Contains(ct, "x-ndjson") {
		t.Errorf("content-type = %q", ct)
	}
	if w.Header().Get("X-Accel-Buffering") != "no" {
		t.Errorf("missing X-Accel-Buffering: no")
	}

	types := map[string]int{}
	records := 0
	for _, line := range strings.Split(strings.TrimSpace(w.Body.String()), "\n") {
		if line == "" {
			continue
		}
		var msg model.StreamMessage
		if err := json.Unmarshal([]byte(line), &msg); err != nil {
			t.Fatalf("bad ndjson line %q: %v", line, err)
		}
		types[msg.Type]++
		if msg.Type == model.StreamTypeRecords {
			data := msg.Data.(map[string]any)
			if recs, ok := data["records"].([]any); ok {
				records += len(recs)
			}
		}
	}
	if types[model.StreamTypeMeta] != 1 || types[model.StreamTypeDone] != 1 {
		t.Errorf("message types = %v", types)
	}
	if records != 2 {
		t.Errorf("records = %d, want 2", records)
	}
}

func TestStreamHandlerRawTooManyConcurrent(t *testing.T) {
	gin.SetMode(gin.TestMode)
	es := fakeRawES(t)
	h := buildStreamTestHandler(t, es.URL)
	h.limiter = service.NewLimiter(1)
	if !h.limiter.Acquire() {
		t.Fatal("pre-acquire failed")
	}

	r := gin.New()
	r.GET("/data/raw/stream", h.Raw)

	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/data/raw/stream?cluster=c1&job=1&from=now-1h&to=now", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusTooManyRequests {
		t.Errorf("status = %d, want 429 body=%s", w.Code, w.Body.String())
	}
}
