package handler

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/joblens/tap/internal/cluster"
	"github.com/joblens/tap/internal/config"
	"github.com/joblens/tap/internal/model"
	"github.com/joblens/tap/internal/repository"
	"github.com/joblens/tap/internal/service"
)

func fakeNoDataES(t *testing.T, failAggregation bool) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var violations atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		if r.URL.Path == "/" {
			_ = json.NewEncoder(w).Encode(map[string]any{"version": map[string]any{"number": "8.0.0"}})
			return
		}
		if strings.HasSuffix(r.URL.Path, "_search") {
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("read search body: %v", err)
			}
			if strings.Contains(string(body), "first_seen") {
				if failAggregation {
					w.WriteHeader(http.StatusServiceUnavailable)
					_ = json.NewEncoder(w).Encode(map[string]any{"error": "aggregation unavailable"})
					return
				}
				_ = json.NewEncoder(w).Encode(map[string]any{
					"took":      1,
					"timed_out": false,
					"hits": map[string]any{
						"total": map[string]any{"value": 0, "relation": "eq"},
						"hits":  []any{},
					},
					"aggregations": map[string]any{
						"first_seen": map[string]any{"value": nil},
						"last_seen":  map[string]any{"value": nil},
					},
				})
				return
			}
		}
		violations.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)
	return srv, &violations
}

func buildRawTestHandler(t *testing.T, esURL string) *RawHandler {
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
	return NewRawHandler(esMgr, qsvc)
}

func TestRawFullRangeNoDataReturnsEmpty(t *testing.T) {
	gin.SetMode(gin.TestMode)
	es, violations := fakeNoDataES(t, false)
	h := buildRawTestHandler(t, es.URL)

	r := gin.New()
	r.GET("/data/raw", h.Query)

	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/data/raw?cluster=c1&job=1&collector=gpu&full_range=true&size=1&flatten=false&fields=data.job_id", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", w.Code, w.Body.String())
	}
	var resp struct {
		Code int `json:"code"`
		Data struct {
			Records    []json.RawMessage `json:"records"`
			Pagination struct {
				Returned int  `json:"returned"`
				HasMore  bool `json:"has_more"`
			} `json:"pagination"`
		} `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v body=%s", err, w.Body.String())
	}
	if resp.Code != 0 {
		t.Errorf("code = %d body=%s", resp.Code, w.Body.String())
	}
	if resp.Data.Records == nil {
		t.Errorf("records must be [] not null: %s", w.Body.String())
	}
	if len(resp.Data.Records) != 0 {
		t.Errorf("records = %d, want 0", len(resp.Data.Records))
	}
	if resp.Data.Pagination.Returned != 0 || resp.Data.Pagination.HasMore {
		t.Errorf("pagination = %+v", resp.Data.Pagination)
	}
	if got := violations.Load(); got != 0 {
		t.Errorf("unexpected ES data/PIT requests after no-data discovery: %d", got)
	}
}

func TestRawStreamFullRangeNoDataCompletesEmpty(t *testing.T) {
	gin.SetMode(gin.TestMode)
	es, violations := fakeNoDataES(t, false)
	h := buildStreamTestHandler(t, es.URL)

	r := gin.New()
	r.GET("/data/raw/stream", h.Raw)

	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/data/raw/stream?cluster=c1&job=1&collector=gpu&full_range=true&page_size=2", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", w.Code, w.Body.String())
	}
	types := map[string]int{}
	records := 0
	var done map[string]any
	for _, line := range strings.Split(strings.TrimSpace(w.Body.String()), "\n") {
		if line == "" {
			continue
		}
		var msg model.StreamMessage
		if err := json.Unmarshal([]byte(line), &msg); err != nil {
			t.Fatalf("bad ndjson %q: %v", line, err)
		}
		types[msg.Type]++
		switch msg.Type {
		case model.StreamTypeRecords:
			if data, ok := msg.Data.(map[string]any); ok {
				if recs, ok := data["records"].([]any); ok {
					records += len(recs)
				}
			}
		case model.StreamTypeDone:
			done, _ = msg.Data.(map[string]any)
		}
	}
	if types[model.StreamTypeMeta] != 1 || types[model.StreamTypeDone] != 1 {
		t.Errorf("message types = %v body=%s", types, w.Body.String())
	}
	if records != 0 {
		t.Errorf("records = %d, want 0", records)
	}
	if done == nil || done["complete"] != true {
		t.Errorf("done = %v, want complete=true", done)
	}
	if returned, _ := done["returned"].(float64); returned != 0 {
		t.Errorf("done.returned = %v, want 0", done["returned"])
	}
	if got := violations.Load(); got != 0 {
		t.Errorf("unexpected ES data/PIT requests after no-data discovery: %d", got)
	}
}

func TestRawFullRangeUpstreamFailureStillErrors(t *testing.T) {
	gin.SetMode(gin.TestMode)
	es, _ := fakeNoDataES(t, true)
	h := buildRawTestHandler(t, es.URL)

	r := gin.New()
	r.GET("/data/raw", h.Query)

	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/data/raw?cluster=c1&job=1&collector=gpu&full_range=true&size=1", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 body=%s", w.Code, w.Body.String())
	}
}
