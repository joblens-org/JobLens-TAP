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

	"github.com/joblens/tap/internal/cluster"
	"github.com/joblens/tap/internal/config"
	"github.com/joblens/tap/internal/model"
	"github.com/joblens/tap/internal/repository"
)

func newFakeManagementAPI(t *testing.T, esURL string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/clusters/scheme" {
			http.NotFound(w, r)
			return
		}
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
}

func newQueryServiceForTest(t *testing.T, esURL string) (*QueryService, *StreamService) {
	t.Helper()
	mgmt := newFakeManagementAPI(t, esURL)
	t.Cleanup(mgmt.Close)

	mgr := cluster.NewManager(mgmt.URL, 5*time.Minute)
	if err := mgr.InitialFetch(context.Background()); err != nil {
		t.Fatalf("initial fetch: %v", err)
	}
	esMgr := repository.NewClientManager(mgr)
	cfg := &config.Config{Registry: model.BuildDefaultRegistry()}
	cfg.Normalize()
	cfg.DefaultCollectors = cfg.Registry.GetCollectorNames()
	qsvc := NewQueryService(cfg, esMgr, mgr)
	return qsvc, NewStreamService(qsvc)
}

func writeESProduct(w http.ResponseWriter) {
	w.Header().Set("X-Elastic-Product", "Elasticsearch")
}

func rawHit(id, ts string) map[string]any {
	return map[string]any{
		"_id":    id,
		"_index": "cpumem_collector_2026.01.01",
		"_score": 1,
		"sort":   []any{ts},
		"_source": map[string]any{
			"hostname":   "h1",
			"@timestamp": ts,
			"job_info":   map[string]any{"NativeJobID": "1"},
			"data":       map[string]any{"summary": map[string]any{"cpuPercent": 1.0}},
		},
	}
}

func TestStreamRawPagingAndMerge(t *testing.T) {
	var mu sync.Mutex
	call := 0
	es := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			writeESProduct(w)
			_ = json.NewEncoder(w).Encode(map[string]any{"version": map[string]any{"number": "8.0.0"}})
			return
		}
		if !strings.HasSuffix(r.URL.Path, "_search") {
			http.NotFound(w, r)
			return
		}
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		call++
		n := call
		mu.Unlock()

		var hits []map[string]any
		switch {
		case !strings.Contains(string(body), "search_after"):
			hits = []map[string]any{rawHit("a", "2026-01-01T00:00:04Z"), rawHit("b", "2026-01-01T00:00:03Z")}
		case n <= 2:
			hits = []map[string]any{rawHit("c", "2026-01-01T00:00:02Z"), rawHit("d", "2026-01-01T00:00:01Z")}
		default:
			hits = []map[string]any{}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"took":      1,
			"timed_out": false,
			"hits": map[string]any{
				"total":     map[string]any{"value": 4, "relation": "eq"},
				"max_score": 1.0,
				"hits":      hits,
			},
		})
	}))
	defer es.Close()

	_, ssvc := newQueryServiceForTest(t, es.URL)

	req := &model.RawStreamRequest{
		Cluster: "c1", Job: "1", From: "now-1h", To: "now", PageSize: 2,
	}
	var records []model.Record
	emit := func(msg model.StreamMessage) error {
		if msg.Type != model.StreamTypeRecords {
			return nil
		}
		sr := msg.Data.(model.StreamRecords)
		records = append(records, sr.Records.([]model.Record)...)
		return nil
	}

	done, err := ssvc.StreamRaw(context.Background(), req, []string{"c1"}, emit)
	if err != nil {
		t.Fatalf("StreamRaw failed: %v", err)
	}
	if done.Returned != 4 || len(records) != 4 {
		t.Fatalf("returned=%d len=%d, want 4", done.Returned, len(records))
	}
	want := []string{"2026-01-01T00:00:04Z", "2026-01-01T00:00:03Z", "2026-01-01T00:00:02Z", "2026-01-01T00:00:01Z"}
	for i := range want {
		if records[i].Time != want[i] {
			t.Fatalf("order[%d]=%s, want %s (all=%v)", i, records[i].Time, want[i], records)
		}
	}
}

func TestStreamTimeSeriesWindowed(t *testing.T) {
	var mu sync.Mutex
	calls := 0
	es := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			writeESProduct(w)
			_ = json.NewEncoder(w).Encode(map[string]any{"version": map[string]any{"number": "8.0.0"}})
			return
		}
		if !strings.HasSuffix(r.URL.Path, "_search") {
			http.NotFound(w, r)
			return
		}
		mu.Lock()
		calls++
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{
			"took":      1,
			"timed_out": false,
			"hits":      map[string]any{"total": map[string]any{"value": 10, "relation": "eq"}, "hits": []any{}},
			"aggregations": map[string]any{
				"timeseries": map[string]any{
					"buckets": []map[string]any{
						{"key_as_string": "2026-01-01T00:00:00Z", "key": 1, "doc_count": 1, "avg_cpu": map[string]any{"value": 0.5}},
						{"key_as_string": "2026-01-01T00:01:00Z", "key": 2, "doc_count": 1, "avg_cpu": map[string]any{"value": 0.7}},
					},
				},
				"stats_cpu": map[string]any{"count": 10, "min": 0.1, "max": 0.9, "avg": 0.6, "std_deviation": 0.1},
			},
		})
	}))
	defer es.Close()

	_, ssvc := newQueryServiceForTest(t, es.URL)

	req := &model.TimeSeriesStreamRequest{
		Cluster: "c1", Job: "1", Metric: "cpu", Interval: "1m", From: "now-1h", To: "now", Agg: "avg",
	}
	var records []model.TimeSeriesRecord
	emit := func(msg model.StreamMessage) error {
		if msg.Type == model.StreamTypeRecords {
			sr := msg.Data.(model.StreamRecords)
			records = append(records, sr.Records.([]model.TimeSeriesRecord)...)
		}
		return nil
	}

	done, err := ssvc.StreamTimeSeries(context.Background(), req, emit)
	if err != nil {
		t.Fatalf("StreamTimeSeries failed: %v", err)
	}
	if done.Returned != 2 || len(records) != 2 {
		t.Fatalf("returned=%d len=%d, want 2", done.Returned, len(records))
	}
	if st, ok := done.Stats["cpu"]; !ok || st.GlobalMax != 0.7 {
		t.Errorf("stats = %+v", done.Stats)
	}
}
