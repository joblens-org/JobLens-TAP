package service

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/joblens/tap/internal/model"
)

type windowCapture struct {
	gte       string
	lt        string
	lte       string
	boundsMax string
}

func TestStreamTimeSeriesAlignedWindowsAndWeightedStats(t *testing.T) {
	var mu sync.Mutex
	var captured []windowCapture
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
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		filters, _ := body["query"].(map[string]any)["bool"].(map[string]any)["filter"].([]any)
		capture := windowCapture{}
		for _, raw := range filters {
			filter, _ := raw.(map[string]any)
			rangeFilter, _ := filter["range"].(map[string]any)
			if rangeFilter == nil {
				continue
			}
			bounds, _ := rangeFilter["@timestamp"].(map[string]any)
			capture.gte, _ = bounds["gte"].(string)
			capture.lt, _ = bounds["lt"].(string)
			capture.lte, _ = bounds["lte"].(string)
		}
		aggs, _ := body["aggs"].(map[string]any)
		histogram, _ := aggs["timeseries"].(map[string]any)["date_histogram"].(map[string]any)
		capture.boundsMax, _ = histogram["extended_bounds"].(map[string]any)["max"].(string)
		mu.Lock()
		captured = append(captured, capture)
		mu.Unlock()

		buckets := []map[string]any{}
		stats := map[string]any{"count": 0.0, "sum": 0.0, "max": 0.0}
		switch capture.gte {
		case "2026-01-01T00:00:30Z":
			buckets = []map[string]any{{"key_as_string": "2026-01-01T00:00:00Z", "key": 0, "doc_count": 1, "avg_cpu": map[string]any{"value": 100.0}}}
			stats = map[string]any{"count": 1.0, "sum": 100.0, "max": 100.0}
		case "2026-01-01T00:02:00Z":
			buckets = []map[string]any{{"key_as_string": "2026-01-01T00:02:00Z", "key": 120000, "doc_count": 99, "avg_cpu": map[string]any{"value": 0.0}}}
			stats = map[string]any{"count": 99.0, "sum": 0.0, "max": 0.0}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"took":      1,
			"timed_out": false,
			"hits":      map[string]any{"total": map[string]any{"value": 0, "relation": "eq"}, "hits": []any{}},
			"aggregations": map[string]any{
				"timeseries": map[string]any{"buckets": buckets},
				"stats_cpu":  stats,
			},
		})
	}))
	defer es.Close()

	_, ssvc := newQueryServiceForTest(t, es.URL)
	req := &model.TimeSeriesStreamRequest{
		Cluster: "c1", Job: "1", Metric: "cpu", Interval: "1m",
		From: "2026-01-01T00:00:30Z", To: "2026-01-01T00:04:30Z",
		Agg: "avg", WindowBuckets: 2,
	}
	seen := map[string]int{}
	emit := func(msg model.StreamMessage) error {
		if msg.Type != model.StreamTypeRecords {
			return nil
		}
		sr := msg.Data.(model.StreamRecords)
		for _, record := range sr.Records.([]model.TimeSeriesRecord) {
			seen[record.Timestamp]++
		}
		return nil
	}

	done, err := ssvc.StreamTimeSeries(context.Background(), req, emit)
	if err != nil {
		t.Fatalf("StreamTimeSeries failed: %v", err)
	}
	if len(captured) != 3 {
		t.Fatalf("windows queried = %d, want 3 (%+v)", len(captured), captured)
	}
	if captured[0].lt != "2026-01-01T00:02:00Z" || captured[1].lt != "2026-01-01T00:04:00Z" {
		t.Fatalf("non-last windows must use exclusive lt: %+v", captured)
	}
	if captured[2].lte != "2026-01-01T00:04:30Z" || captured[2].lt != "" {
		t.Fatalf("last window must keep inclusive lte: %+v", captured[2])
	}
	if captured[0].boundsMax != "2026-01-01T00:01:59.999Z" {
		t.Fatalf("window bounds must not leak into the next bucket: %q", captured[0].boundsMax)
	}
	for timestamp, count := range seen {
		if count != 1 {
			t.Fatalf("bucket %s emitted %d times, want once", timestamp, count)
		}
	}
	if len(seen) != 2 {
		t.Fatalf("buckets = %v, want 2 unique", seen)
	}
	stats, ok := done.Stats["cpu"]
	if !ok || stats.GlobalMax != 100 || stats.GlobalAvg != 1 {
		t.Fatalf("weighted stats = %+v, want max=100 avg=1", done.Stats)
	}
	if !done.Complete || done.Truncated || done.Status != "success" {
		t.Fatalf("done = %+v", done)
	}
}
