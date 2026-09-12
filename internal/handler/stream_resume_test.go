package handler

import (
	"bufio"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/joblens/tap/internal/model"
)

func resumeBackend(t *testing.T) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	opens := &atomic.Int32{}
	es := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		if r.URL.Path == "/" {
			_ = json.NewEncoder(w).Encode(map[string]any{"version": map[string]any{"number": "8.19.3"}})
			return
		}
		if strings.HasSuffix(r.URL.Path, "_pit") {
			if r.Method == http.MethodPost {
				opens.Add(1)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"id": "pit-initial", "succeeded": true})
			return
		}
		var query struct {
			After []json.RawMessage `json:"search_after"`
			Size  int               `json:"size"`
			PIT   struct {
				ID string `json:"id"`
			} `json:"pit"`
		}
		if err := json.NewDecoder(r.Body).Decode(&query); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		if query.PIT.ID == "" || r.URL.Path != "/_search" {
			t.Errorf("未使用PIT搜索: %s %+v", r.URL.Path, query)
		}
		after := 0
		if len(query.After) > 1 {
			if err := json.Unmarshal(query.After[1], &after); err != nil {
				t.Error(err)
			}
		}
		hits := []map[string]any{}
		for i := after + 1; i <= 3 && len(hits) < query.Size; i++ {
			hits = append(hits, map[string]any{"_id": strconv.Itoa(i), "_index": "cpumem_collector_2026.01.01", "sort": []any{"2026-01-01T00:00:00Z", i}, "_source": map[string]any{"@timestamp": "2026-01-01T00:00:00Z", "hostname": "h", "job_info": map[string]any{"NativeJobID": "1"}, "data": map[string]any{"n": i}}})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"pit_id": "pit-current", "hits": map[string]any{"hits": hits}})
	}))
	t.Cleanup(es.Close)
	return es, opens
}

func streamMessages(t *testing.T, body string) ([]model.StreamMessage, model.StreamDone) {
	t.Helper()
	var messages []model.StreamMessage
	var done model.StreamDone
	scanner := bufio.NewScanner(strings.NewReader(body))
	for scanner.Scan() {
		var msg struct {
			Type string          `json:"type"`
			ID   string          `json:"id"`
			Data json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &msg); err != nil {
			t.Fatal(err)
		}
		messages = append(messages, model.StreamMessage{Type: msg.Type, ID: msg.ID})
		if msg.Type == model.StreamTypeDone {
			if err := json.Unmarshal(msg.Data, &done); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return messages, done
}

func TestRawResumeUsesSameSnapshotAndTerminalSSEStops(t *testing.T) {
	gin.SetMode(gin.TestMode)
	es, opens := resumeBackend(t)
	h := buildStreamTestHandler(t, es.URL)
	router := gin.New()
	router.GET("/raw", h.Raw)
	base := "/raw?cluster=c1&job=1&from=2026-01-01T00:00:00Z&to=2026-01-01T00:01:00Z&page_size=2"
	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest("GET", base+"&max_records=1", nil))
	_, first := streamMessages(t, w.Body.String())
	if first.Returned != 1 || !first.Truncated || first.Cursor == "" {
		t.Fatalf("第一批 %s", w.Body.String())
	}
	w = httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest("GET", base+"&cursor="+url.QueryEscape(first.Cursor), nil))
	_, final := streamMessages(t, w.Body.String())
	if final.Returned != 2 || !final.Complete || final.Cursor == "" || opens.Load() != 1 {
		t.Fatalf("恢复结果 %s opens=%d", w.Body.String(), opens.Load())
	}
	w = httptest.NewRecorder()
	req := httptest.NewRequest("GET", base+"&format=sse", nil)
	req.Header.Set("Last-Event-ID", final.Cursor)
	router.ServeHTTP(w, req)
	if w.Code != 204 || w.Body.Len() != 0 {
		t.Fatalf("终态重连 %d %s", w.Code, w.Body.String())
	}
}

func TestStreamInvalidTimeReturnsHTTP400BeforeHeaders(t *testing.T) {
	gin.SetMode(gin.TestMode)
	es, opens := resumeBackend(t)
	h := buildStreamTestHandler(t, es.URL)
	router := gin.New()
	router.GET("/raw", h.Raw)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest("GET", "/raw?cluster=c1&job=1&from=bad", nil))
	if w.Code != 400 || opens.Load() != 0 {
		t.Fatalf("status=%d opens=%d body=%s", w.Code, opens.Load(), w.Body.String())
	}
}
