package handler

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestStreamHTTPIndexPolicy_whenDatesOrAvailabilityDiffer(t *testing.T) {
	for _, tc := range []struct {
		name, from, to, indexPath string
		backendStatus             int
	}{
		{"missing_today_with_history", "2026-09-12T02:00:00Z", "2026-09-13T03:00:00Z", "/cpumem_collector_2026.09.12,cpumem_collector_2026.09.13/_pit", 200},
		{"mixed_offset", "2026-09-12T02:00:00+00:00", "2026-09-12T11:00:00+08:00", "/cpumem_collector_2026.09.12/_pit", 200},
		{"east_midnight", "2026-09-12T23:30:00+08:00", "2026-09-13T00:30:00+08:00", "/cpumem_collector_2026.09.12/_pit", 200},
		{"all_missing", "2026-09-12T02:00:00Z", "2026-09-12T03:00:00Z", "/cpumem_collector_2026.09.12/_pit", 404},
		{"unavailable_shards", "2026-09-12T02:00:00Z", "2026-09-12T03:00:00Z", "/cpumem_collector_2026.09.12/_pit", 503},
		{"forbidden", "2026-09-12T02:00:00Z", "2026-09-12T03:00:00Z", "/cpumem_collector_2026.09.12/_pit", 403},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// 给定：真实 TAP handler 与按索引和参数执行策略的 HTTP ES 替身。
			es := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("X-Elastic-Product", "Elasticsearch")
				body := `{"version":{"number":"8.19.3"}}`
				switch {
				case r.URL.Path == "/":
				case r.Method == http.MethodDelete && r.URL.Path == "/_pit":
					body = `{"succeeded":true}`
				case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/_pit"):
					q := r.URL.Query()
					if r.URL.Path != tc.indexPath || q.Has("allow_no_indices") || q.Get("allow_partial_search_results") != "false" {
						t.Errorf("unexpected PIT request: %s", r.URL)
						w.WriteHeader(400)
						return
					}
					status := tc.backendStatus
					if q.Get("ignore_unavailable") != "true" && strings.Contains(r.URL.Path, ",") {
						status = 404
					}
					w.WriteHeader(status)
					body = `{"id":"history-pit"}`
				case r.URL.Path == "/_search":
					if r.URL.Query().Get("allow_partial_search_results") != "false" {
						t.Error("partial search enabled")
					}
					body = `{"hits":{"hits":[{"_id":"history","_index":"cpumem_collector_2026.09.12","sort":["2026-09-12T02:30:00Z",1],"_source":{"@timestamp":"2026-09-12T02:30:00Z","hostname":"h","data":{}}}]}}`
				default:
					t.Errorf("unexpected ES request: %s", r.URL)
					w.WriteHeader(400)
					return
				}
				if _, err := io.WriteString(w, body); err != nil {
					t.Error(err)
				}
			}))
			defer es.Close()
			h := buildStreamTestHandler(t, es.URL)
			router := gin.New()
			router.GET("/data/raw/stream", h.Raw)
			tap := httptest.NewServer(router)
			defer tap.Close()
			query := url.Values{"cluster": {"c1"}, "job": {"1"}, "collector": {"cpumem"}, "from": {tc.from}, "to": {tc.to}, "page_size": {"2"}}
			client := tap.Client()
			client.Timeout = 10 * time.Second
			// 当：通过真实 TCP HTTP 请求流式入口，正确转义正号。
			resp, err := client.Get(tap.URL + "/data/raw/stream?" + query.Encode())
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			// 则：PIT 建立前失败保留 HTTP 错误；成功流含历史记录和 done。
			if tc.backendStatus != 200 {
				var failure struct {
					Code int `json:"code"`
				}
				if err := json.NewDecoder(resp.Body).Decode(&failure); err != nil {
					t.Fatal(err)
				}
				if resp.StatusCode != tc.backendStatus || failure.Code != tc.backendStatus {
					t.Fatalf("HTTP=%d code=%d want=%d", resp.StatusCode, failure.Code, tc.backendStatus)
				}
				t.Logf("HTTP=%d code=%d; no stream started", resp.StatusCode, failure.Code)
				return
			}
			decoder := json.NewDecoder(resp.Body)
			records, done, failures := 0, 0, 0
			for {
				var event struct {
					Type string `json:"type"`
					Data struct {
						Records []json.RawMessage `json:"records"`
						Code    int               `json:"code"`
					} `json:"data"`
				}
				if err := decoder.Decode(&event); err == io.EOF {
					break
				} else if err != nil {
					t.Fatal(err)
				}
				switch event.Type {
				case "records":
					records += len(event.Data.Records)
				case "done":
					done++
				case "error":
					failures++
					if event.Data.Code != tc.backendStatus {
						t.Errorf("error code=%d want=%d", event.Data.Code, tc.backendStatus)
					}
				}
			}
			if tc.backendStatus == 200 {
				if records != 1 || done != 1 || failures != 0 {
					t.Fatalf("HTTP=%d records=%d done=%d errors=%d", resp.StatusCode, records, done, failures)
				}
			} else if failures != 1 || done != 0 || records != 0 {
				t.Fatalf("HTTP=%d records=%d done=%d errors=%d", resp.StatusCode, records, done, failures)
			}
			t.Logf("HTTP=%d records=%d done=%d errors=%d", resp.StatusCode, records, done, failures)
		})
	}
}
