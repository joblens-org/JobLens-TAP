package handler

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestRawPostResumePreservesLargeSnapshot(t *testing.T) {
	// Given：真实 ES HTTP 边界返回长 PIT，生产 handler 生成签名游标。
	gin.SetMode(gin.TestMode)
	pit := strings.Repeat("p", 16500)
	var searches atomic.Int32
	es := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		response := `{"version":{"number":"8.19.3"}}`
		switch {
		case strings.HasSuffix(r.URL.Path, "_pit"):
			response = `{"id":"` + pit + `","succeeded":true}`
		case r.URL.Path == "/_search":
			var query struct {
				PIT struct {
					ID string `json:"id"`
				} `json:"pit"`
				After []json.RawMessage `json:"search_after"`
			}
			if err := json.NewDecoder(r.Body).Decode(&query); err != nil {
				t.Error(err)
				w.WriteHeader(400)
				return
			}
			if query.PIT.ID != pit {
				t.Error("PIT 被截断或替换")
			}
			searches.Add(1)
			n := "1"
			if len(query.After) == 2 {
				if string(query.After[1]) == "2" {
					if _, err := io.WriteString(w, `{"hits":{"hits":[]}}`); err != nil {
						t.Error(err)
					}
					return
				}
				if string(query.After[1]) != "1" {
					t.Errorf("search_after=%s", query.After[1])
				}
				n = "2"
			}
			response = `{"pit_id":"` + pit + `","hits":{"hits":[{"_id":"` + n + `","_index":"cpumem_collector_2026.01.01","sort":["2026-01-01T00:00:00Z",` + n + `],"_source":{"@timestamp":"2026-01-01T00:00:00Z","hostname":"h","job_info":{"NativeJobID":"1"},"data":{"n":` + n + `}}}]}}`
		}
		if _, err := io.WriteString(w, response); err != nil {
			t.Error(err)
		}
	}))
	defer es.Close()
	h := buildStreamTestHandler(t, es.URL)
	router := gin.New()
	router.GET("/data/raw/stream", h.Raw)
	router.POST("/data/raw/stream", h.Raw)
	base := "/data/raw/stream?cluster=c1&job=1&collector=cpumem&from=2026-01-01T00:00:00Z&to=2026-01-01T00:01:00Z&page_size=1&max_records=1"
	first := httptest.NewRecorder()
	router.ServeHTTP(first, httptest.NewRequest("GET", base, nil))
	_, done := streamMessages(t, first.Body.String())
	if len(done.Cursor) < 22000 || done.Returned != 1 {
		t.Fatalf("first status=%d cursor=%d", first.Code, len(done.Cursor))
	}
	body, err := json.Marshal(struct {
		Cursor string `json:"cursor"`
	}{done.Cursor})
	if err != nil {
		t.Fatal(err)
	}
	// When：仅 cursor 放入 JSON，查询保持不变。
	w := httptest.NewRecorder()
	req := httptest.NewRequest("POST", base, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)
	// Then：原 PIT 的 search_after 恢复到第二条，不重新打开快照。
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"n":2`) {
		t.Fatalf("resume status=%d", w.Code)
	}
	_, final := streamMessages(t, w.Body.String())
	if !final.Complete || final.Returned != 1 {
		t.Fatal("第二页未完成")
	}
	t.Logf("TAP real handler cursor=%d POST request_line=%d body=%d status=%d", len(done.Cursor), len(base)+14, len(body), w.Code)
	before := searches.Load()
	for _, tc := range []struct {
		name, body, suffix string
		status             int
	}{
		{"duplicate_invalid_valid", `{"cursor":"invalid",` + string(body[1:]), "", 400},
		{"duplicate_valid_invalid", string(body[:len(body)-1]) + `,"cursor":"invalid"}`, "", 400},
		{"duplicate_escaped", `{"curs\u006fr":"invalid",` + string(body[1:]), "", 400},
		{"trailing", string(body) + `{}`, "", 400},
		{"malformed", `{"cursor":`, "", 400},
		{"empty", `{"cursor":""}`, "", 400},
		{"unknown", `{"cursor":"x","job":"other"}`, "", 400},
		{"ambiguous", string(body), "&cursor=", 400},
		{"scope", string(body), "&fields=hostname", 400},
		{"signature", `{"cursor":"invalid"}`, "", 400},
		{"large", strings.Repeat(" ", 128*1024+1), "", 413},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := searches.Load()
			w := httptest.NewRecorder()
			req := httptest.NewRequest("POST", base+tc.suffix, strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			router.ServeHTTP(w, req)
			if w.Code != tc.status || searches.Load() != before {
				t.Fatalf("status=%d searches before=%d after=%d", w.Code, before, searches.Load())
			}
		})
	}
	h.cfg.StreamPITKeepAlive = -time.Second
	expiredPage := httptest.NewRecorder()
	router.ServeHTTP(expiredPage, httptest.NewRequest("GET", base, nil))
	_, expired := streamMessages(t, expiredPage.Body.String())
	expiredBody, err := json.Marshal(struct {
		Cursor string `json:"cursor"`
	}{expired.Cursor})
	if err != nil {
		t.Fatal(err)
	}
	before = searches.Load()
	expiredRequest := httptest.NewRequest("POST", base, bytes.NewReader(expiredBody))
	expiredRequest.Header.Set("Content-Type", "application/json")
	expiredResponse := httptest.NewRecorder()
	router.ServeHTTP(expiredResponse, expiredRequest)
	if expiredResponse.Code != 410 || searches.Load() != before {
		t.Fatalf("expired status=%d searches=%d", expiredResponse.Code, searches.Load())
	}
}
