package handler

import (
	"bufio"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/joblens/tap/internal/service"
)

func TestStreamRawByteBudgetRejectsBeforeHeaders(t *testing.T) {
	gin.SetMode(gin.TestMode)
	es, _ := resumeBackend(t)
	h := buildStreamTestHandler(t, es.URL)
	h.cfg.StreamMaxBytes = 1

	router := gin.New()
	router.GET("/raw", h.Raw)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest("GET", "/raw?cluster=c1&job=1&from=2026-01-01T00:00:00Z&to=2026-01-01T00:01:00Z", nil))

	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413 body=%s", w.Code, w.Body.String())
	}
	var body struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil || !strings.Contains(body.Message, "byte budget") {
		t.Fatalf("body = %s err=%v", w.Body.String(), err)
	}
}

func TestStreamRawClientDisconnectReleasesSlot(t *testing.T) {
	gin.SetMode(gin.TestMode)
	releaseBackend := make(chan struct{})
	searchStarted := make(chan struct{}, 4)
	es := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		switch {
		case r.URL.Path == "/":
			_ = json.NewEncoder(w).Encode(map[string]any{"version": map[string]any{"number": "8.19.3"}})
		case strings.HasSuffix(r.URL.Path, "_pit"):
			_ = json.NewEncoder(w).Encode(map[string]any{"id": "pit-1", "succeeded": true})
		case strings.HasSuffix(r.URL.Path, "_search"):
			select {
			case searchStarted <- struct{}{}:
			default:
			}
			select {
			case <-releaseBackend:
			case <-time.After(5 * time.Second):
			}
			w.WriteHeader(http.StatusInternalServerError)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(es.Close)

	h := buildStreamTestHandler(t, es.URL)
	h.limiter = service.NewLimiter(1)
	router := gin.New()
	router.GET("/raw", h.Raw)
	srv := httptest.NewServer(router)
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(releaseBackend) })

	resp, err := http.Get(srv.URL + "/raw?cluster=c1&job=1&from=2026-01-01T00:00:00Z&to=2026-01-01T00:01:00Z&page_size=1")
	if err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(resp.Body)
	if _, err := reader.ReadString('\n'); err != nil {
		_ = resp.Body.Close()
		t.Fatalf("read meta: %v", err)
	}
	select {
	case <-searchStarted:
	case <-time.After(3 * time.Second):
		_ = resp.Body.Close()
		t.Fatal("backend search did not start")
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatalf("close body: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for {
		if h.limiter.Acquire() {
			h.limiter.Release()
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("stream did not release its concurrency slot after client disconnect")
		}
		time.Sleep(20 * time.Millisecond)
	}
}
