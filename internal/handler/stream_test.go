package handler

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/joblens/tap/internal/config"
	"github.com/joblens/tap/internal/model"
	"github.com/joblens/tap/internal/service"
)

func newTestStreamHandler() *StreamHandler {
	return &StreamHandler{
		limiter: service.NewLimiter(4),
		cfg:     &config.Config{StreamTimeout: time.Minute, SSEHeartbeat: 0},
	}
}

func TestResolveStreamFormat(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name      string
		requested string
		accept    string
		want      string
		wantErr   bool
	}{
		{"explicit sse", model.StreamFormatSSE, "", model.StreamFormatSSE, false},
		{"explicit ndjson", model.StreamFormatNDJSON, "text/event-stream", model.StreamFormatNDJSON, false},
		{"accept sse", "", "text/event-stream", model.StreamFormatSSE, false},
		{"default ndjson", "", "application/json", model.StreamFormatNDJSON, false},
		{"invalid", "xml", "", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest("GET", "/", nil)
			if tt.accept != "" {
				c.Request.Header.Set("Accept", tt.accept)
			}
			got, err := resolveStreamFormat(c, tt.requested)
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error for %q", tt.requested)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("resolveStreamFormat = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestStreamWriterNDJSON(t *testing.T) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	sw := &streamWriter{c: c, format: model.StreamFormatNDJSON}

	if err := sw.write(model.StreamMessage{Type: model.StreamTypeRecords, Data: map[string]any{"n": 1}}); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	body := w.Body.String()
	if !strings.Contains(body, `"type":"records"`) || !strings.Contains(body, `"n":1`) {
		t.Errorf("unexpected ndjson body: %q", body)
	}
	if !strings.HasSuffix(body, "\n") {
		t.Errorf("ndjson line must end with newline: %q", body)
	}
}

func TestStreamWriterSSE(t *testing.T) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	sw := &streamWriter{c: c, format: model.StreamFormatSSE}

	if err := sw.write(model.StreamMessage{Type: model.StreamTypeRecords, ID: "abc", Data: map[string]any{"n": 1}}); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	body := w.Body.String()
	if !strings.Contains(body, "id: abc\n") {
		t.Errorf("missing id line: %q", body)
	}
	if !strings.Contains(body, "event: records\n") || !strings.HasSuffix(body, "\n\n") {
		t.Errorf("unexpected sse body: %q", body)
	}
}

func TestStreamErrorFrom(t *testing.T) {
	qe := service.NewQueryError(service.StatusBadRequest, service.ErrKindTooManyBuckets, "too many")
	se := streamErrorFrom(qe)
	if se.Code != service.StatusBadRequest || se.Kind != service.ErrKindTooManyBuckets {
		t.Errorf("query error mapping = %+v", se)
	}

	se = streamErrorFrom(context.DeadlineExceeded)
	if se.Code != service.StatusGatewayTimeout || se.Kind != service.ErrKindQueryTimeout {
		t.Errorf("deadline mapping = %+v", se)
	}

	se = streamErrorFrom(errors.New("boom"))
	if se.Code != http.StatusInternalServerError {
		t.Errorf("generic mapping = %+v", se)
	}
}

func TestStreamHandlerRawValidation(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := newTestStreamHandler()
	r := gin.New()
	r.GET("/data/raw/stream", h.Raw)

	tests := []struct {
		name string
		url  string
	}{
		{"missing from", "/data/raw/stream?cluster=c1&job=j1"},
		{"bad format", "/data/raw/stream?cluster=c1&job=j1&from=now-1h&format=xml"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			req := httptest.NewRequest("GET", tt.url, nil)
			r.ServeHTTP(w, req)
			if w.Code != http.StatusBadRequest {
				t.Errorf("expected 400, got %d body=%s", w.Code, w.Body.String())
			}
		})
	}
}

func TestStreamHandlerTimeSeriesValidation(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := newTestStreamHandler()
	r := gin.New()
	r.GET("/data/timeseries/stream", h.TimeSeries)

	tests := []struct {
		name string
		url  string
	}{
		{"missing metric", "/data/timeseries/stream?cluster=c1&job=j1&interval=1m&from=now-1h"},
		{"missing interval", "/data/timeseries/stream?cluster=c1&job=j1&metric=cpu&from=now-1h"},
		{"bad format", "/data/timeseries/stream?cluster=c1&job=j1&metric=cpu&interval=1m&from=now-1h&format=xml"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			req := httptest.NewRequest("GET", tt.url, nil)
			r.ServeHTTP(w, req)
			if w.Code != http.StatusBadRequest {
				t.Errorf("expected 400, got %d body=%s", w.Code, w.Body.String())
			}
		})
	}
}
