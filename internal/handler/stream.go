package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/joblens/tap/internal/config"
	"github.com/joblens/tap/internal/model"
	"github.com/joblens/tap/internal/service"
)

// StreamHandler 流式查询处理器（NDJSON / SSE）
type StreamHandler struct {
	streamSvc *service.StreamService
	querySvc  *service.QueryService
	limiter   *service.Limiter
	cfg       *config.Config
}

func NewStreamHandler(streamSvc *service.StreamService, querySvc *service.QueryService, limiter *service.Limiter, cfg *config.Config) *StreamHandler {
	return &StreamHandler{streamSvc: streamSvc, querySvc: querySvc, limiter: limiter, cfg: cfg}
}

type streamWriter struct {
	c      *gin.Context
	format string
	mu     sync.Mutex
	failed bool
}

var errStreamClosed = errors.New("stream closed")

func (sw *streamWriter) write(msg model.StreamMessage) error {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	if sw.failed {
		return errStreamClosed
	}

	var err error
	if sw.format == model.StreamFormatSSE {
		err = sw.writeSSE(msg)
	} else {
		err = sw.writeNDJSON(msg)
	}
	if err != nil {
		sw.failed = true
		return err
	}
	sw.c.Writer.Flush()
	return nil
}

func (sw *streamWriter) writeSSE(msg model.StreamMessage) error {
	payload, err := json.Marshal(msg.Data)
	if err != nil {
		return err
	}
	if msg.ID != "" {
		if _, err := fmt.Fprintf(sw.c.Writer, "id: %s\n", msg.ID); err != nil {
			return err
		}
	}
	_, err = fmt.Fprintf(sw.c.Writer, "event: %s\ndata: %s\n\n", msg.Type, payload)
	return err
}

func (sw *streamWriter) writeNDJSON(msg model.StreamMessage) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	_, err = sw.c.Writer.Write(data)
	return err
}

func (sw *streamWriter) ping() {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	if sw.failed {
		return
	}
	var err error
	if sw.format == model.StreamFormatSSE {
		_, err = io.WriteString(sw.c.Writer, ": ping\n\n")
	} else {
		_, err = io.WriteString(sw.c.Writer, "{\"type\":\"ping\"}\n")
	}
	if err != nil {
		sw.failed = true
		return
	}
	sw.c.Writer.Flush()
}

func resolveStreamFormat(c *gin.Context, requested string) (string, error) {
	switch requested {
	case model.StreamFormatSSE:
		return model.StreamFormatSSE, nil
	case model.StreamFormatNDJSON:
		return model.StreamFormatNDJSON, nil
	case "":
	default:
		return "", fmt.Errorf("unsupported format: %s", requested)
	}
	if strings.Contains(c.GetHeader("Accept"), "text/event-stream") {
		return model.StreamFormatSSE, nil
	}
	return model.StreamFormatNDJSON, nil
}

func (h *StreamHandler) beginStream(c *gin.Context, format string) *streamWriter {
	if format == model.StreamFormatSSE {
		c.Header("Content-Type", "text/event-stream")
		c.Header("Connection", "keep-alive")
	} else {
		c.Header("Content-Type", "application/x-ndjson")
	}
	c.Header("Cache-Control", "no-cache")
	c.Header("X-Accel-Buffering", "no")

	rc := http.NewResponseController(c.Writer)
	_ = rc.SetWriteDeadline(time.Now().Add(h.cfg.StreamTimeout + 30*time.Second))

	c.Status(http.StatusOK)
	c.Writer.Flush()

	if format == model.StreamFormatSSE {
		_, _ = io.WriteString(c.Writer, "retry: 3000\n\n")
		c.Writer.Flush()
	}
	return &streamWriter{c: c, format: format}
}

func (h *StreamHandler) heartbeat(ctx context.Context, sw *streamWriter) {
	interval := h.cfg.SSEHeartbeat
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sw.ping()
		}
	}
}

func streamErrorFrom(err error) model.StreamError {
	var qe *service.QueryError
	if errors.As(err, &qe) {
		return model.StreamError{Code: qe.Status, Kind: qe.Kind, Message: qe.Msg}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return model.StreamError{Code: service.StatusGatewayTimeout, Kind: service.ErrKindQueryTimeout, Message: "stream timed out"}
	}
	return model.StreamError{Code: http.StatusInternalServerError, Kind: "stream_failed", Message: err.Error()}
}

func (h *StreamHandler) finishWithError(sw *streamWriter, err error, returned int, started time.Time) {
	se := streamErrorFrom(err)
	_ = sw.write(model.StreamMessage{Type: model.StreamTypeError, Data: se})
	_ = sw.write(model.StreamMessage{Type: model.StreamTypeDone, Data: model.StreamDone{
		Returned:   returned,
		DurationMs: time.Since(started).Milliseconds(),
		Truncated:  true,
	}})
}

// Raw GET /data/raw/stream
func (h *StreamHandler) Raw(c *gin.Context) {
	var req model.RawStreamRequest
	if err := c.ShouldBindQuery(&req); err != nil {
		respondBadRequest(c, service.ErrKindInvalidRequest, "invalid request: "+err.Error())
		return
	}
	if req.To == "" {
		req.To = "now"
	}
	if !req.FullRange && req.From == "" {
		respondBadRequest(c, service.ErrKindInvalidRequest, "from parameter is required when full_range is not set")
		return
	}

	format, err := resolveStreamFormat(c, req.Format)
	if err != nil {
		respondBadRequest(c, service.ErrKindInvalidRequest, err.Error())
		return
	}

	clusterIDs, err := h.querySvc.IndexService().ParseClusterParam(req.Cluster)
	if err != nil || len(clusterIDs) == 0 {
		respondBadRequest(c, service.ErrKindInvalidRequest, "invalid cluster parameter")
		return
	}

	if !h.limiter.Acquire() {
		c.Set("error_kind", service.ErrKindTooManyRequests)
		c.JSON(http.StatusTooManyRequests, model.Response{
			Code:    http.StatusTooManyRequests,
			Message: "too many concurrent streams",
		})
		return
	}
	defer h.limiter.Release()

	slog.Info("[StreamHandler.Raw] request",
		"cluster", req.Cluster,
		"job", req.Job,
		"format", format,
		"full_range", req.FullRange,
		"page_size", req.PageSize,
	)

	started := time.Now()
	sw := h.beginStream(c, format)

	ctx, cancel := context.WithTimeout(c.Request.Context(), h.cfg.StreamTimeout)
	defer cancel()
	go h.heartbeat(ctx, sw)

	done, err := h.streamSvc.StreamRaw(ctx, &req, clusterIDs, sw.write)
	if err != nil {
		h.finishWithError(sw, err, 0, started)
		return
	}
	if err := sw.write(model.StreamMessage{Type: model.StreamTypeDone, Data: done}); err != nil {
		return
	}

	slog.Info("[StreamHandler.Raw] completed",
		"cluster", req.Cluster,
		"job", req.Job,
		"returned", done.Returned,
		"truncated", done.Truncated,
	)
}

// TimeSeries GET /data/timeseries/stream
func (h *StreamHandler) TimeSeries(c *gin.Context) {
	var req model.TimeSeriesStreamRequest
	if err := c.ShouldBindQuery(&req); err != nil {
		respondBadRequest(c, service.ErrKindInvalidRequest, "invalid request: "+err.Error())
		return
	}
	if req.To == "" {
		req.To = "now"
	}
	if req.Agg == "" {
		req.Agg = "avg"
	}

	format, err := resolveStreamFormat(c, req.Format)
	if err != nil {
		respondBadRequest(c, service.ErrKindInvalidRequest, err.Error())
		return
	}

	clusterIDs, err := h.querySvc.IndexService().ParseClusterParam(req.Cluster)
	if err != nil || len(clusterIDs) == 0 {
		respondBadRequest(c, service.ErrKindInvalidRequest, "invalid cluster parameter")
		return
	}
	if len(clusterIDs) > 1 {
		respondBadRequest(c, service.ErrKindInvalidRequest, "timeseries query only supports single cluster, please specify one cluster")
		return
	}

	if !h.limiter.Acquire() {
		c.Set("error_kind", service.ErrKindTooManyRequests)
		c.JSON(http.StatusTooManyRequests, model.Response{
			Code:    http.StatusTooManyRequests,
			Message: "too many concurrent streams",
		})
		return
	}
	defer h.limiter.Release()

	slog.Info("[StreamHandler.TimeSeries] request",
		"cluster", req.Cluster,
		"job", req.Job,
		"metric", req.Metric,
		"interval", req.Interval,
		"format", format,
	)

	started := time.Now()
	sw := h.beginStream(c, format)

	ctx, cancel := context.WithTimeout(c.Request.Context(), h.cfg.StreamTimeout)
	defer cancel()
	go h.heartbeat(ctx, sw)

	done, err := h.streamSvc.StreamTimeSeries(ctx, &req, sw.write)
	if err != nil {
		h.finishWithError(sw, err, 0, started)
		return
	}
	if err := sw.write(model.StreamMessage{Type: model.StreamTypeDone, Data: done}); err != nil {
		return
	}

	slog.Info("[StreamHandler.TimeSeries] completed",
		"cluster", req.Cluster,
		"job", req.Job,
		"returned", done.Returned,
		"truncated", done.Truncated,
	)
}
