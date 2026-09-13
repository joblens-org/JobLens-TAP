package handler

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/joblens/tap/internal/model"
	"github.com/joblens/tap/internal/service"
)

type streamEmission struct {
	message model.StreamMessage
	ack     chan error
}

type streamResult struct {
	done *model.StreamDone
	err  error
}

type streamProgress struct {
	returned  int
	bytes     int64
	cursor    string
	started   time.Time
	messages  int
	marshalNs int64
	writeNs   int64
}

func selfMemoryKB() (rss string, hwm string) {
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return "", ""
	}
	for _, line := range strings.Split(string(data), "\n") {
		switch {
		case strings.HasPrefix(line, "VmRSS:"):
			rss = strings.TrimSpace(strings.TrimPrefix(line, "VmRSS:"))
		case strings.HasPrefix(line, "VmHWM:"):
			hwm = strings.TrimSpace(strings.TrimPrefix(line, "VmHWM:"))
		}
	}
	return rss, hwm
}

func (h *StreamHandler) runStream(c *gin.Context, format string, run func(context.Context, service.EmitFunc) (*model.StreamDone, error)) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), h.cfg.StreamTimeout)
	emissions := make(chan streamEmission)
	finished := make(chan streamResult, 1)
	go func() {
		done, err := run(ctx, func(msg model.StreamMessage) error {
			emission := streamEmission{message: msg, ack: make(chan error, 1)}
			select {
			case emissions <- emission:
			case <-ctx.Done():
				return ctx.Err()
			}
			select {
			case err := <-emission.ack:
				return err
			case <-ctx.Done():
				return ctx.Err()
			}
		})
		finished <- streamResult{done, err}
	}()
	joined := false
	defer func() {
		cancel()
		if !joined {
			<-finished
		}
	}()
	interval := h.cfg.SSEHeartbeat
	if interval <= 0 {
		interval = time.Hour
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	started := time.Now()
	var sw *streamWriter
	progress := streamProgress{started: started}
	write := func(msg model.StreamMessage) error {
		if sw == nil {
			sw = h.beginStream(c, format)
		}
		return sw.write(msg)
	}
	for {
		select {
		case emission := <-emissions:
			msg := emission.message
			tMarshal := time.Now()
			data, err := json.Marshal(msg)
			progress.marshalNs += time.Since(tMarshal).Nanoseconds()
			progress.messages++
			if err == nil && progress.bytes+int64(len(data))+1 > h.cfg.StreamMaxBytes {
				err = service.NewQueryError(413, "byte_limit", "stream byte budget exhausted")
			}
			if err == nil {
				tWrite := time.Now()
				err = write(msg)
				progress.writeNs += time.Since(tWrite).Nanoseconds()
			}
			if err == nil {
				progress.bytes += int64(len(data)) + 1
				if msg.Type == model.StreamTypeRecords {
					if batch, ok := msg.Data.(model.StreamRecords); ok {
						progress.returned += batch.Returned
					}
					progress.cursor = msg.ID
				}
			}
			emission.ack <- err
			if err != nil {
				cancel()
				result := <-finished
				joined = true
				if result.err == nil {
					result.err = err
				}
				h.endStream(c, sw, result, progress)
				return
			}
		case result := <-finished:
			joined = true
			if sw == nil && result.err == nil && result.done != nil && result.done.Complete && result.done.Cursor != "" && format == model.StreamFormatSSE {
				c.Status(http.StatusNoContent)
				return
			}
			if sw == nil && result.err == nil {
				sw = h.beginStream(c, format)
			}
			h.endStream(c, sw, result, progress)
			return
		case <-ticker.C:
			if sw != nil && h.cfg.SSEHeartbeat > 0 {
				sw.ping()
			}
		case <-ctx.Done():
			cancel()
			result := <-finished
			joined = true
			if result.err == nil {
				result.err = ctx.Err()
			}
			h.endStream(c, sw, result, progress)
			return
		}
	}
}

func (h *StreamHandler) endStream(c *gin.Context, sw *streamWriter, result streamResult, progress streamProgress) {
	if result.err != nil {
		slog.Warn("stream terminated", "error", result.err, "returned", progress.returned)
		if c.Request.Context().Err() != nil {
			return
		}
		se := streamErrorFrom(result.err)
		if sw == nil {
			c.JSON(se.Code, model.Response{Code: se.Code, Message: se.Message})
			return
		}
		if err := sw.write(model.StreamMessage{Type: model.StreamTypeError, Data: se}); err != nil {
			return
		}
		status := "failed"
		if progress.returned > 0 {
			status = "partial"
		}
		result.done = &model.StreamDone{Status: status, StopReason: se.Kind}
		var qe *service.QueryError
		if errors.As(result.err, &qe) && qe.Kind == "byte_limit" {
			result.done.Truncated = true
		}
	}
	if result.done == nil {
		result.done = &model.StreamDone{Status: "success", Complete: true, StopReason: "exhausted"}
	}
	result.done.Returned, result.done.Bytes = progress.returned, progress.bytes
	result.done.DurationMs = time.Since(progress.started).Milliseconds()
	rss, hwm := selfMemoryKB()
	slog.Info("PERF stream",
		"returned", progress.returned,
		"messages", progress.messages,
		"bytes", progress.bytes,
		"marshal_ms", progress.marshalNs/1e6,
		"write_ms", progress.writeNs/1e6,
		"duration_ms", result.done.DurationMs,
		"status", result.done.Status,
		"stop_reason", result.done.StopReason,
		"vm_rss_kb", rss,
		"vm_hwm_kb", hwm,
	)
	if result.done.Cursor == "" && !result.done.Complete {
		result.done.Cursor = progress.cursor
	}
	if sw == nil {
		sw = h.beginStream(c, model.StreamFormatNDJSON)
	}
	if err := sw.write(model.StreamMessage{Type: model.StreamTypeDone, ID: result.done.Cursor, Data: result.done}); err != nil {
		slog.Debug("stream terminal write failed", "error", err)
	}
}
