package handler

import (
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
	"github.com/joblens/tap/internal/model"
)

type streamWriter struct {
	c            *gin.Context
	format       string
	mu           sync.Mutex
	failed       bool
	writeTimeout time.Duration
}

var errStreamClosed = errors.New("stream closed")

func (sw *streamWriter) write(msg model.StreamMessage) error {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	if sw.failed {
		return errStreamClosed
	}
	if err := setStreamDeadline(sw.c, sw.writeTimeout); err != nil {
		sw.failed = true
		return err
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

func (sw *streamWriter) writeEncoded(data []byte) error {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	if sw.failed {
		return errStreamClosed
	}
	if err := setStreamDeadline(sw.c, sw.writeTimeout); err != nil {
		sw.failed = true
		return err
	}
	if _, err := sw.c.Writer.Write(append(data, '\n')); err != nil {
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
	if err := setStreamDeadline(sw.c, sw.writeTimeout); err != nil {
		sw.failed = true
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

func (h *StreamHandler) beginStream(c *gin.Context, format string) *streamWriter {
	if format == model.StreamFormatSSE {
		c.Header("Content-Type", "text/event-stream")
	} else {
		c.Header("Content-Type", "application/x-ndjson")
	}
	c.Header("Cache-Control", "no-cache")
	c.Header("X-Accel-Buffering", "no")

	if err := setStreamDeadline(c, h.cfg.StreamWriteTimeout); err != nil {
		slog.Warn("stream write deadline failed", "error", err)
	}

	c.Status(http.StatusOK)
	c.Writer.Flush()

	if format == model.StreamFormatSSE {
		_, _ = io.WriteString(c.Writer, "retry: 3000\n\n")
		c.Writer.Flush()
	}
	return &streamWriter{c: c, format: format, writeTimeout: h.cfg.StreamWriteTimeout}
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

func setStreamDeadline(c *gin.Context, timeout time.Duration) error {
	if timeout <= 0 {
		return nil
	}
	err := http.NewResponseController(c.Writer).SetWriteDeadline(time.Now().Add(timeout))
	if errors.Is(err, http.ErrNotSupported) {
		return nil
	}
	return err
}
