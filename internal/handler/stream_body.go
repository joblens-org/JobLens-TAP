package handler

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/joblens/tap/internal/model"
	"github.com/joblens/tap/internal/service"
)

const resumeBodyLimit = 128 << 10

func bindRawStream(c *gin.Context, req *model.RawStreamRequest) bool {
	if err := c.ShouldBindQuery(req); err != nil {
		respondBadRequest(c, service.ErrKindInvalidRequest, "invalid query parameters")
		return false
	}
	if c.Request.Method != http.MethodPost {
		return true
	}
	if c.Request.URL.Query().Has("cursor") || c.GetHeader("Last-Event-ID") != "" {
		respondBadRequest(c, service.ErrKindInvalidRequest, "POST cursor must only appear in the JSON body")
		return false
	}
	if c.ContentType() != "application/json" || c.GetHeader("Content-Encoding") != "" {
		c.AbortWithStatus(http.StatusUnsupportedMediaType)
		return false
	}
	body, err := io.ReadAll(http.MaxBytesReader(c.Writer, c.Request.Body, resumeBodyLimit))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			c.AbortWithStatus(http.StatusRequestEntityTooLarge)
		} else {
			respondBadRequest(c, service.ErrKindInvalidRequest, "invalid request body")
		}
		return false
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	opening, err := decoder.Token()
	if err != nil || opening != json.Delim('{') {
		respondBadRequest(c, service.ErrKindInvalidRequest, "body must contain only cursor")
		return false
	}
	key, err := decoder.Token()
	if err != nil || key != "cursor" {
		respondBadRequest(c, service.ErrKindInvalidRequest, "body must contain only cursor")
		return false
	}
	var cursor string
	if decoder.Decode(&cursor) != nil || cursor == "" || len(cursor) > 64<<10 {
		respondBadRequest(c, service.ErrKindInvalidRequest, "invalid cursor")
		return false
	}
	closing, err := decoder.Token()
	if err != nil || closing != json.Delim('}') {
		respondBadRequest(c, service.ErrKindInvalidRequest, "body must contain only one cursor")
		return false
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		respondBadRequest(c, service.ErrKindInvalidRequest, "invalid trailing body")
		return false
	}
	req.Cursor = cursor
	return true
}
