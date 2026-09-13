package handler

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/joblens/tap/internal/config"
	"github.com/joblens/tap/internal/model"
	"github.com/joblens/tap/internal/repository"
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

func streamErrorFrom(err error) model.StreamError {
	var backend *repository.SearchError
	if errors.As(err, &backend) {
		return model.StreamError{Code: backend.Status, Kind: backend.Kind, Message: backend.Error()}
	}
	var qe *service.QueryError
	if errors.As(err, &qe) {
		return model.StreamError{Code: qe.Status, Kind: qe.Kind, Message: qe.Msg}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return model.StreamError{Code: service.StatusGatewayTimeout, Kind: service.ErrKindQueryTimeout, Message: "stream timed out"}
	}
	return model.StreamError{Code: http.StatusInternalServerError, Kind: "stream_failed", Message: err.Error()}
}

// Raw GET /data/raw/stream
func (h *StreamHandler) Raw(c *gin.Context) {
	var req model.RawStreamRequest
	if !bindRawStream(c, &req) {
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

	if req.Cursor == "" {
		req.Cursor = c.GetHeader("Last-Event-ID")
	}
	h.runStream(c, format, func(ctx context.Context, emit service.EmitFunc) (*model.StreamDone, error) {
		return h.streamSvc.StreamRaw(ctx, &req, clusterIDs, emit)
	})
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

	if c.GetHeader("Last-Event-ID") != "" {
		respondBadRequest(c, service.ErrKindInvalidRequest, "timeseries streams do not support resume")
		return
	}
	h.runStream(c, format, func(ctx context.Context, emit service.EmitFunc) (*model.StreamDone, error) {
		return h.streamSvc.StreamTimeSeries(ctx, &req, emit)
	})
}
