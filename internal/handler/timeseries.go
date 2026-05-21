package handler

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/joblens/tap/internal/model"
	"github.com/joblens/tap/internal/repository"
	"github.com/joblens/tap/internal/service"
)

// TimeSeriesHandler 时序数据查询处理器
type TimeSeriesHandler struct {
	esManager *repository.ClientManager
	querySvc  *service.QueryService
}

// NewTimeSeriesHandler 创建时序数据查询处理器
func NewTimeSeriesHandler(esManager *repository.ClientManager, querySvc *service.QueryService) *TimeSeriesHandler {
	return &TimeSeriesHandler{
		esManager: esManager,
		querySvc:  querySvc,
	}
}

// Query 时序数据查询
// GET /data/timeseries
func (h *TimeSeriesHandler) Query(c *gin.Context) {
	var req model.TimeSeriesRequest
	if err := c.ShouldBindQuery(&req); err != nil {
		c.JSON(http.StatusBadRequest, model.Response{
			Code:    400,
			Message: "invalid request: " + err.Error(),
		})
		return
	}

	// 设置默认值
	if req.To == "" {
		req.To = "now"
	}
	if req.Agg == "" {
		req.Agg = "avg"
	}

	slog.Debug("[TimeSeriesHandler.Query] processing",
		"cluster", req.Cluster,
		"job", req.Job,
		"metric", req.Metric,
		"from", req.From,
		"to", req.To,
		"interval", req.Interval,
		"agg", req.Agg,
		"by", req.By,
	)

	slog.Info("[TimeSeriesHandler.Query] request",
		"cluster", req.Cluster,
		"job", req.Job,
		"metric", req.Metric,
		"interval", req.Interval,
		"from", req.From,
		"to", req.To,
	)

	startTime := time.Now()

	// 1. 解析集群 ID
	clusterIDs, err := h.querySvc.IndexService().ParseClusterParam(req.Cluster)
	if err != nil {
		c.JSON(http.StatusBadRequest, model.Response{
			Code:    400,
			Message: "invalid cluster parameter: " + err.Error(),
		})
		return
	}

	// Timeseries 接口建议单集群查询
	if len(clusterIDs) > 1 {
		c.JSON(http.StatusBadRequest, model.Response{
			Code:    400,
			Message: "timeseries query only supports single cluster, please specify one cluster",
		})
		return
	}

	// 2. 解析 metrics
	metrics := h.querySvc.ParseMetrics(req.Metric)
	if len(metrics) == 0 {
		c.JSON(http.StatusBadRequest, model.Response{
			Code:    400,
			Message: "metric parameter is required",
		})
		return
	}

	// 3. 执行时序查询
	resp, err := h.querySvc.ExecuteMultiMetricTimeSeriesQuery(c.Request.Context(), clusterIDs[0], &req, metrics)
	if err != nil {
		slog.Error("timeseries query failed",
			"cluster", req.Cluster,
			"job", req.Job,
			"metric", req.Metric,
			"error", err,
		)
		c.Set("error_kind", "timeseries_query_failed")
		c.Set("error_detail", err.Error())
		c.JSON(http.StatusInternalServerError, model.Response{
			Code:    500,
			Message: "timeseries query failed: " + err.Error(),
		})
		return
	}

	// 4. 返回响应
	slog.Debug("[TimeSeriesHandler.Query] completed",
		"query_time_ms", int(time.Since(startTime).Milliseconds()),
		"record_count", len(resp.Records),
	)

	slog.Info("[TimeSeriesHandler.Query] completed",
		"cluster", req.Cluster,
		"job", req.Job,
		"metrics", resp.Metrics,
		"records", len(resp.Records),
		"took_ms", int(time.Since(startTime).Milliseconds()),
	)

	c.JSON(http.StatusOK, model.Response{
		Code:    0,
		Message: "success",
		Data:    resp,
		Meta: &model.Meta{
			QueryTimeMs:     int(time.Since(startTime).Milliseconds()),
			ClustersQueried: clusterIDs,
			IndicesHit:      []string{},
		},
	})
}
