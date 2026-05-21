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

// RawHandler 原始数据查询处理器
type RawHandler struct {
	esManager *repository.ClientManager
	querySvc  *service.QueryService
}

// NewRawHandler 创建原始数据查询处理器
func NewRawHandler(esManager *repository.ClientManager, querySvc *service.QueryService) *RawHandler {
	return &RawHandler{
		esManager: esManager,
		querySvc:  querySvc,
	}
}

// Query 原始数据查询
// GET /data/raw
func (h *RawHandler) Query(c *gin.Context) {
	var req model.RawQueryRequest
	if err := c.ShouldBindQuery(&req); err != nil {
		c.JSON(http.StatusBadRequest, model.Response{
			Code:    400,
			Message: "invalid request: " + err.Error(),
		})
		return
	}

	// 设置默认值
	if req.Size <= 0 {
		req.Size = 100
	}
	if req.To == "" {
		req.To = "now"
	}

	// 验证必填参数：非 full_range 模式时必须提供 from
	if !req.FullRange && req.From == "" {
		c.JSON(http.StatusBadRequest, model.Response{
			Code:    400,
			Message: "from parameter is required when full_range is not set",
		})
		return
	}

	slog.Debug("[RawHandler.Query] processing",
		"cluster", req.Cluster,
		"job", req.Job,
		"collector", req.Collector,
		"from", req.From,
		"to", req.To,
		"size", req.Size,
		"fields", req.Fields,
		"full_range", req.FullRange,
	)

	slog.Info("[RawHandler.Query] request",
		"cluster", req.Cluster,
		"job", req.Job,
		"from", req.From,
		"to", req.To,
		"collector", req.Collector,
		"size", req.Size,
	)

	startTime := time.Now()

	// 1. 解析集群 ID（支持多值和通配）
	clusterIDs, err := h.querySvc.IndexService().ParseClusterParam(req.Cluster)
	if err != nil {
		c.JSON(http.StatusBadRequest, model.Response{
			Code:    400,
			Message: "invalid cluster parameter: " + err.Error(),
		})
		return
	}

	// 2. 执行查询（单集群或多集群并行）
	var resp *model.RawQueryResponse
	var meta *model.Meta

	if len(clusterIDs) == 1 {
		// 单集群查询
		resp, err = h.querySvc.ExecuteRawQuery(c.Request.Context(), clusterIDs[0], &req, nil)
		if err != nil {
			slog.Error("raw query failed",
				"cluster", req.Cluster,
				"job", req.Job,
				"error", err,
			)
			c.Set("error_kind", "raw_query_failed")
			c.Set("error_detail", err.Error())
			c.JSON(http.StatusInternalServerError, model.Response{
				Code:    500,
				Message: "query failed: " + err.Error(),
			})
			return
		}
		meta = &model.Meta{
			QueryTimeMs:     int(time.Since(startTime).Milliseconds()),
			ClustersQueried: clusterIDs,
			IndicesHit:      resp.IndicesResolved,
		}
	} else {
		// 多集群并行查询
		resp, meta, err = h.querySvc.ExecuteMultiClusterQuery(c.Request.Context(), clusterIDs, &req)
		if err != nil {
			slog.Error("multi-cluster query failed",
				"cluster", req.Cluster,
				"job", req.Job,
				"error", err,
			)
			c.Set("error_kind", "multi_cluster_query_failed")
			c.Set("error_detail", err.Error())
			c.JSON(http.StatusInternalServerError, model.Response{
				Code:    500,
				Message: "multi-cluster query failed: " + err.Error(),
			})
			return
		}
		meta.QueryTimeMs = int(time.Since(startTime).Milliseconds())
	}

	// 3. 返回响应
	slog.Debug("[RawHandler.Query] completed",
		"query_time_ms", meta.QueryTimeMs,
		"clusters_queried", meta.ClustersQueried,
		"records_count", len(resp.Records),
	)

	slog.Info("[RawHandler.Query] completed",
		"cluster", req.Cluster,
		"job", req.Job,
		"records", len(resp.Records),
		"total", resp.Pagination.Total,
		"took_ms", meta.QueryTimeMs,
	)

	c.JSON(http.StatusOK, model.Response{
		Code:    0,
		Message: "success",
		Data:    resp,
		Meta:    meta,
	})
}
