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

// SummaryHandler 任务摘要查询处理器
type SummaryHandler struct {
	esManager *repository.ClientManager
	querySvc  *service.QueryService
}

// NewSummaryHandler 创建任务摘要查询处理器
func NewSummaryHandler(esManager *repository.ClientManager, querySvc *service.QueryService) *SummaryHandler {
	return &SummaryHandler{
		esManager: esManager,
		querySvc:  querySvc,
	}
}

// Query 任务摘要查询
// GET /data/summary
func (h *SummaryHandler) Query(c *gin.Context) {
	var req model.SummaryRequest
	if err := c.ShouldBindQuery(&req); err != nil {
		c.JSON(http.StatusBadRequest, model.Response{
			Code:    400,
			Message: "invalid request: " + err.Error(),
		})
		return
	}

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

	slog.Debug("[SummaryHandler.Query] processing",
		"cluster", req.Cluster,
		"clusterIDs", clusterIDs,
		"job", req.Job,
	)

	slog.Info("[SummaryHandler.Query] request",
		"cluster", req.Cluster,
		"job", req.Job,
	)

	// Summary 接口建议单集群查询
	if len(clusterIDs) > 1 {
		c.JSON(http.StatusBadRequest, model.Response{
			Code:    400,
			Message: "summary query only supports single cluster, please specify one cluster",
		})
		return
	}

	// 2. 执行 Summary 查询
	resp, err := h.querySvc.ExecuteSummaryQuery(c.Request.Context(), clusterIDs[0], &req)
	if err != nil {
		slog.Error("summary query failed",
			"cluster", req.Cluster,
			"job", req.Job,
			"error", err,
		)
		c.Set("error_kind", "summary_query_failed")
		c.Set("error_detail", err.Error())
		c.JSON(http.StatusInternalServerError, model.Response{
			Code:    500,
			Message: "summary query failed: " + err.Error(),
		})
		return
	}

	// 3. 返回响应
	slog.Debug("[SummaryHandler.Query] completed",
		"query_time_ms", int(time.Since(startTime).Milliseconds()),
		"stats_keys_count", len(resp.Stats),
	)

	slog.Info("[SummaryHandler.Query] completed",
		"cluster", req.Cluster,
		"job", req.Job,
		"duration_sec", resp.Time.DurationSec,
		"stats_keys", len(resp.Stats),
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
