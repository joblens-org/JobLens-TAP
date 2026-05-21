package handler

import (
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/joblens/tap/internal/model"
	"github.com/joblens/tap/internal/service"
)

// CollectionHandler 采集处理器
type CollectionHandler struct {
	collectionSvc *service.CollectionService
}

// NewCollectionHandler 创建采集处理器
func NewCollectionHandler(collectionSvc *service.CollectionService) *CollectionHandler {
	return &CollectionHandler{
		collectionSvc: collectionSvc,
	}
}

// Trigger 触发作业采集
// POST /collect
func (h *CollectionHandler) Trigger(c *gin.Context) {
	var req model.TriggerCollectionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, model.Response{
			Code:    400,
			Message: "invalid request: " + err.Error(),
		})
		return
	}

	// 执行采集触发
	slog.Info("[CollectionHandler.Trigger] request",
		"cluster_name", req.ClusterName,
		"job_id", req.JobID,
		"collector", req.Collector,
	)

	resp, err := h.collectionSvc.TriggerCollection(c.Request.Context(), req.ClusterName, req.ClusterTag, req.JobID, req.Collector)
	if err != nil {
		c.JSON(http.StatusInternalServerError, model.Response{
			Code:    500,
			Message: "trigger collection failed: " + err.Error(),
		})
		return
	}

	// 返回响应
	var statusCode int
	if resp.Status == "success" {
		statusCode = http.StatusOK
	} else {
		statusCode = http.StatusMultiStatus // 207 Multi-Status 用于部分成功
	}

	c.JSON(statusCode, model.Response{
		Code:    0,
		Message: resp.Message,
		Data:    resp,
	})
}

// TriggerDirect 直接触发作业采集（跳过脚本查询）
// POST /collect/direct
func (h *CollectionHandler) TriggerDirect(c *gin.Context) {
	var req model.DirectTriggerCollectionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, model.Response{
			Code:    400,
			Message: "invalid request: " + err.Error(),
		})
		return
	}

	// 执行直接采集触发
	slog.Info("[CollectionHandler.TriggerDirect] request",
		"cluster_name", req.ClusterName,
		"job_id", req.JobID,
		"node", req.Node,
		"collector", req.Collector,
	)

	resp, err := h.collectionSvc.TriggerDirectCollection(c.Request.Context(), &req)
	if err != nil {
		// 参数类错误（如 htcondor 缺少 slot）返回 400
		c.JSON(http.StatusBadRequest, model.Response{
			Code:    400,
			Message: err.Error(),
		})
		return
	}

	// 返回响应
	var statusCode int
	if resp.Status == "success" {
		statusCode = http.StatusOK
	} else {
		statusCode = http.StatusMultiStatus
	}

	c.JSON(statusCode, model.Response{
		Code:    0,
		Message: resp.Message,
		Data:    resp,
	})
}
