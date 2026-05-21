package handler

import (
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/joblens/tap/internal/model"
	"github.com/joblens/tap/internal/service"
)

// CheckJobHandler Job 存在性检查处理器
type CheckJobHandler struct {
	querySvc *service.QueryService
}

// NewCheckJobHandler 创建 Job 存在性检查处理器
func NewCheckJobHandler(querySvc *service.QueryService) *CheckJobHandler {
	return &CheckJobHandler{
		querySvc: querySvc,
	}
}

// Check 检查 Job 数据是否存在
// GET /data/check-job
func (h *CheckJobHandler) Check(c *gin.Context) {
	var req model.CheckJobRequest
	if err := c.ShouldBindQuery(&req); err != nil {
		c.JSON(http.StatusBadRequest, model.Response{
			Code:    400,
			Message: "invalid request: " + err.Error(),
		})
		return
	}

	slog.Debug("[CheckJobHandler.Check] processing",
		"cluster_name", req.ClusterName,
		"cluster_tag", req.ClusterTag,
		"job_id", req.JobID,
	)

	slog.Info("[CheckJobHandler.Check] request",
		"cluster_name", req.ClusterName,
		"job_id", req.JobID,
		"cluster_tag", req.ClusterTag,
	)

	resp, err := h.querySvc.CheckJobExists(c.Request.Context(), req.ClusterName, req.ClusterTag, req.JobID)
	if err != nil {
		slog.Error("[CheckJobHandler.Check] check failed",
			"cluster_name", req.ClusterName,
			"job_id", req.JobID,
			"error", err,
		)
		c.Set("error_kind", "check_job_failed")
		c.Set("error_detail", err.Error())
		c.JSON(http.StatusInternalServerError, model.Response{
			Code:    500,
			Message: "check job failed: " + err.Error(),
		})
		return
	}

	slog.Info("[CheckJobHandler.Check] completed",
		"cluster_name", req.ClusterName,
		"job_id", req.JobID,
		"exists", resp.Exists,
		"count", resp.Count,
	)

	c.JSON(http.StatusOK, model.Response{
		Code:    0,
		Message: "success",
		Data:    resp,
	})
}
