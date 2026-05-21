package handler

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/joblens/tap/internal/model"
	"github.com/joblens/tap/internal/repository"
)

// HealthHandler 健康检查处理器
type HealthHandler struct {
	esManager *repository.ClientManager
	version   string
	gitCommit string
	buildTime string
}

// NewHealthHandler 创建健康检查处理器
func NewHealthHandler(esManager *repository.ClientManager, version, gitCommit, buildTime string) *HealthHandler {
	return &HealthHandler{
		esManager: esManager,
		version:   version,
		gitCommit: gitCommit,
		buildTime: buildTime,
	}
}

// Health 服务健康检查
// GET /health
func (h *HealthHandler) Health(c *gin.Context) {
	c.JSON(http.StatusOK, model.Response{
		Code:    0,
		Message: "ok",
		Data: gin.H{
			"status": "healthy",
			"time":   time.Now().UTC().Format(time.RFC3339),
		},
	})
}

// Ready 就绪探针 - 检查 ES 连接
// GET /ready
func (h *HealthHandler) Ready(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	// 检查所有集群连接
	allHealthy := true
	clusterStatus := make(map[string]string)

	for clusterID, client := range h.esManager.GetAllClients() {
		if err := client.Ping(ctx); err != nil {
			allHealthy = false
			clusterStatus[clusterID] = "unhealthy: " + err.Error()
		} else {
			clusterStatus[clusterID] = "healthy"
		}
	}

	if !allHealthy {
		c.JSON(http.StatusServiceUnavailable, model.Response{
			Code:    1,
			Message: "some clusters are unavailable",
			Data: gin.H{
				"clusters": clusterStatus,
				"version":  h.version,
				"commit":   h.gitCommit,
				"build":    h.buildTime,
			},
		})
		return
	}

	c.JSON(http.StatusOK, model.Response{
		Code:    0,
		Message: "ready",
		Data: gin.H{
			"clusters": clusterStatus,
			"version":  h.version,
			"commit":   h.gitCommit,
			"build":    h.buildTime,
		},
	})
}
