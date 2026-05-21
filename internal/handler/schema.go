package handler

import (
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/joblens/tap/internal/cluster"
	"github.com/joblens/tap/internal/config"
	"github.com/joblens/tap/internal/model"
)

// SchemaHandler Schema 发现处理器
type SchemaHandler struct {
	clusterMgr *cluster.Manager
	cfg        *config.Config
}

// NewSchemaHandler 创建 Schema 处理器
func NewSchemaHandler(clusterMgr *cluster.Manager, cfg *config.Config) *SchemaHandler {
	return &SchemaHandler{
		clusterMgr: clusterMgr,
		cfg:        cfg,
	}
}

// Get Schema 发现，返回集群列表、采集器详情和全局别名
// GET /schema?cluster=xxx
func (h *SchemaHandler) Get(c *gin.Context) {
	var req model.SchemaRequest
	if err := c.ShouldBindQuery(&req); err != nil {
		c.JSON(http.StatusBadRequest, model.Response{
			Code:    400,
			Message: "invalid request: " + err.Error(),
		})
		return
	}

	// 构建集群列表
	allClusters := h.clusterMgr.GetAll()
	collectorNames := h.cfg.Registry.GetCollectorNames()

	var clusters []model.ClusterInfo
	for _, mc := range allClusters {
		if req.Cluster != "" && req.Cluster != mc.Name {
			continue
		}
		clusterInfo := model.ClusterInfo{
			ID:         mc.Name,
			Type:       mc.Type,
			Alias:      mc.Alias,
			Enabled:    mc.Enabled,
			Collectors: collectorNames,
		}
		// 优先使用 Alias 作为 ID
		if mc.Alias != "" {
			clusterInfo.ID = mc.Alias
		}
		clusters = append(clusters, clusterInfo)
	}

	// 构建采集器详情列表
	registryCollectors := h.cfg.Registry.GetAllCollectors()
	collectors := make([]model.CollectorInfo, 0, len(registryCollectors))
	for _, ce := range registryCollectors {
		collectors = append(collectors, model.CollectorInfo{
			Name:         ce.Name,
			Description:  ce.Description,
			IndexPattern: ce.IndexPattern,
			Aliases:      ce.Aliases,
		})
	}

	// 获取全局别名
	commonAliases := h.cfg.Registry.GetGlobalAliases()

	slog.Info("[SchemaHandler.Get] request",
		"cluster_filter", req.Cluster,
		"collector_filter", req.Collector,
		"clusters_returned", len(clusters),
		"collectors_returned", len(collectors),
	)

	c.JSON(http.StatusOK, model.Response{
		Code:    0,
		Message: "success",
		Data: model.SchemaResponse{
			Clusters:      clusters,
			Collectors:    collectors,
			CommonAliases: commonAliases,
		},
	})
}
