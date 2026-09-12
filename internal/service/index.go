package service

import (
	"log/slog"
	"strings"
	"time"

	"github.com/joblens/tap/internal/cluster"
	"github.com/joblens/tap/internal/config"
)

// IndexService 索引路由服务
type IndexService struct {
	cfg        *config.Config
	clusterMgr *cluster.Manager
}

// NewIndexService 创建索引服务
func NewIndexService(cfg *config.Config, clusterMgr *cluster.Manager) *IndexService {
	return &IndexService{
		cfg:        cfg,
		clusterMgr: clusterMgr,
	}
}

// ResolveIndices 解析采集器和时间范围为索引名称列表
// 新模式: 通过注册中心的 index_pattern 渲染（默认 "{collector}_collector_{date}"）
// collector: 采集器类型（cpumem/io/net），为空时包含所有
// from, to: 时间范围
func (s *IndexService) ResolveIndices(collector string, from, to time.Time, collectors []string) ([]string, error) {
	if from.After(to) {
		return nil, NewQueryError(StatusBadRequest, ErrKindInvalidRequest, "from must not be after to")
	}
	// 确定采集器列表
	if collector != "" {
		collectors = []string{collector}
	}
	if len(collectors) == 0 {
		return nil, NewQueryError(StatusBadRequest, ErrKindInvalidRequest, "at least one collector is required to resolve indices")
	}

	// 生成日期范围
	dates := s.generateDateRange(from, to)

	// 生成索引名称（通过注册中心渲染 index_pattern）
	var indices []string
	for _, coll := range collectors {
		for _, date := range dates {
			index := s.cfg.Registry.RenderIndexName(coll, date.Format("2006.01.02"))
			indices = append(indices, index)
		}
	}

	slog.Debug("[ResolveIndices] resolved",
		"collector", collector,
		"from", from.Format(time.RFC3339),
		"to", to.Format(time.RFC3339),
		"date_count", len(dates),
		"index_count", len(indices),
		"indices", indices,
	)

	return indices, nil
}

// ResolveWildcardIndices 生成通配符索引列表（不按日期展开），用于长作业全时段查询
func (s *IndexService) ResolveWildcardIndices(collector string, collectors []string) []string {
	if collector != "" {
		collectors = []string{collector}
	}
	indices := make([]string, 0, len(collectors))
	for _, coll := range collectors {
		indices = append(indices, s.cfg.Registry.RenderIndexName(coll, "*"))
	}
	return indices
}

// ParseClusterParam 解析集群参数（支持多值和通配）
// 返回解析后的 cluster_name 列表
func (s *IndexService) ParseClusterParam(param string) ([]string, error) {
	// 通配符 * 表示所有集群
	if param == "*" {
		return s.clusterMgr.GetAllNames(), nil
	}

	// 提取 cluster_name 部分（去掉 :cluster_tag 后缀）
	var result []string
	parts := strings.Split(param, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		cn, _ := config.ParseClusterFilter(part)
		if cn != "" {
			result = append(result, cn)
		}
	}

	slog.Debug("[ParseClusterParam] parsed",
		"input", param,
		"clusterIDs", result,
		"count", len(result),
	)

	return result, nil
}

// generateDateRange 生成日期范围（闭区间）
func (s *IndexService) generateDateRange(from, to time.Time) []time.Time {
	// writer 使用 system_clock 的 UTC 日分区，而非时间字符串显示的偏移日期。
	start := from.UTC().Truncate(24 * time.Hour)
	end := to.UTC().Truncate(24 * time.Hour)

	var dates []time.Time
	for d := start; !d.After(end); d = d.AddDate(0, 0, 1) {
		dates = append(dates, d)
	}

	return dates
}
