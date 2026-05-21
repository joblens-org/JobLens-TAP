package service

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/joblens/tap/internal/cluster"
	"github.com/joblens/tap/internal/config"
	"github.com/joblens/tap/internal/model"
	"github.com/joblens/tap/internal/repository"
)

// QueryService 查询服务
type QueryService struct {
	cfg        *config.Config
	esManager  *repository.ClientManager
	clusterMgr *cluster.Manager
	indexSvc   *IndexService
	parserSvc  *ParserService
}

// NewQueryService 创建查询服务
func NewQueryService(cfg *config.Config, esManager *repository.ClientManager, clusterMgr *cluster.Manager) *QueryService {
	return &QueryService{
		cfg:        cfg,
		esManager:  esManager,
		clusterMgr: clusterMgr,
		indexSvc:   NewIndexService(cfg, clusterMgr),
		parserSvc:  NewParserService(),
	}
}

// IndexService 获取 Index 服务
func (s *QueryService) IndexService() *IndexService {
	return s.indexSvc
}

// ParserService 获取 Parser 服务
func (s *QueryService) ParserService() *ParserService {
	return s.parserSvc
}

// RawQueryResult Raw 查询结果
type RawQueryResult struct {
	ClusterID string
	Response  *model.RawQueryResponse
	Indices   []string
	Error     error
}

// BuildRawQuery 构建 Raw 接口的 ES DSL
// clusterName/clusterTag: 集群标识过滤条件
// jobFilters: BuildJobFilter 生成的 JobID filter 列表
func (s *QueryService) BuildRawQuery(req *model.RawQueryRequest, from, to time.Time, searchAfter []any, fields []string, clusterName, clusterTag string, jobFilters []map[string]any) map[string]any {
	// 构建 filter 条件
	filters := []map[string]any{
		{
			"term": map[string]any{
				"job_info.cluster_name.keyword": clusterName,
			},
		},
		{
			"range": map[string]any{
				"@timestamp": map[string]any{
					"gte": from.Format(time.RFC3339),
					"lte": to.Format(time.RFC3339),
				},
			},
		},
	}

	// 可选 clusterTag 精确过滤
	if clusterTag != "" {
		filters = append(filters, map[string]any{
			"term": map[string]any{
				"job_info.clusterTag.keyword": clusterTag,
			},
		})
	}

	// 添加 JobID filter
	filters = append(filters, jobFilters...)

	// 构建 _source
	var sourceIncludes []string
	if len(fields) > 0 {
		sourceIncludes = []string{"@timestamp", "hostname", "job_info"}
		for _, field := range fields {
			if esField, ok := s.cfg.Registry.GetESField(field); ok {
				sourceIncludes = append(sourceIncludes, esField)
			} else {
				sourceIncludes = append(sourceIncludes, field)
			}
		}
	}

	query := map[string]any{
		"query": map[string]any{
			"bool": map[string]any{
				"filter": filters,
			},
		},
		"size": req.Size,
		"sort": []map[string]any{
			{"@timestamp": "desc"},
		},
	}

	// 添加 _source 过滤
	if len(sourceIncludes) > 0 {
		query["_source"] = map[string]any{
			"includes": sourceIncludes,
		}
	}

	// 添加 search_after（游标分页）
	if len(searchAfter) > 0 {
		query["search_after"] = searchAfter
	}

	return query
}

// DiscoverJobTimeRange 通过 ES 聚合发现 Job 的完整时间范围
func (s *QueryService) DiscoverJobTimeRange(ctx context.Context, clusterName string, req *model.RawQueryRequest, jobFilters []map[string]any) (time.Time, time.Time, error) {
	esClient, info, err := s.esManager.GetClientForCluster(clusterName)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}

	// 将别名替换为集群真实名称，用于 ES 查询过滤
	realName := info.Name

	// 通配符索引模式（通过注册中心渲染）
	var indices []string
	if req.Collector != "" {
		indices = []string{s.cfg.Registry.RenderIndexName(req.Collector, "*")}
	} else {
		for _, coll := range s.cfg.DefaultCollectors {
			indices = append(indices, s.cfg.Registry.RenderIndexName(coll, "*"))
		}
	}

	// 构建 filter：cluster_name + job
	filters := []map[string]any{
		{"term": map[string]any{"job_info.cluster_name.keyword": realName}},
	}
	filters = append(filters, jobFilters...)

	query := map[string]any{
		"query": map[string]any{
			"bool": map[string]any{
				"filter": filters,
			},
		},
		"size": 0,
		"aggs": map[string]any{
			"first_seen": map[string]any{"min": map[string]any{"field": "@timestamp"}},
			"last_seen":  map[string]any{"max": map[string]any{"field": "@timestamp"}},
		},
	}

	slog.Debug("[DiscoverJobTimeRange] discovering job time range",
		"cluster", clusterName,
		"job", req.Job,
		"collector", req.Collector,
		"indices", indices,
	)

	result, err := esClient.Search(ctx, indices, query, "")
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("search aggregation: %w", err)
	}

	var firstTime, lastTime time.Time
	if aggs, ok := result.Aggregations["first_seen"].(map[string]any); ok {
		if v, ok := aggs["value_as_string"].(string); ok {
			firstTime, _ = time.Parse(time.RFC3339, v)
		} else if v, ok := aggs["value"].(float64); ok {
			firstTime = time.UnixMilli(int64(v))
		}
	}
	if aggs, ok := result.Aggregations["last_seen"].(map[string]any); ok {
		if v, ok := aggs["value_as_string"].(string); ok {
			lastTime, _ = time.Parse(time.RFC3339, v)
		} else if v, ok := aggs["value"].(float64); ok {
			lastTime = time.UnixMilli(int64(v))
		}
	}

	if firstTime.IsZero() || lastTime.IsZero() {
		return time.Time{}, time.Time{}, fmt.Errorf("no data found for job %s in cluster %s", req.Job, clusterName)
	}

	slog.Debug("[DiscoverJobTimeRange] discovered",
		"cluster", clusterName,
		"job", req.Job,
		"first_seen", firstTime.Format(time.RFC3339),
		"last_seen", lastTime.Format(time.RFC3339),
	)

	slog.Info("[DiscoverJobTimeRange] discovered",
		"cluster", clusterName,
		"job", req.Job,
		"first_seen", firstTime.Format(time.RFC3339),
		"last_seen", lastTime.Format(time.RFC3339),
		"duration", lastTime.Sub(firstTime).String(),
	)

	return firstTime, lastTime, nil
}

// ExecuteRawQuery 执行单集群 Raw 查询
func (s *QueryService) ExecuteRawQuery(ctx context.Context, clusterName string, req *model.RawQueryRequest, cursor *model.Cursor) (*model.RawQueryResponse, error) {
	// 解析 cluster 参数获取 clusterName 和 clusterTag
	cn, ct := config.ParseClusterFilter(req.Cluster)
	if cn == "" {
		return nil, fmt.Errorf("failed to parse cluster name from: %s", req.Cluster)
	}

	// 获取 ES 客户端和集群信息
	esClient, clusterInfo, err := s.esManager.GetClientForCluster(cn)
	if err != nil {
		return nil, err
	}

	// 解析 JobID 为 filter
	jobFilters, err := s.parserSvc.BuildJobFilter(req.Job)
	if err != nil {
		return nil, fmt.Errorf("invalid job id: %w", err)
	}

	// 单 Tag 自动优化：用户未指定 tag 但集群只有一个 → 自动补充
	if ct == "" && len(clusterInfo.Tags) == 1 {
		ct = clusterInfo.Tags[0]
	}

	// 将别名替换为集群真实名称，用于 ES 查询过滤
	cn = clusterInfo.Name

	slog.Debug("[ExecuteRawQuery] entry",
		"cluster_name", cn,
		"cluster_tag", ct,
		"job", req.Job,
		"collector", req.Collector,
		"from", req.From,
		"to", req.To,
		"size", req.Size,
		"fields", req.Fields,
		"full_range", req.FullRange,
	)

	// 解析时间范围
	var from, to time.Time
	if req.FullRange {
		from, to, err = s.DiscoverJobTimeRange(ctx, cn, req, jobFilters)
		if err != nil {
			return nil, fmt.Errorf("discover time range: %w", err)
		}
	} else {
		from, err = s.parserSvc.ParseTime(req.From)
		if err != nil {
			return nil, fmt.Errorf("parse from time: %w", err)
		}
		to, err = s.parserSvc.ParseTime(req.To)
		if err != nil {
			return nil, fmt.Errorf("parse to time: %w", err)
		}
		if err := s.parserSvc.ValidateTimeRange(from, to, s.cfg.MaxTimeRangeDays); err != nil {
			return nil, err
		}
	}

	// 解析字段
	fields := s.parserSvc.ParseFields(req.Fields)

	// 解析游标
	var searchAfter []any
	if cursor != nil && cursor.Cluster == cn {
		searchAfter = cursor.SearchAfter
	}

	// 构建查询
	query := s.BuildRawQuery(req, from, to, searchAfter, fields, cn, ct, jobFilters)

	// 解析索引
	indices, err := s.indexSvc.ResolveIndices(req.Collector, from, to, s.cfg.DefaultCollectors)
	if err != nil {
		return nil, fmt.Errorf("resolve indices: %w", err)
	}

	// 确定 routing（指定 tag 时启用）
	routing := ""
	if ct != "" {
		routing = ct
	}

	// 执行查询
	startTime := time.Now()
	result, err := esClient.Search(ctx, indices, query, routing)
	if err != nil {
		return nil, fmt.Errorf("execute search: %w", err)
	}
	queryTimeMs := time.Since(startTime).Milliseconds()

	// 扁平化结果
	records := FlattenHits(result.Hits, cn, req.Flatten, fields)

	// 构建响应
	response := &model.RawQueryResponse{
		Records:         records,
		IndicesResolved: indices,
		Pagination: &model.Pagination{
			Returned: len(records),
			Total:    int(result.Total),
			HasMore:  len(records) >= req.Size && len(result.Hits) > 0,
		},
	}

	// 生成下一页游标
	if response.Pagination.HasMore && len(result.Hits) > 0 {
		lastHit := result.Hits[len(result.Hits)-1]
		nextCursor := model.Cursor{
			Cluster:     cn,
			SearchAfter: lastHit.Sort,
			QueryHash:   computeQueryHash(req),
		}
		response.Pagination.NextCursor = encodeCursor(&nextCursor)
	}

	slog.Debug("raw query executed",
		"cluster", cn,
		"took_ms", queryTimeMs,
		"total", result.Total,
		"returned", len(records),
		"indices", indices,
		"routing", routing,
	)

	slog.Info("[ExecuteRawQuery] completed",
		"cluster", cn,
		"job", req.Job,
		"indices", len(indices),
		"total_hits", result.Total,
		"returned", len(records),
		"took_ms", queryTimeMs,
	)

	return response, nil
}

// ExecuteMultiClusterQuery 执行多集群并行查询
func (s *QueryService) ExecuteMultiClusterQuery(ctx context.Context, clusterIDs []string, req *model.RawQueryRequest) (*model.RawQueryResponse, *model.Meta, error) {
	// LOG_REASON: 多集群查询入口，记录集群数量和关键参数，便于定位并行查询中的部分失败/全部失败
	slog.Debug("[ExecuteMultiClusterQuery] entry",
		"clusters", clusterIDs,
		"cluster_count", len(clusterIDs),
		"job", req.Job,
		"collector", req.Collector,
		"from", req.From,
		"to", req.To,
		"size", req.Size,
	)

	// 解析游标
	var cursor *model.Cursor
	if req.Cursor != "" {
		cursor = decodeCursor(req.Cursor)
	}

	// 如果游标指定了集群，只查询该集群
	if cursor != nil && cursor.Cluster != "" {
		clusterIDs = []string{cursor.Cluster}
	}

	// 并行查询所有集群
	results := make(chan *RawQueryResult, len(clusterIDs))
	var wg sync.WaitGroup

	for _, clusterID := range clusterIDs {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			resp, err := s.ExecuteRawQuery(ctx, id, req, cursor)
			results <- &RawQueryResult{
				ClusterID: id,
				Response:  resp,
				Error:     err,
			}
		}(clusterID)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	// 收集结果
	var allResults []*RawQueryResult
	var errors []error

	for result := range results {
		if result.Error != nil {
			errors = append(errors, fmt.Errorf("cluster %s: %w", result.ClusterID, result.Error))
		} else {
			allResults = append(allResults, result)
		}
	}

	// 如果所有查询都失败
	if len(allResults) == 0 && len(errors) > 0 {
		return nil, nil, fmt.Errorf("all cluster queries failed: %v", errors)
	}

	// 记录错误但不中断
	if len(errors) > 0 {
		for _, err := range errors {
			slog.Warn("partial query failed", "error", err)
		}
	}

	// 收集成功响应
	var responses []*model.RawQueryResponse
	for _, r := range allResults {
		if r.Response != nil {
			responses = append(responses, r.Response)
		}
	}

	// 合并结果
	merged := MergeResults(responses, req.Size)

	// 构建元信息（使用合并结果中的索引列表）
	meta := &model.Meta{
		ClustersQueried: clusterIDs,
		IndicesHit:      merged.IndicesResolved,
	}

	slog.Info("[ExecuteMultiClusterQuery] completed",
		"total_clusters", len(clusterIDs),
		"success_clusters", len(allResults),
		"failed_clusters", len(errors),
		"total_records", len(merged.Records),
	)

	return merged, meta, nil
}

// encodeCursor 编码游标为 Base64 字符串
func encodeCursor(cursor *model.Cursor) string {
	data, err := json.Marshal(cursor)
	if err != nil {
		return ""
	}
	return base64.StdEncoding.EncodeToString(data)
}

// decodeCursor 解码游标
func decodeCursor(encoded string) *model.Cursor {
	data, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil
	}
	var cursor model.Cursor
	if err := json.Unmarshal(data, &cursor); err != nil {
		return nil
	}
	return &cursor
}

// computeQueryHash 计算查询参数的哈希值（用于游标验证）
func computeQueryHash(req *model.RawQueryRequest) string {
	h := sha256.New()
	fmt.Fprintf(h, "%s:%s:%s:%s:%s:%d", req.Cluster, req.Job, req.From, req.To, req.Collector, req.Size)
	return hex.EncodeToString(h.Sum(nil))[:16]
}

// ParseMetrics 解析 metric 参数（逗号分隔）
func (s *QueryService) ParseMetrics(metricStr string) []string {
	return s.parserSvc.ParseFields(metricStr)
}

// GroupMetricsByCollector 按 collector 分组 metrics
func GroupMetricsByCollector(metrics []string, registry *model.CollectorRegistry) map[string][]string {
	result := make(map[string][]string)
	for _, metric := range metrics {
		collector := registry.InferCollectorFromMetric(metric)
		result[collector] = append(result[collector], metric)
	}
	return result
}

// BuildMultiMetricTimeSeriesQuery 构建多 metric 的 ES DSL
func (s *QueryService) BuildMultiMetricTimeSeriesQuery(req *model.TimeSeriesRequest, from, to time.Time, metrics []string, collector, clusterName, clusterTag string, jobFilters []map[string]any) map[string]any {
	slog.Debug("[BuildMultiMetricTimeSeriesQuery] building query",
		"job", req.Job,
		"metrics", metrics,
		"collector", collector,
		"interval", req.Interval,
		"agg", req.Agg,
		"by", req.By,
		"from", from.Format(time.RFC3339),
		"to", to.Format(time.RFC3339),
	)

	// 构建 filter 条件
	filters := []map[string]any{
		{
			"term": map[string]any{
				"job_info.cluster_name.keyword": clusterName,
			},
		},
		{
			"range": map[string]any{
				"@timestamp": map[string]any{
					"gte": from.Format(time.RFC3339),
					"lte": to.Format(time.RFC3339),
				},
			},
		},
	}
	if clusterTag != "" {
		filters = append(filters, map[string]any{
			"term": map[string]any{"job_info.clusterTag.keyword": clusterTag},
		})
	}
	filters = append(filters, jobFilters...)

	// 构建日期直方图聚合
	dateHistAgg := map[string]any{
		"field":          "@timestamp",
		"fixed_interval": req.Interval,
		"min_doc_count":  0,
		"extended_bounds": map[string]any{
			"min": from.Format(time.RFC3339),
			"max": to.Format(time.RFC3339),
		},
	}

	// 为每个 metric 构建聚合
	metricAggs := make(map[string]any)
	globalStatsAggs := make(map[string]any)

	for _, metric := range metrics {
		esField, ok := s.cfg.Registry.GetESField(metric)
		if !ok {
			esField = metric
		}
		aggName := req.Agg + "_" + metric
		metricAggs[aggName] = buildMetricAgg(req.Agg, esField)
		globalStatsAggs["stats_"+metric] = map[string]any{
			"extended_stats": map[string]any{
				"field": esField,
			},
		}
	}

	// 根据是否有分组维度构建不同的聚合
	var aggregations map[string]any

	if req.By != "" {
		aggregations = map[string]any{
			"group_by_" + req.By: map[string]any{
				"terms": map[string]any{
					"field": getGroupByField(req.By),
					"size":  100,
				},
				"aggs": map[string]any{
					"timeseries": map[string]any{
						"date_histogram": dateHistAgg,
						"aggs":           metricAggs,
					},
				},
			},
		}
	} else {
		aggregations = map[string]any{
			"timeseries": map[string]any{
				"date_histogram": dateHistAgg,
				"aggs":           metricAggs,
			},
		}
		for k, v := range globalStatsAggs {
			aggregations[k] = v
		}
	}

	return map[string]any{
		"query": map[string]any{
			"bool": map[string]any{
				"filter": filters,
			},
		},
		"size": 0,
		"aggs": aggregations,
	}
}

// BuildTimeSeriesQuery 构建 Timeseries 接口的 ES DSL（聚合查询）
func (s *QueryService) BuildTimeSeriesQuery(req *model.TimeSeriesRequest, from, to time.Time, clusterName, clusterTag string, jobFilters []map[string]any) map[string]any {
	slog.Debug("[BuildTimeSeriesQuery] building query",
		"job", req.Job,
		"metric", req.Metric,
		"interval", req.Interval,
		"agg", req.Agg,
		"by", req.By,
		"from", from.Format(time.RFC3339),
		"to", to.Format(time.RFC3339),
	)

	// 获取指标对应的 ES 字段
	esField, ok := s.cfg.Registry.GetESField(req.Metric)
	if !ok {
		esField = req.Metric
	}

	// 构建 filter 条件
	filters := []map[string]any{
		{
			"term": map[string]any{
				"job_info.cluster_name.keyword": clusterName,
			},
		},
		{
			"range": map[string]any{
				"@timestamp": map[string]any{
					"gte": from.Format(time.RFC3339),
					"lte": to.Format(time.RFC3339),
				},
			},
		},
	}
	if clusterTag != "" {
		filters = append(filters, map[string]any{
			"term": map[string]any{"job_info.clusterTag.keyword": clusterTag},
		})
	}
	filters = append(filters, jobFilters...)

	// 构建聚合操作名称
	aggName := req.Agg + "_" + req.Metric

	// 构建日期直方图聚合
	dateHistAgg := map[string]any{
		"field":          "@timestamp",
		"fixed_interval": req.Interval,
		"min_doc_count":  0,
		"extended_bounds": map[string]any{
			"min": from.Format(time.RFC3339),
			"max": to.Format(time.RFC3339),
		},
	}

	// 根据是否有分组维度构建不同的聚合
	var aggregations map[string]any

	if req.By != "" {
		aggregations = map[string]any{
			"group_by_" + req.By: map[string]any{
				"terms": map[string]any{
					"field": getGroupByField(req.By),
					"size":  100,
				},
				"aggs": map[string]any{
					"timeseries": map[string]any{
						"date_histogram": dateHistAgg,
						"aggs": map[string]any{
							aggName: buildMetricAgg(req.Agg, esField),
						},
					},
				},
			},
		}
	} else {
		aggregations = map[string]any{
			"timeseries": map[string]any{
				"date_histogram": dateHistAgg,
				"aggs": map[string]any{
					aggName: buildMetricAgg(req.Agg, esField),
				},
			},
			"global_stats": map[string]any{
				"extended_stats": map[string]any{
					"field": esField,
				},
			},
		}
	}

	return map[string]any{
		"query": map[string]any{
			"bool": map[string]any{
				"filter": filters,
			},
		},
		"size": 0,
		"aggs": aggregations,
	}
}

// buildMetricAgg 构建指标聚合
func buildMetricAgg(aggType, field string) map[string]any {
	switch aggType {
	case "avg":
		return map[string]any{
			"avg": map[string]any{
				"field": field,
			},
		}
	case "max":
		return map[string]any{
			"max": map[string]any{
				"field": field,
			},
		}
	case "min":
		return map[string]any{
			"min": map[string]any{
				"field": field,
			},
		}
	case "sum":
		return map[string]any{
			"sum": map[string]any{
				"field": field,
			},
		}
	default:
		return map[string]any{
			"avg": map[string]any{
				"field": field,
			},
		}
	}
}

// getGroupByField 获取分组字段
func getGroupByField(by string) string {
	switch by {
	case "host":
		return "hostname.keyword"
	case "collector":
		return "collector"
	default:
		return by
	}
}

// inferCollectorFromMetric 根据指标推断采集器类型（委托给注册中心）
func (s *QueryService) inferCollectorFromMetric(metric string) string {
	return s.cfg.Registry.InferCollectorFromMetric(metric)
}

// ExecuteTimeSeriesQuery 执行时序查询（单 metric，向后兼容）
func (s *QueryService) ExecuteTimeSeriesQuery(ctx context.Context, clusterID string, req *model.TimeSeriesRequest) (*model.TimeSeriesResponse, error) {
	// 复用多 metric 方法
	metrics := []string{req.Metric}
	return s.ExecuteMultiMetricTimeSeriesQuery(ctx, clusterID, req, metrics)
}

// ExecuteMultiMetricTimeSeriesQuery 执行多 metric 时序查询
func (s *QueryService) ExecuteMultiMetricTimeSeriesQuery(ctx context.Context, clusterName string, req *model.TimeSeriesRequest, metrics []string) (*model.TimeSeriesResponse, error) {
	slog.Debug("[ExecuteMultiMetricTimeSeriesQuery] entry",
		"cluster", clusterName,
		"job", req.Job,
		"metrics", metrics,
		"interval", req.Interval,
		"agg", req.Agg,
		"by", req.By,
		"from", req.From,
		"to", req.To,
	)

	// 解析 cluster 参数
	cn, ct := config.ParseClusterFilter(req.Cluster)

	// 获取 ES 客户端和集群信息
	esClient, clusterInfo, err := s.esManager.GetClientForCluster(cn)
	if err != nil {
		return nil, err
	}

	// 单 Tag 自动优化
	if ct == "" && len(clusterInfo.Tags) == 1 {
		ct = clusterInfo.Tags[0]
	}

	// 将别名替换为集群真实名称，用于 ES 查询过滤
	cn = clusterInfo.Name

	// 解析 JobID
	jobFilters, err := s.parserSvc.BuildJobFilter(req.Job)
	if err != nil {
		return nil, fmt.Errorf("invalid job id: %w", err)
	}

	// 解析时间范围
	from, err := s.parserSvc.ParseTime(req.From)
	if err != nil {
		return nil, fmt.Errorf("parse from time: %w", err)
	}
	to, err := s.parserSvc.ParseTime(req.To)
	if err != nil {
		return nil, fmt.Errorf("parse to time: %w", err)
	}
	if err := s.parserSvc.ValidateTimeRange(from, to, s.cfg.MaxTimeRangeDays); err != nil {
		return nil, err
	}

	// 按 collector 分组 metrics
	metricsByCollector := GroupMetricsByCollector(metrics, s.cfg.Registry)

	// 初始化响应
	response := &model.TimeSeriesResponse{
		Metrics:   metrics,
		Interval:  req.Interval,
		TimeRange: &model.TimeRange{From: from.Format(time.RFC3339), To: to.Format(time.RFC3339)},
		Records:   []model.TimeSeriesRecord{},
		Stats:     make(map[string]*model.TimeSeriesStats),
	}

	// 确定 routing
	routing := ""
	if ct != "" {
		routing = ct
	}

	// 对每个 collector 执行查询
	for collector, collectorMetrics := range metricsByCollector {
		query := s.BuildMultiMetricTimeSeriesQuery(req, from, to, collectorMetrics, collector, cn, ct, jobFilters)

		indices, err := s.indexSvc.ResolveIndices(collector, from, to, s.cfg.DefaultCollectors)
		if err != nil {
			return nil, fmt.Errorf("resolve indices: %w", err)
		}

		startTime := time.Now()
		result, err := esClient.Search(ctx, indices, query, routing)
		if err != nil {
			return nil, fmt.Errorf("execute search: %w", err)
		}
		queryTimeMs := time.Since(startTime).Milliseconds()

		slog.Debug("timeseries query executed",
			"cluster", cn,
			"took_ms", queryTimeMs,
			"metrics", collectorMetrics,
			"collector", collector,
			"interval", req.Interval,
			"indices", indices,
		)

		s.parseMultiMetricAggregation(result.Aggregations, req, collectorMetrics, response)
	}

	slog.Info("[ExecuteMultiMetricTimeSeriesQuery] completed",
		"cluster", cn,
		"job", req.Job,
		"metrics", metrics,
		"records", len(response.Records),
	)

	return response, nil
}

// parseMultiMetricAggregation 解析多 metric 聚合结果
func (s *QueryService) parseMultiMetricAggregation(aggs map[string]any, req *model.TimeSeriesRequest, metrics []string, response *model.TimeSeriesResponse) {
	if aggs == nil {
		return
	}

	// 解析每个 metric 的全局统计
	for _, metric := range metrics {
		statsKey := "stats_" + metric
		if globalStats, ok := aggs[statsKey].(map[string]any); ok {
			stats := &model.TimeSeriesStats{}
			if max, ok := globalStats["max"].(float64); ok {
				stats.GlobalMax = max
			}
			if avg, ok := globalStats["avg"].(float64); ok {
				stats.GlobalAvg = avg
			}
			response.Stats[metric] = stats
		}
	}

	// 处理分组聚合
	groupByKey := "group_by_" + req.By
	if groupBuckets, ok := aggs[groupByKey].(map[string]any); ok {
		if buckets, ok := groupBuckets["buckets"].([]any); ok {
			for _, b := range buckets {
				bucket, ok := b.(map[string]any)
				if !ok {
					continue
				}

				label := ""
				if key, ok := bucket["key"].(string); ok {
					label = key
				}

				// 解析时序数据（扁平化每条记录点）
				if tsAgg, ok := bucket["timeseries"].(map[string]any); ok {
					if tsBuckets, ok := tsAgg["buckets"].([]any); ok {
						for _, tb := range tsBuckets {
							tsBucket, ok := tb.(map[string]any)
							if !ok {
								continue
							}

							timestamp := ""
							if keyAsString, ok := tsBucket["key_as_string"].(string); ok {
								timestamp = keyAsString
							}

							// 为每个 metric 产生一条 record
							for _, metric := range metrics {
								aggName := req.Agg + "_" + metric
								var value float64
								if metricAgg, ok := tsBucket[aggName].(map[string]any); ok {
									if v, ok := metricAgg["value"].(float64); ok {
										value = v
									}
								}

								response.Records = append(response.Records, model.TimeSeriesRecord{
									Metric:    metric,
									Label:     label,
									Timestamp: timestamp,
									Value:     value,
								})
							}
						}
					}
				}
			}
		}
	} else {
		// 无分组，直接解析 timeseries（扁平化每条记录点）
		if tsAgg, ok := aggs["timeseries"].(map[string]any); ok {
			if buckets, ok := tsAgg["buckets"].([]any); ok {
				for _, b := range buckets {
					bucket, ok := b.(map[string]any)
					if !ok {
						continue
					}

					timestamp := ""
					if keyAsString, ok := bucket["key_as_string"].(string); ok {
						timestamp = keyAsString
					}

					// 为每个 metric 产生一条 record
					for _, metric := range metrics {
						aggName := req.Agg + "_" + metric
						var value float64
						if metricAgg, ok := bucket[aggName].(map[string]any); ok {
							if v, ok := metricAgg["value"].(float64); ok {
								value = v
							}
						}

						response.Records = append(response.Records, model.TimeSeriesRecord{
							Metric:    metric,
							Label:     "",
							Timestamp: timestamp,
							Value:     value,
						})
					}
				}
			}
		}
	}

	// 如果没有全局统计，从时序数据计算
	for metric, stats := range response.Stats {
		if stats.GlobalMax == 0 && stats.GlobalAvg == 0 {
			calculateStatsFromRecords(response.Records, metric, stats)
		}
	}

	// 为没有统计的 metric 补充统计
	for _, metric := range metrics {
		if _, ok := response.Stats[metric]; !ok {
			stats := &model.TimeSeriesStats{}
			calculateStatsFromRecords(response.Records, metric, stats)
			response.Stats[metric] = stats
		}
	}

	// LOG_REASON: 记录聚合解析结果摘要，便于确认 ES 返回的聚合是否被正确解析为时序数据
	slog.Debug("[parseMultiMetricAggregation] parsed",
		"metrics", metrics,
		"record_count", len(response.Records),
		"stats_count", len(response.Stats),
	)
}

// calculateStatsFromRecords 从扁平时序记录计算统计值
func calculateStatsFromRecords(records []model.TimeSeriesRecord, metric string, stats *model.TimeSeriesStats) {
	var maxVal float64
	var sumVal float64
	var count int

	for _, r := range records {
		if r.Metric != metric {
			continue
		}
		if r.Value > maxVal {
			maxVal = r.Value
		}
		sumVal += r.Value
		count++
	}

	stats.GlobalMax = maxVal
	if count > 0 {
		stats.GlobalAvg = sumVal / float64(count)
	}
}

// BuildSummaryQuery 构建 Summary 接口的 ES DSL
func (s *QueryService) BuildSummaryQuery(jobRaw string, collectors []string, clusterName, clusterTag string, jobFilters []map[string]any) map[string]any {
	slog.Debug("[BuildSummaryQuery] building query",
		"job", jobRaw,
		"collectors", collectors,
	)

	// 构建 filter 条件
	filters := []map[string]any{
		{
			"term": map[string]any{
				"job_info.cluster_name.keyword": clusterName,
			},
		},
	}
	if clusterTag != "" {
		filters = append(filters, map[string]any{
			"term": map[string]any{"job_info.clusterTag.keyword": clusterTag},
		})
	}
	filters = append(filters, jobFilters...)

	// 构建聚合 — 使用 data.summary.* 字段
	aggregations := map[string]any{
		"first_seen": map[string]any{
			"min": map[string]any{"field": "@timestamp"},
		},
		"last_seen": map[string]any{
			"max": map[string]any{"field": "@timestamp"},
		},
		"unique_hosts": map[string]any{
			"cardinality": map[string]any{"field": "hostname.keyword"},
		},
		// 跨采集器统计
		"cpu_stats": map[string]any{
			"extended_stats": map[string]any{
				"field": "data.summary.cpuPercent",
				"sigma": 2,
			},
		},
		"mem_stats": map[string]any{
			"extended_stats": map[string]any{
				"field": "data.summary.mem_rss_kb",
				"sigma": 2,
			},
		},
		"io_bytes_stats": map[string]any{
			"sum": map[string]any{"field": "data.summary.read_bytes"},
		},
	}

	query := map[string]any{
		"query": map[string]any{
			"bool": map[string]any{
				"filter": filters,
			},
		},
		"size": 0,
		"aggs": aggregations,
	}

	return query
}

// ExecuteSummaryQuery 执行 Summary 查询
func (s *QueryService) ExecuteSummaryQuery(ctx context.Context, clusterName string, req *model.SummaryRequest) (*model.SummaryResponse, error) {
	slog.Debug("[ExecuteSummaryQuery] entry",
		"cluster", clusterName,
		"job", req.Job,
		"collectors", req.Collectors,
	)

	// 解析 cluster 参数
	cn, ct := config.ParseClusterFilter(req.Cluster)

	// 获取 ES 客户端和集群信息
	esClient, clusterInfo, err := s.esManager.GetClientForCluster(cn)
	if err != nil {
		return nil, err
	}

	// 单 Tag 自动优化
	if ct == "" && len(clusterInfo.Tags) == 1 {
		ct = clusterInfo.Tags[0]
	}

	// 将别名替换为集群真实名称，用于 ES 查询过滤
	cn = clusterInfo.Name

	// 解析 JobID
	jobFilters, err := s.parserSvc.BuildJobFilter(req.Job)
	if err != nil {
		return nil, fmt.Errorf("invalid job id: %w", err)
	}

	// 构建查询
	query := s.BuildSummaryQuery(req.Job, s.cfg.DefaultCollectors, cn, ct, jobFilters)

	// 解析索引
	now := time.Now()
	from := now.AddDate(0, 0, -s.cfg.MaxTimeRangeDays)
	indices, err := s.indexSvc.ResolveIndices("", from, now, s.cfg.DefaultCollectors)
	if err != nil {
		return nil, fmt.Errorf("resolve indices: %w", err)
	}

	// 确定 routing
	routing := ""
	if ct != "" {
		routing = ct
	}

	// 执行查询
	startTime := time.Now()
	result, err := esClient.Search(ctx, indices, query, routing)
	if err != nil {
		return nil, fmt.Errorf("execute search: %w", err)
	}
	queryTimeMs := time.Since(startTime).Milliseconds()

	slog.Debug("summary query executed",
		"cluster", cn,
		"job", req.Job,
		"took_ms", queryTimeMs,
		"indices", len(indices),
		"routing", routing,
	)

	// 解析聚合结果
	response := s.parseSummaryAggregation(result.Aggregations, req, s.cfg.DefaultCollectors)

	slog.Info("[ExecuteSummaryQuery] completed",
		"cluster", cn,
		"job", req.Job,
		"first_seen", response.Time.FirstSeen,
		"last_seen", response.Time.LastSeen,
		"duration_sec", response.Time.DurationSec,
		"stats_keys", len(response.Stats),
	)

	return response, nil
}

// parseSummaryAggregation 解析 Summary 聚合结果
func (s *QueryService) parseSummaryAggregation(aggs map[string]any, req *model.SummaryRequest, collectors []string) *model.SummaryResponse {
	response := &model.SummaryResponse{
		Job:     req.Job,
		Cluster: req.Cluster,
		Time:    &model.JobTimeRange{},
		Scope:   &model.JobScope{},
		Stats:   make(map[string]any),
	}

	if aggs == nil {
		return response
	}

	// 解析时间范围
	if firstSeen, ok := aggs["first_seen"].(map[string]any); ok {
		if value, ok := firstSeen["value_as_string"].(string); ok {
			response.Time.FirstSeen = value
		} else if value, ok := firstSeen["value"].(float64); ok {
			response.Time.FirstSeen = time.UnixMilli(int64(value)).Format(time.RFC3339)
		}
	}
	if lastSeen, ok := aggs["last_seen"].(map[string]any); ok {
		if value, ok := lastSeen["value_as_string"].(string); ok {
			response.Time.LastSeen = value
		} else if value, ok := lastSeen["value"].(float64); ok {
			response.Time.LastSeen = time.UnixMilli(int64(value)).Format(time.RFC3339)
		}
	}
	if response.Time.FirstSeen != "" && response.Time.LastSeen != "" {
		first, err1 := time.Parse(time.RFC3339, response.Time.FirstSeen)
		last, err2 := time.Parse(time.RFC3339, response.Time.LastSeen)
		if err1 == nil && err2 == nil {
			response.Time.DurationSec = int64(last.Sub(first).Seconds())
		}
	}

	// 解析主机数
	if uniqueHosts, ok := aggs["unique_hosts"].(map[string]any); ok {
		if value, ok := uniqueHosts["value"].(float64); ok {
			response.Scope.Hosts = make([]string, 0, int(value))
		}
	}

	// 解析 CPU 统计
	cpuData := make(map[string]any)
	if cpuStats, ok := aggs["cpu_stats"].(map[string]any); ok {
		if max, ok := cpuStats["max"].(float64); ok && max > 0 {
			cpuData["max"] = round2(max)
		}
		if avg, ok := cpuStats["avg"].(float64); ok && avg > 0 {
			cpuData["avg"] = round2(avg)
			if stdDev, ok := cpuStats["std_deviation"].(float64); ok {
				cpuData["p99"] = round2(avg + 2*stdDev)
			}
		}
	}
	if len(cpuData) > 0 {
		response.Stats["cpu"] = cpuData
	}

	// 解析内存统计
	memData := make(map[string]any)
	if memStats, ok := aggs["mem_stats"].(map[string]any); ok {
		if max, ok := memStats["max"].(float64); ok && max > 0 {
			memData["max_kb"] = int64(max)
		}
		if avg, ok := memStats["avg"].(float64); ok && avg > 0 {
			memData["avg_kb"] = int64(avg)
		}
	}
	if len(memData) > 0 {
		response.Stats["mem"] = memData
	}

	// 解析 IO 统计
	if ioBytes, ok := aggs["io_bytes_stats"].(map[string]any); ok {
		if value, ok := ioBytes["value"].(float64); ok && value > 0 {
			response.Stats["io"] = map[string]any{"total_bytes": int64(value)}
		}
	}

	// 设置 scope
	response.Scope.Collectors = collectors

	slog.Debug("[parseSummaryAggregation] parsed",
		"job", response.Job,
		"first_seen", response.Time.FirstSeen,
		"last_seen", response.Time.LastSeen,
		"duration_sec", response.Time.DurationSec,
		"stats_keys_count", len(response.Stats),
	)

	return response
}

// round2 保留两位小数
func round2(v float64) float64 {
	return float64(int(v*100+0.5)) / 100
}

// CheckJobExists 检查指定集群中是否存在某个 Job 的数据
// 使用 ES _search size=0 + min/max 聚合，同时获得 count 和 first_seen/last_seen
// clusterTag 为空时不过滤 tag，查询所有 tag 的数据
func (s *QueryService) CheckJobExists(ctx context.Context, clusterName, clusterTag, jobID string) (*model.CheckJobResponse, error) {
	// 获取集群信息
	_, ok := s.clusterMgr.Get(clusterName)
	if !ok {
		return nil, fmt.Errorf("cluster not found: %s", clusterName)
	}

	// 获取 ES 客户端
	esClient, info, err := s.esManager.GetClientForCluster(clusterName)
	if err != nil {
		return nil, err
	}

	// 单 Tag 自动优化：用户未指定 tag 但集群只有一个 → 自动补充
	if clusterTag == "" && len(info.Tags) == 1 {
		clusterTag = info.Tags[0]
	}

	// 将别名替换为集群真实名称，用于 ES 查询过滤
	realName := info.Name

	// 构建过滤条件
	filters := []map[string]any{
		{"term": map[string]any{"job_info.cluster_name.keyword": realName}},
		{"term": map[string]any{"job_info.NativeJobID.keyword": jobID}},
	}
	if clusterTag != "" {
		filters = append(filters, map[string]any{
			"term": map[string]any{"job_info.clusterTag.keyword": clusterTag},
		})
	}

	query := map[string]any{
		"query": map[string]any{
			"bool": map[string]any{
				"filter": filters,
			},
		},
		"size": 0,
		"aggs": map[string]any{
			"first_seen": map[string]any{"min": map[string]any{"field": "@timestamp"}},
			"last_seen":  map[string]any{"max": map[string]any{"field": "@timestamp"}},
		},
	}

	// 使用通配符索引覆盖全部采集器和时间段
	indices := make([]string, 0, len(s.cfg.DefaultCollectors))
	for _, coll := range s.cfg.DefaultCollectors {
		indices = append(indices, s.cfg.Registry.RenderIndexName(coll, "*"))
	}

	slog.Debug("[CheckJobExists] checking job existence",
		"cluster_name", clusterName,
		"cluster_tag", clusterTag,
		"job_id", jobID,
		"indices", indices,
	)

	// 确定 routing
	routing := ""
	if clusterTag != "" {
		routing = clusterTag
	}

	// 执行 _search size=0 + 聚合（不返回文档体，与 _count 性能相当）
	result, err := esClient.Search(ctx, indices, query, routing)
	if err != nil {
		return nil, fmt.Errorf("search failed: %w", err)
	}

	count := result.Total

	// 解析 first_seen/last_seen
	var timeRange *model.JobTimeRange
	if count > 0 {
		var firstTime, lastTime time.Time
		if aggs, ok := result.Aggregations["first_seen"].(map[string]any); ok {
			if v, ok := aggs["value_as_string"].(string); ok {
				firstTime, _ = time.Parse(time.RFC3339, v)
			} else if v, ok := aggs["value"].(float64); ok {
				firstTime = time.UnixMilli(int64(v))
			}
		}
		if aggs, ok := result.Aggregations["last_seen"].(map[string]any); ok {
			if v, ok := aggs["value_as_string"].(string); ok {
				lastTime, _ = time.Parse(time.RFC3339, v)
			} else if v, ok := aggs["value"].(float64); ok {
				lastTime = time.UnixMilli(int64(v))
			}
		}
		if !firstTime.IsZero() && !lastTime.IsZero() {
			timeRange = &model.JobTimeRange{
				FirstSeen:   firstTime.Format(time.RFC3339),
				LastSeen:    lastTime.Format(time.RFC3339),
				DurationSec: int64(lastTime.Sub(firstTime).Seconds()),
			}
		}
	}

	slog.Debug("[CheckJobExists] completed",
		"cluster_name", realName,
		"job_id", jobID,
		"count", count,
		"exists", count > 0,
	)

	slog.Info("[CheckJobExists] completed",
		"cluster", realName,
		"job_id", jobID,
		"exists", count > 0,
		"count", count,
	)

	return &model.CheckJobResponse{
		Exists:  count > 0,
		Count:   count,
		JobID:   jobID,
		Cluster: realName,
		Time:    timeRange,
	}, nil
}
