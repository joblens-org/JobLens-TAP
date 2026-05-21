package tests

import (
	"context"
	"testing"
	"time"

	"github.com/joblens/tap/internal/model"
	"github.com/joblens/tap/internal/service"
)

var querySvc *service.QueryService

// initQueryService 初始化查询服务
func initQueryService() {
	if querySvc == nil {
		querySvc = service.NewQueryService(appCfg, esManager, clusterMgr)
	}
}

// TestParserService_ParseTime 测试时间解析
func TestParserService_ParseTime(t *testing.T) {
	initQueryService()

	parser := querySvc.ParserService()

	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"ISO8601", "2026-04-05T10:00:00Z", false},
		{"now", "now", false},
		{"now-1h", "now-1h", false},
		{"now-1d", "now-1d", false},
		{"now-30m", "now-30m", false},
		{"relative 1h", "1h", false},
		{"relative 1d", "1d", false},
		{"invalid", "invalid-time", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parser.ParseTime(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Errorf("ParseTime(%s) expected error, got nil", tt.input)
				}
			} else {
				if err != nil {
					t.Errorf("ParseTime(%s) unexpected error: %v", tt.input, err)
				}
				if result.IsZero() {
					t.Errorf("ParseTime(%s) returned zero time", tt.input)
				}
				t.Logf("ParseTime(%s) = %v", tt.input, result)
			}
		})
	}
}

// TestParserService_ParseFields 测试字段解析
func TestParserService_ParseFields(t *testing.T) {
	initQueryService()

	parser := querySvc.ParserService()

	tests := []struct {
		name     string
		input    string
		expected int
	}{
		{"empty", "", 0},
		{"single", "cpu", 1},
		{"multiple", "cpu,mem,host", 3},
		{"with spaces", "cpu, mem , host", 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parser.ParseFields(tt.input)
			if len(result) != tt.expected {
				t.Errorf("ParseFields(%s) = %v, expected %d fields", tt.input, result, tt.expected)
			}
		})
	}
}

// TestParserService_ValidateTimeRange 测试时间范围验证
func TestParserService_ValidateTimeRange(t *testing.T) {
	initQueryService()

	parser := querySvc.ParserService()

	now := time.Now()

	// 有效范围
	err := parser.ValidateTimeRange(now.Add(-1*time.Hour), now, 7)
	if err != nil {
		t.Errorf("Valid time range should pass: %v", err)
	}

	// 无效范围：from > to
	err = parser.ValidateTimeRange(now, now.Add(-1*time.Hour), 7)
	if err == nil {
		t.Error("Invalid time range (from > to) should fail")
	}

	// 超出最大范围
	err = parser.ValidateTimeRange(now.Add(-10*24*time.Hour), now, 7)
	if err == nil {
		t.Error("Time range exceeding max days should fail")
	}
}

// TestIndexService_ResolveIndices 测试索引解析
func TestIndexService_ResolveIndices(t *testing.T) {
	skipIfShort(t)
	initQueryService()

	indexSvc := querySvc.IndexService()

	from := time.Now().Add(-24 * time.Hour)
	to := time.Now()

	// 测试无 collector 过滤
	indices, err := indexSvc.ResolveIndices("", from, to, nil)
	if err != nil {
		t.Fatalf("ResolveIndices failed: %v", err)
	}
	t.Logf("Resolved indices (no filter): %v", indices)

	// 测试指定 collector
	indices, err = indexSvc.ResolveIndices("cpumem", from, to, nil)
	if err != nil {
		t.Fatalf("ResolveIndices with collector failed: %v", err)
	}
	t.Logf("Resolved indices (cpumem): %v", indices)
}

// TestIndexService_ParseClusterParam 测试集群参数解析
func TestIndexService_ParseClusterParam(t *testing.T) {
	initQueryService()

	indexSvc := querySvc.IndexService()

	// 测试单个集群
	clusters, err := indexSvc.ParseClusterParam(testCfg.ClusterID)
	if err != nil {
		t.Fatalf("ParseClusterParam failed: %v", err)
	}
	if len(clusters) != 1 || clusters[0] != testCfg.ClusterID {
		t.Errorf("Expected [%s], got %v", testCfg.ClusterID, clusters)
	}

	// 测试通配符
	clusters, err = indexSvc.ParseClusterParam("*")
	if err != nil {
		t.Fatalf("ParseClusterParam with wildcard failed: %v", err)
	}
	if len(clusters) == 0 {
		t.Error("Expected at least one cluster with wildcard")
	}
	t.Logf("Wildcard clusters: %v", clusters)

	// 测试不存在的集群
	_, err = indexSvc.ParseClusterParam("nonexistent")
	if err == nil {
		t.Error("Expected error for nonexistent cluster")
	}
}

// TestQueryService_BuildRawQuery 测试 Raw DSL 构建
func TestQueryService_BuildRawQuery(t *testing.T) {
	initQueryService()

	req := &model.RawQueryRequest{
		Cluster:   testCfg.ClusterID,
		Job:       testCfg.JobID,
		From:      "now-1h",
		To:        "now",
		Collector: "cpumem",
		Fields:    "cpu,mem",
		Size:      100,
	}

	from := time.Now().Add(-1 * time.Hour)
	to := time.Now()
	fields := []string{"cpu", "mem"}

	query := querySvc.BuildRawQuery(req, from, to, nil, fields, testCfg.ClusterID, "", nil)

	// 验证查询结构
	if query["size"] != 100 {
		t.Errorf("Expected size 100, got %v", query["size"])
	}

	if query["query"] == nil {
		t.Error("Expected query to be set")
	}

	t.Logf("Built query: %+v", query)
}

// TestQueryService_ExecuteRawQuery 测试 Raw 查询执行
func TestQueryService_ExecuteRawQuery(t *testing.T) {
	skipIfShort(t)
	skipIfNoJobID(t)
	initQueryService()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	req := &model.RawQueryRequest{
		Cluster: testCfg.ClusterID,
		Job:     testCfg.JobID,
		From:    "2026-03-28T00:00:00+08:00",
		To:      "2026-03-28T23:59:59+08:00",
		Size:    10,
	}

	resp, err := querySvc.ExecuteRawQuery(ctx, testCfg.ClusterID, req, nil)
	if err != nil {
		t.Fatalf("ExecuteRawQuery failed: %v", err)
	}

	if resp == nil {
		t.Fatal("Expected non-nil response")
	}

	t.Logf("Raw query returned %d records, total %d", resp.Pagination.Returned, resp.Pagination.Total)

	// 验证记录结构
	for i, record := range resp.Records {
		if i >= 3 {
			break
		}
		t.Logf("Record %d: cluster=%s, collector=%s, time=%s, host=%s",
			i, record.Cluster, record.Collector, record.Time, record.Host)
	}
}

// TestQueryService_ExecuteRawQuery_WithFields 测试带字段的 Raw 查询
func TestQueryService_ExecuteRawQuery_WithFields(t *testing.T) {
	skipIfShort(t)
	skipIfNoJobID(t)
	initQueryService()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	req := &model.RawQueryRequest{
		Cluster: testCfg.ClusterID,
		Job:     testCfg.JobID,
		From:    "2026-03-28T00:00:00+08:00",
		To:      "2026-03-28T23:59:59+08:00",
		Fields:  "cpu,mem",
		Size:    10,
	}

	resp, err := querySvc.ExecuteRawQuery(ctx, testCfg.ClusterID, req, nil)
	if err != nil {
		t.Fatalf("ExecuteRawQuery failed: %v", err)
	}

	t.Logf("Raw query with fields returned %d records", len(resp.Records))

	for i, record := range resp.Records {
		if i >= 3 {
			break
		}
		t.Logf("Record %d: cpu=%v, mem=%v, fields=%v",
			i, record.CPU, record.Mem, record.Fields)
	}
}

// TestQueryService_ExecuteMultiMetricTimeSeriesQuery 测试时序查询
func TestQueryService_ExecuteMultiMetricTimeSeriesQuery(t *testing.T) {
	skipIfShort(t)
	skipIfNoJobID(t)
	initQueryService()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	req := &model.TimeSeriesRequest{
		Cluster:  testCfg.ClusterID,
		Job:      testCfg.JobID,
		Metric:   "cpu,mem",
		Interval: "1m",
		From:     "2026-03-28T00:00:00+08:00",
		To:       "2026-03-28T23:59:59+08:00",
		Agg:      "avg",
	}

	metrics := []string{"cpu", "mem"}

	resp, err := querySvc.ExecuteMultiMetricTimeSeriesQuery(ctx, testCfg.ClusterID, req, metrics)
	if err != nil {
		t.Fatalf("ExecuteMultiMetricTimeSeriesQuery failed: %v", err)
	}

	if resp == nil {
		t.Fatal("Expected non-nil response")
	}

	t.Logf("Timeseries query: metrics=%v, record_count=%d", resp.Metrics, len(resp.Records))

	// 统计每个 metric 的 record 数量
	metricCounts := make(map[string]int)
	for _, r := range resp.Records {
		metricCounts[r.Metric]++
	}
	for metric, count := range metricCounts {
		t.Logf("Metric %s: %d records", metric, count)
	}

	for metric, stats := range resp.Stats {
		t.Logf("Stats %s: max=%.2f, avg=%.2f", metric, stats.GlobalMax, stats.GlobalAvg)
	}
}

// TestQueryService_ExecuteMultiMetricTimeSeriesQuery_GroupByHost 测试按主机分组
func TestQueryService_ExecuteMultiMetricTimeSeriesQuery_GroupByHost(t *testing.T) {
	skipIfShort(t)
	skipIfNoJobID(t)
	initQueryService()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	req := &model.TimeSeriesRequest{
		Cluster:  testCfg.ClusterID,
		Job:      testCfg.JobID,
		Metric:   "cpu",
		Interval: "5m",
		From:     "2026-03-28T00:00:00+08:00",
		To:       "2026-03-28T23:59:59+08:00",
		Agg:      "avg",
		By:       "host",
	}

	metrics := []string{"cpu"}

	resp, err := querySvc.ExecuteMultiMetricTimeSeriesQuery(ctx, testCfg.ClusterID, req, metrics)
	if err != nil {
		t.Fatalf("ExecuteMultiMetricTimeSeriesQuery with group by failed: %v", err)
	}

	t.Logf("Timeseries with group by host: %d records", len(resp.Records))

	// 显示前几条记录
	for i, r := range resp.Records {
		if i >= 5 {
			break
		}
		t.Logf("Record %d: metric=%s, host=%s, timestamp=%s, value=%.2f", i, r.Metric, r.Label, r.Timestamp, r.Value)
	}
}

// TestQueryService_ExecuteSummaryQuery 测试摘要查询
func TestQueryService_ExecuteSummaryQuery(t *testing.T) {
	skipIfShort(t)
	skipIfNoJobID(t)
	initQueryService()

	t.Skip("Skipping summary query test - actual data time range is not recent enough")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	req := &model.SummaryRequest{
		Cluster: testCfg.ClusterID,
		Job:     testCfg.JobID,
	}

	resp, err := querySvc.ExecuteSummaryQuery(ctx, testCfg.ClusterID, req)
	if err != nil {
		t.Fatalf("ExecuteSummaryQuery failed: %v", err)
	}

	if resp == nil {
		t.Fatal("Expected non-nil response")
	}

	t.Logf("Summary: job=%d, cluster=%s", resp.Job, resp.Cluster)
	t.Logf("Time: first=%s, last=%s, duration=%ds",
		resp.Time.FirstSeen, resp.Time.LastSeen, resp.Time.DurationSec)
	t.Logf("Scope: collectors=%v, samples=%d",
		resp.Scope.Collectors, resp.Scope.SamplesCount)

	for collector, stats := range resp.Stats {
		t.Logf("Stats[%s]: %+v", collector, stats)
	}
}

// TestQueryService_ExecuteSummaryQuery_WithCollectors 测试指定采集器的摘要
func TestQueryService_ExecuteSummaryQuery_WithCollectors(t *testing.T) {
	skipIfShort(t)
	skipIfNoJobID(t)
	initQueryService()

	t.Skip("Skipping summary query test - actual data time range is not recent enough")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	req := &model.SummaryRequest{
		Cluster:    testCfg.ClusterID,
		Job:        testCfg.JobID,
		Collectors: "cpumem_collector",
	}

	resp, err := querySvc.ExecuteSummaryQuery(ctx, testCfg.ClusterID, req)
	if err != nil {
		t.Fatalf("ExecuteSummaryQuery failed: %v", err)
	}

	t.Logf("Summary with cpumem_collector only: %+v", resp.Stats)
}

// TestQueryService_ParseMetrics 测试 metric 解析
func TestQueryService_ParseMetrics(t *testing.T) {
	initQueryService()

	tests := []struct {
		name     string
		input    string
		expected int
	}{
		{"single", "cpu", 1},
		{"multiple", "cpu,mem", 2},
		{"three", "cpu,mem,host", 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := querySvc.ParseMetrics(tt.input)
			if len(result) != tt.expected {
				t.Errorf("ParseMetrics(%s) = %v, expected %d", tt.input, result, tt.expected)
			}
		})
	}
}

// TestGroupMetricsByCollector 测试按 collector 分组
func TestGroupMetricsByCollector(t *testing.T) {
	tests := []struct {
		name     string
		metrics  []string
		expected map[string]int
	}{
		{
			name:    "process metrics",
			metrics: []string{"cpu", "mem", "name"},
			expected: map[string]int{
				"cpumem": 3,
			},
		},
		{
			name:    "mixed metrics",
			metrics: []string{"cpu", "io_bytes"},
			expected: map[string]int{
				"cpumem": 1,
				"io":     1,
			},
		},
		{
			name:    "generic metrics",
			metrics: []string{"host", "time"},
			expected: map[string]int{
				"": 2,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			registry := model.BuildDefaultRegistry()
			result := service.GroupMetricsByCollector(tt.metrics, registry)
			for collector, count := range tt.expected {
				if len(result[collector]) != count {
					t.Errorf("GroupMetricsByCollector: expected %d metrics for collector '%s', got %d",
						count, collector, len(result[collector]))
				}
			}
		})
	}
}
