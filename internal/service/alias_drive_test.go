package service

import (
	"testing"

	"github.com/joblens/tap/internal/config"
	"github.com/joblens/tap/internal/model"
)

// newTestQueryService 构建不依赖 ES 的查询服务（仅测试查询构建与解析逻辑）
func newTestQueryService() *QueryService {
	return NewQueryService(&config.Config{
		Registry: model.BuildDefaultRegistry(),
	}, nil, nil)
}

func TestBuildSummaryQuery_ConfigDriven(t *testing.T) {
	s := newTestQueryService()

	query := s.BuildSummaryQuery("172.0", nil, "sz01", "", nil)
	aggs, ok := query["aggs"].(map[string]any)
	if !ok {
		t.Fatal("查询缺少 aggs")
	}

	// 通用聚合保留
	for _, name := range []string{"first_seen", "last_seen", "unique_hosts"} {
		if _, ok := aggs[name]; !ok {
			t.Errorf("缺少通用聚合 %s", name)
		}
	}

	// cpumem: extended_stats
	cpuStats, ok := aggs["cpu_stats"].(map[string]any)
	if !ok {
		t.Fatal("缺少 cpu_stats（应由 summary_agg=extended_stats 声明生成）")
	}
	es, ok := cpuStats["extended_stats"].(map[string]any)
	if !ok || es["field"] != "data.summary.cpuPercent" {
		t.Errorf("cpu_stats extended_stats = %+v", cpuStats)
	}

	// new_io_usage: sum 聚合指向新字段
	ioStats, ok := aggs["io_bytes_stats"].(map[string]any)
	if !ok {
		t.Fatal("缺少 io_bytes_stats")
	}
	sumAgg, ok := ioStats["sum"].(map[string]any)
	if !ok || sumAgg["field"] != "data.job_total.rchar" {
		t.Errorf("io_bytes_stats = %+v, 期望 sum 聚合指向 data.job_total.rchar", ioStats)
	}

	// fs_metadata: max 聚合
	mdStats, ok := aggs["metadata_ops_stats"].(map[string]any)
	if !ok {
		t.Fatal("缺少 metadata_ops_stats")
	}
	maxAgg, ok := mdStats["max"].(map[string]any)
	if !ok || maxAgg["field"] != "data.job_metadata_ops_rate" {
		t.Errorf("metadata_ops_stats = %+v", mdStats)
	}

	// 未声明 summary_agg 的别名不参与聚合
	for _, banned := range []string{"io_legacy_stats", "name_stats", "mem_peak_stats", "file_rchar_stats"} {
		if _, ok := aggs[banned]; ok {
			t.Errorf("别名未声明 summary_agg，不应生成聚合 %s", banned)
		}
	}
}

func TestParseSummaryAggregation_ConfigDriven(t *testing.T) {
	s := newTestQueryService()
	req := &model.SummaryRequest{Job: "172.0", Cluster: "sz01"}

	aggs := map[string]any{
		"first_seen": map[string]any{"value_as_string": "2026-09-08T10:00:00Z"},
		"last_seen":  map[string]any{"value_as_string": "2026-09-08T11:00:00Z"},
		"unique_hosts": map[string]any{
			"value": float64(2),
		},
		"cpu_stats": map[string]any{
			"max":           390.0,
			"avg":           200.0,
			"std_deviation": 5.0,
		},
		"mem_stats": map[string]any{
			"max": 1048576.6,
			"avg": 524288.4,
		},
		"io_bytes_stats": map[string]any{
			"value": 1024.0,
		},
		"metadata_ops_stats": map[string]any{
			"value": 12.34567,
		},
	}

	resp := s.parseSummaryAggregation(aggs, req, nil)

	cpu, ok := resp.Stats["cpu"].(map[string]any)
	if !ok || cpu["max"] != 390.0 || cpu["avg"] != 200.0 || cpu["p99"] != 210.0 {
		t.Errorf("stats.cpu = %+v", resp.Stats["cpu"])
	}

	mem, ok := resp.Stats["mem"].(map[string]any)
	if !ok || mem["max"] != int64(1048576) || mem["avg"] != int64(524288) {
		t.Errorf("stats.mem = %+v（long 类型应取整）", resp.Stats["mem"])
	}

	ioBytes, ok := resp.Stats["io_bytes"].(map[string]any)
	if !ok || ioBytes["value"] != int64(1024) {
		t.Errorf("stats.io_bytes = %+v", resp.Stats["io_bytes"])
	}

	mdOps, ok := resp.Stats["metadata_ops"].(map[string]any)
	if !ok || mdOps["value"] != 12.35 {
		t.Errorf("stats.metadata_ops = %+v（float 应保留两位小数）", resp.Stats["metadata_ops"])
	}

	if resp.Time.DurationSec != 3600 {
		t.Errorf("duration = %d, want 3600", resp.Time.DurationSec)
	}
}
