package service

import (
	"testing"

	"github.com/joblens/tap/internal/config"
	"github.com/joblens/tap/internal/model"
	"github.com/joblens/tap/internal/repository"
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

func newFlattenHit(index string, data map[string]any) repository.SearchHit {
	return repository.SearchHit{
		Index:  index,
		Source: map[string]any{"hostname": "node1", "@timestamp": "2026-09-08T10:00:00Z", "data": data},
	}
}

func TestFlattenHit_RecordFieldDriven(t *testing.T) {
	s := newTestQueryService()
	registry := s.cfg.Registry

	t.Run("cpumem文档提取cpu/mem/name", func(t *testing.T) {
		hit := newFlattenHit("cpumem_collector_2026.09.08", map[string]any{
			"summary": map[string]any{"cpuPercent": 150.5, "mem_rss_kb": 4096.0, "name": "python"},
		})
		r := FlattenHit(hit, "sz01", true, registry)
		if r.CPU == nil || *r.CPU != 150.5 {
			t.Errorf("CPU = %+v", r.CPU)
		}
		if r.Mem == nil || *r.Mem != 4096 {
			t.Errorf("Mem = %+v", r.Mem)
		}
		if r.Name == nil || *r.Name != "python" {
			t.Errorf("Name = %+v", r.Name)
		}
		if r.IOBytes != nil {
			t.Errorf("cpumem 文档不应有 IOBytes, got %d", *r.IOBytes)
		}
	})

	t.Run("new_io_usage文档提取io_bytes", func(t *testing.T) {
		hit := newFlattenHit("new_io_usage_collector_2026.09.08", map[string]any{
			"job_total": map[string]any{"rchar": 8192.0, "wchar": 4096.0},
			"processes": []any{map[string]any{"pid": 1.0, "rchar": 8192.0}},
		})
		r := FlattenHit(hit, "sz01", true, registry)
		if r.IOBytes == nil || *r.IOBytes != 8192 {
			t.Errorf("IOBytes = %+v（应由 record_field=io_bytes 从 data.job_total.rchar 提取）", r.IOBytes)
		}
		if r.CPU != nil {
			t.Error("new_io_usage 文档不应有 CPU 快捷字段")
		}
		if _, ok := r.Fields["data.job_total.rchar"]; !ok {
			t.Error("扁平化字段 data.job_total.rchar 缺失")
		}
	})

	t.Run("旧io文档不再提取io_bytes", func(t *testing.T) {
		hit := newFlattenHit("io_collector_2026.09.08", map[string]any{
			"summary": map[string]any{"read_bytes": 2048.0},
		})
		r := FlattenHit(hit, "sz01", true, registry)
		if r.IOBytes != nil {
			t.Errorf("io_legacy 未声明 record_field，不应提取 IOBytes, got %d", *r.IOBytes)
		}
	})

	t.Run("collector从索引名提取", func(t *testing.T) {
		cases := []struct {
			index string
			want  string
		}{
			{"fs_metadata_collector_2026.09.08", "fs_metadata"},
			{"new_io_usage_collector_2026.09.08", "new_io_usage"},
			{"cpumem_collector_2026.09.08", "cpumem"},
			{"sz01_cpumem_collector_2026.04.27", "cpumem"}, // 旧模式 {site}_{collector}
		}
		for _, tc := range cases {
			hit := newFlattenHit(tc.index, map[string]any{})
			r := FlattenHit(hit, "sz01", true, registry)
			if r.Collector != tc.want {
				t.Errorf("索引 %q 提取 collector = %q, want %q", tc.index, r.Collector, tc.want)
			}
		}
	})
}
func TestBuildMetricAggWithNested(t *testing.T) {
	s := newTestQueryService()

	t.Run("nested别名包裹nested聚合", func(t *testing.T) {
		agg := s.buildMetricAggWithNested("file_rchar", "sum", "data.files.total.rchar")
		nested, ok := agg["nested"].(map[string]any)
		if !ok || nested["path"] != "data.files" {
			t.Fatalf("期望 nested{path: data.files}, got %+v", agg)
		}
		inner, ok := agg["aggs"].(map[string]any)
		if !ok {
			t.Fatal("缺少内层 aggs")
		}
		metric, ok := inner["metric"].(map[string]any)
		if !ok {
			t.Fatal("缺少内层 metric 聚合")
		}
		if sum, ok := metric["sum"].(map[string]any); !ok || sum["field"] != "data.files.total.rchar" {
			t.Errorf("内层 sum 聚合 = %+v", metric)
		}
	})

	t.Run("标量别名不包裹", func(t *testing.T) {
		agg := s.buildMetricAggWithNested("cpu", "avg", "data.summary.cpuPercent")
		if _, hasNested := agg["nested"]; hasNested {
			t.Error("cpu 不应包裹 nested")
		}
		if avg, ok := agg["avg"].(map[string]any); !ok || avg["field"] != "data.summary.cpuPercent" {
			t.Errorf("avg 聚合 = %+v", agg)
		}
	})

	t.Run("stats聚合nested包裹", func(t *testing.T) {
		agg := s.buildStatsAggWithNested("proc_rchar", "data.processes.rchar")
		nested, ok := agg["nested"].(map[string]any)
		if !ok || nested["path"] != "data.processes" {
			t.Fatalf("期望 nested{path: data.processes}, got %+v", agg)
		}
	})
}

func TestExtractMetricValue_Nested(t *testing.T) {
	s := newTestQueryService()

	t.Run("nested结构解包", func(t *testing.T) {
		bucket := map[string]any{
			"sum_file_rchar": map[string]any{
				"doc_count": float64(3),
				"metric":    map[string]any{"value": 4096.0},
			},
		}
		got := s.extractMetricValue(bucket, "sum_file_rchar", "file_rchar")
		if got != 4096 {
			t.Errorf("extractMetricValue = %v, want 4096", got)
		}
	})

	t.Run("标量结构直接取值", func(t *testing.T) {
		bucket := map[string]any{
			"avg_cpu": map[string]any{"value": 3.14},
		}
		got := s.extractMetricValue(bucket, "avg_cpu", "cpu")
		if got != 3.14 {
			t.Errorf("extractMetricValue = %v, want 3.14", got)
		}
	})

	t.Run("空bucket返回0", func(t *testing.T) {
		got := s.extractMetricValue(map[string]any{}, "avg_cpu", "cpu")
		if got != 0 {
			t.Errorf("extractMetricValue = %v, want 0", got)
		}
	})
}
