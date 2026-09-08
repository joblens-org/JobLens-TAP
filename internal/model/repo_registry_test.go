package model

import (
	"os"
	"path/filepath"
	"testing"
)

// 仓库自带的 collector-registry.json 必须能被正确加载，且关键采集器/别名就位
func TestRepoRegistryFile(t *testing.T) {
	path := filepath.Join("..", "..", "collector-registry.json")
	if _, err := os.Stat(path); err != nil {
		t.Skipf("registry file not found: %v", err)
	}

	r, err := LoadRegistry(path)
	if err != nil {
		t.Fatalf("仓库注册文件加载失败: %v", err)
	}

	for _, name := range []string{"cpumem", "io", "new_io_usage", "fs_metadata", "net", "gpu"} {
		if !r.HasCollector(name) {
			t.Errorf("仓库注册文件缺少采集器 %s", name)
		}
	}

	agentNames := map[string]string{
		"cpumem":      "cpumem_collector",
		"new_io_usage": "new_io_usage_collector",
		"fs_metadata": "fs_metadata_collector",
	}
	for collector, want := range agentNames {
		if got := r.GetAgentName(collector); got != want {
			t.Errorf("GetAgentName(%q) = %q, want %q", collector, got, want)
		}
	}

	if es, ok := r.GetESField("io_bytes"); !ok || es != "data.job_total.rchar" {
		t.Errorf("io_bytes 应指向 data.job_total.rchar, got %q", es)
	}
	if es, ok := r.GetESField("io_legacy"); !ok || es != "data.summary.read_bytes" {
		t.Errorf("io_legacy 应指向 data.summary.read_bytes, got %q", es)
	}
	if got := r.RenderIndexName("new_io_usage", "2026.09.08"); got != "new_io_usage_collector_2026.09.08" {
		t.Errorf("new_io_usage 索引渲染 = %q", got)
	}
	if got := r.RenderIndexName("fs_metadata", "2026.09.08"); got != "fs_metadata_collector_2026.09.08" {
		t.Errorf("fs_metadata 索引渲染 = %q", got)
	}

	if n := len(r.SummaryAliases()); n < 6 {
		t.Errorf("summary_agg 别名数 = %d, want >= 6", n)
	}
	if n := len(r.RecordAliases()); n < 4 {
		t.Errorf("record_field 别名数 = %d, want >= 4", n)
	}
}
