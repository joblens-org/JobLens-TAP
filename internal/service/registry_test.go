package service

import (
	"os"
	"testing"

	"github.com/joblens/tap/internal/model"
)

func TestGroupMetricsByCollector(t *testing.T) {
	r := model.BuildDefaultRegistry()

	tests := []struct {
		name     string
		metrics  []string
		expected map[string]int
	}{
		{
			name:    "cpumem指标",
			metrics: []string{"cpu", "mem", "name"},
			expected: map[string]int{
				"cpumem": 3,
			},
		},
		{
			name:    "混合采集器指标",
			metrics: []string{"cpu", "io_bytes"},
			expected: map[string]int{
				"cpumem": 1,
				"io":     1,
			},
		},
		{
			name:    "全局指标",
			metrics: []string{"host", "time"},
			expected: map[string]int{
				"": 2,
			},
		},
		{
			name:    "未知指标",
			metrics: []string{"unknown_metric"},
			expected: map[string]int{
				"": 1,
			},
		},
		{
			name:    "空指标列表",
			metrics: []string{},
			expected: map[string]int{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GroupMetricsByCollector(tt.metrics, r)
			for collector, count := range tt.expected {
				if len(result[collector]) != count {
					t.Errorf("GroupMetricsByCollector: collector %q 预期 %d 个指标，实际 %d 个",
						collector, count, len(result[collector]))
				}
			}
		})
	}
}

func TestGroupMetricsByCollector_WithGPURegistry(t *testing.T) {
	// 模拟包含 gpu 的注册中心
	jsonContent := `{
		"version": 1,
		"collectors": [
			{"name": "cpumem", "aliases": [{"alias": "cpu", "es_field": "a"}]},
			{"name": "gpu", "aliases": [{"alias": "gpu_util", "es_field": "b"}]}
		],
		"global_aliases": [{"alias": "host", "es_field": "c"}]
	}`

	// 使用临时文件加载
	dir := t.TempDir()
	path := dir + "/test-registry.json"
	if err := writeTestFile(path, jsonContent); err != nil {
		t.Fatalf("写入临时文件失败: %v", err)
	}

	r, err := model.LoadRegistry(path)
	if err != nil {
		t.Fatalf("加载注册中心失败: %v", err)
	}

	result := GroupMetricsByCollector([]string{"cpu", "gpu_util", "host"}, r)
	if len(result["cpumem"]) != 1 || result["cpumem"][0] != "cpu" {
		t.Error("cpu 应归属 cpumem")
	}
	if len(result["gpu"]) != 1 || result["gpu"][0] != "gpu_util" {
		t.Error("gpu_util 应归属 gpu")
	}
	if len(result[""]) != 1 || result[""][0] != "host" {
		t.Error("host 应为全局别名")
	}
}

// 辅助函数：写入测试文件
func writeTestFile(path, content string) error {
	return os.WriteFile(path, []byte(content), 0644)
}
