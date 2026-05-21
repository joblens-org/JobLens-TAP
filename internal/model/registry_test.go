package model

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// 辅助函数：创建临时 JSON 注册文件
func writeTempRegistry(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "collector-registry.json")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("写入临时注册文件失败: %v", err)
	}
	return path
}

// 有效的注册文件 JSON
func validRegistryJSON() string {
	rf := RegistryFile{
		Version: 1,
		Collectors: []CollectorEntry{
			{
				Name:        "cpumem",
				Description: "CPU和内存",
				Aliases: []FieldAlias{
					{Alias: "cpu", ESField: "data.summary.cpuPercent", Type: "float"},
					{Alias: "mem", ESField: "data.summary.mem_rss_kb", Type: "long"},
				},
			},
			{
				Name:         "io",
				Description:  "IO采集器",
				IndexPattern: "io_collector_{date}",
				Aliases: []FieldAlias{
					{Alias: "io_bytes", ESField: "data.summary.read_bytes", Type: "long"},
				},
			},
			{Name: "net"},
		},
		GlobalAliases: []FieldAlias{
			{Alias: "host", ESField: "hostname.keyword", Type: "keyword"},
			{Alias: "time", ESField: "@timestamp", Type: "date"},
		},
	}
	b, _ := json.MarshalIndent(rf, "", "  ")
	return string(b)
}

// =============================================================================
// BuildDefaultRegistry 测试
// =============================================================================

func TestBuildDefaultRegistry_Collectors(t *testing.T) {
	r := BuildDefaultRegistry()

	// 内置默认采集器包含 cpumem, io, net，不含 gpu
	names := r.GetCollectorNames()
	nameSet := make(map[string]bool, len(names))
	for _, n := range names {
		nameSet[n] = true
	}

	for _, want := range []string{"cpumem", "io", "net"} {
		if !nameSet[want] {
			t.Errorf("默认注册中心缺少采集器 %s", want)
		}
	}
	if nameSet["gpu"] {
		t.Error("默认注册中心不应包含 gpu（gpu 仅在外载文件中定义）")
	}
}

func TestBuildDefaultRegistry_Aliases(t *testing.T) {
	r := BuildDefaultRegistry()

	// 内置别名能否正确解析
	cases := []struct {
		alias   string
		wantES  string
		wantOK  bool
	}{
		{"cpu", "data.summary.cpuPercent", true},
		{"mem", "data.summary.mem_rss_kb", true},
		{"mem_peak", "data.summary.mem_peak_rss_kb", true},
		{"name", "data.summary.name.keyword", true},
		{"io_bytes", "data.summary.read_bytes", true},
		{"host", "hostname.keyword", true},
		{"time", "@timestamp", true},
		{"nonexistent", "", false},
	}

	for _, tc := range cases {
		esField, ok := r.GetESField(tc.alias)
		if ok != tc.wantOK {
			t.Errorf("GetESField(%q) ok=%v, want %v", tc.alias, ok, tc.wantOK)
		}
		if ok && esField != tc.wantES {
			t.Errorf("GetESField(%q) = %q, want %q", tc.alias, esField, tc.wantES)
		}
	}
}

func TestBuildDefaultRegistry_InferCollector(t *testing.T) {
	r := BuildDefaultRegistry()

	cases := []struct {
		metric string
		want   string
	}{
		{"cpu", "cpumem"},
		{"mem", "cpumem"},
		{"io_bytes", "io"},
		{"host", ""},  // 全局别名不属于任何采集器
		{"time", ""},  // 全局别名不属于任何采集器
		{"unknown", ""},
	}

	for _, tc := range cases {
		got := r.InferCollectorFromMetric(tc.metric)
		if got != tc.want {
			t.Errorf("InferCollectorFromMetric(%q) = %q, want %q", tc.metric, got, tc.want)
		}
	}
}

func TestBuildDefaultRegistry_AllAliases(t *testing.T) {
	r := BuildDefaultRegistry()

	aliases := r.AllAliases()
	if len(aliases) < 7 {
		t.Errorf("AllAliases 返回 %d 个别名，预期至少 7 个", len(aliases))
	}

	// 验证不重复
	seen := make(map[string]bool)
	for _, a := range aliases {
		if seen[a] {
			t.Errorf("别名 %s 重复", a)
		}
		seen[a] = true
	}
}

func TestBuildDefaultRegistry_IndexPattern(t *testing.T) {
	r := BuildDefaultRegistry()

	// 内置采集器使用默认模式
	pat := r.GetIndexPattern("cpumem")
	if pat != defaultIndexPattern {
		t.Errorf("cpumem index_pattern = %q, want %q", pat, defaultIndexPattern)
	}

	// 未注册的采集器也返回默认模式
	pat = r.GetIndexPattern("nonexistent")
	if pat != defaultIndexPattern {
		t.Errorf("未知采集器 index_pattern = %q, want %q", pat, defaultIndexPattern)
	}
}

// =============================================================================
// LoadRegistry 测试
// =============================================================================

func TestLoadRegistry_Success(t *testing.T) {
	path := writeTempRegistry(t, validRegistryJSON())
	r, err := LoadRegistry(path)
	if err != nil {
		t.Fatalf("LoadRegistry 失败: %v", err)
	}

	// 验证采集器
	names := r.GetCollectorNames()
	if len(names) != 3 {
		t.Errorf("采集器数量 = %d, want 3", len(names))
	}

	// 验证别名
	if _, ok := r.GetESField("cpu"); !ok {
		t.Error("缺少 cpu 别名")
	}
	if _, ok := r.GetESField("host"); !ok {
		t.Error("缺少全局 host 别名")
	}
}

func TestLoadRegistry_FileNotFound(t *testing.T) {
	_, err := LoadRegistry("/nonexistent/path/collector-registry.json")
	if err == nil {
		t.Error("应返回错误")
	}
}

func TestLoadRegistry_InvalidJSON(t *testing.T) {
	path := writeTempRegistry(t, "这不是json{{{")
	_, err := LoadRegistry(path)
	if err == nil {
		t.Error("应返回 JSON 解析错误")
	}
}

func TestLoadRegistry_EmptyCollectors(t *testing.T) {
	path := writeTempRegistry(t, `{"version": 1, "collectors": [], "global_aliases": []}`)
	_, err := LoadRegistry(path)
	if err == nil {
		t.Error("应返回 '未定义任何采集器' 错误")
	}
}

func TestLoadRegistry_UnsupportedVersion(t *testing.T) {
	path := writeTempRegistry(t, `{"version": 99, "collectors": [{"name": "test"}], "global_aliases": []}`)
	_, err := LoadRegistry(path)
	if err == nil {
		t.Error("应返回版本不支持错误")
	}
}

func TestLoadRegistry_DuplicateCollector(t *testing.T) {
	jsonContent := `{
		"version": 1,
		"collectors": [
			{"name": "cpumem"},
			{"name": "cpumem"}
		],
		"global_aliases": []
	}`
	path := writeTempRegistry(t, jsonContent)
	_, err := LoadRegistry(path)
	if err == nil {
		t.Error("应返回采集器重复错误")
	}
}

func TestLoadRegistry_EmptyCollectorName(t *testing.T) {
	jsonContent := `{
		"version": 1,
		"collectors": [
			{"name": ""}
		],
		"global_aliases": []
	}`
	path := writeTempRegistry(t, jsonContent)
	_, err := LoadRegistry(path)
	if err == nil {
		t.Error("应返回名称为空错误")
	}
}

func TestLoadRegistry_DuplicateAlias(t *testing.T) {
	jsonContent := `{
		"version": 1,
		"collectors": [
			{"name": "cpumem", "aliases": [{"alias": "cpu", "es_field": "a"}]},
			{"name": "gpu",    "aliases": [{"alias": "cpu", "es_field": "b"}]}
		],
		"global_aliases": []
	}`
	path := writeTempRegistry(t, jsonContent)
	_, err := LoadRegistry(path)
	if err == nil {
		t.Error("应返回别名重复错误")
	}
}

func TestLoadRegistry_DuplicateGlobalAlias(t *testing.T) {
	jsonContent := `{
		"version": 1,
		"collectors": [
			{"name": "cpumem", "aliases": [{"alias": "cpu", "es_field": "a"}]}
		],
		"global_aliases": [
			{"alias": "cpu", "es_field": "b"}
		]
	}`
	path := writeTempRegistry(t, jsonContent)
	_, err := LoadRegistry(path)
	if err == nil {
		t.Error("应返回全局别名重复错误")
	}
}

func TestLoadRegistry_CustomIndexPattern(t *testing.T) {
	jsonContent := `{
		"version": 1,
		"collectors": [
			{"name": "gpu", "index_pattern": "gpu_metrics_{date}"}
		],
		"global_aliases": []
	}`
	path := writeTempRegistry(t, jsonContent)
	r, err := LoadRegistry(path)
	if err != nil {
		t.Fatalf("加载失败: %v", err)
	}

	pat := r.GetIndexPattern("gpu")
	if pat != "gpu_metrics_{date}" {
		t.Errorf("index_pattern = %q, want %q", pat, "gpu_metrics_{date}")
	}

	// 验证 RenderIndexName
	index := r.RenderIndexName("gpu", "2026.05.12")
	if index != "gpu_metrics_2026.05.12" {
		t.Errorf("RenderIndexName = %q, want %q", index, "gpu_metrics_2026.05.12")
	}
}

func TestLoadRegistry_IndexPatternDefault(t *testing.T) {
	path := writeTempRegistry(t, validRegistryJSON())
	r, _ := LoadRegistry(path)

	// io 定义了自定义 index_pattern
	if r.GetIndexPattern("io") == defaultIndexPattern {
		t.Error("io 应有自定义 index_pattern")
	}

	// net 未定义 → 使用默认
	if r.GetIndexPattern("net") != defaultIndexPattern {
		t.Errorf("net 应用默认 index_pattern, got %q", r.GetIndexPattern("net"))
	}
}

// =============================================================================
// GetAliasesByCollector 测试
// =============================================================================

func TestGetAliasesByCollector(t *testing.T) {
	r := BuildDefaultRegistry()

	// cpumem 采集器别名
	cpumemAliases := r.GetAliasesByCollector("cpumem")
	if len(cpumemAliases) < 3 {
		t.Errorf("cpumem 别名数 = %d, want >= 3", len(cpumemAliases))
	}
	for _, want := range []string{"cpu", "mem", "mem_peak", "name"} {
		found := false
		for _, a := range cpumemAliases {
			if a == want {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("cpumem 缺少别名 %s", want)
		}
	}

	// 空字符串 → 全局别名
	globalAliases := r.GetAliasesByCollector("")
	for _, want := range []string{"host", "time"} {
		found := false
		for _, a := range globalAliases {
			if a == want {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("全局别名缺少 %s", want)
		}
	}
}

// =============================================================================
// RenderIndexName 测试
// =============================================================================

func TestRenderIndexName(t *testing.T) {
	r := BuildDefaultRegistry()

	cases := []struct {
		collector string
		date      string
		want      string
	}{
		{"cpumem", "2026.05.12", "cpumem_collector_2026.05.12"},
		{"io", "2026.01.01", "io_collector_2026.01.01"},
		{"gpu", "2026.04.02", "gpu_collector_2026.04.02"},
		{"net", "2025.12.31", "net_collector_2025.12.31"},
	}

	for _, tc := range cases {
		got := r.RenderIndexName(tc.collector, tc.date)
		if got != tc.want {
			t.Errorf("RenderIndexName(%q, %q) = %q, want %q", tc.collector, tc.date, got, tc.want)
		}
	}
}

func TestRenderIndexName_CustomPattern(t *testing.T) {
	r := BuildDefaultRegistry()

	// 模拟自定义模式（通过手动注册一个带自定义pattern的collector）
	// BuildDefaultRegistry 只有默认模式，这里测试函数本身
	pat := r.GetIndexPattern("cpumem")
	if pat != defaultIndexPattern {
		t.Fatalf("预期默认模式")
	}

	index := r.RenderIndexName("cpumem", "2026.05.12")
	if index != "cpumem_collector_2026.05.12" {
		t.Errorf("默认渲染失败: %q", index)
	}
}

func TestRenderIndexName_Wildcard(t *testing.T) {
	r := BuildDefaultRegistry()

	// 通配符渲染（CheckJobExists 使用场景）
	index := r.RenderIndexName("cpumem", "*")
	if index != "cpumem_collector_*" {
		t.Errorf("通配符渲染 = %q, want %q", index, "cpumem_collector_*")
	}
}

// =============================================================================
// Reload 热重载测试
// =============================================================================

func TestReload_Success(t *testing.T) {
	path := writeTempRegistry(t, validRegistryJSON())
	r, err := LoadRegistry(path)
	if err != nil {
		t.Fatalf("初始加载失败: %v", err)
	}

	// 初始状态：3 个采集器
	if len(r.GetCollectorNames()) != 3 {
		t.Fatalf("初始采集器数 = %d, want 3", len(r.GetCollectorNames()))
	}

	// 覆盖文件，添加 gpu 采集器
	newContent := `{
		"version": 1,
		"collectors": [
			{"name": "cpumem", "aliases": [{"alias": "cpu", "es_field": "data.summary.cpuPercent", "type": "float"}]},
			{"name": "io"},
			{"name": "net"},
			{"name": "gpu", "aliases": [{"alias": "gpu_util", "es_field": "data.summary.gpu_util", "type": "float"}]}
		],
		"global_aliases": []
	}`
	if err := os.WriteFile(path, []byte(newContent), 0644); err != nil {
		t.Fatalf("覆盖文件失败: %v", err)
	}

	// 热重载
	if err := r.Reload(); err != nil {
		t.Fatalf("Reload 失败: %v", err)
	}

	// 验证新数据
	names := r.GetCollectorNames()
	if len(names) != 4 {
		t.Errorf("重载后采集器数 = %d, want 4", len(names))
	}

	// 新别名应可查询
	esField, ok := r.GetESField("gpu_util")
	if !ok {
		t.Error("重载后缺少 gpu_util 别名")
	}
	if esField != "data.summary.gpu_util" {
		t.Errorf("gpu_util ES字段 = %q", esField)
	}

	// 旧别名不应丢失（验证替换非追加）
	namesSet := make(map[string]bool)
	for _, n := range names {
		namesSet[n] = true
	}
	if !namesSet["cpumem"] || !namesSet["io"] || !namesSet["net"] || !namesSet["gpu"] {
		t.Error("重载后采集器集合不正确")
	}
}

func TestReload_InvalidFile_KeepsOldData(t *testing.T) {
	path := writeTempRegistry(t, validRegistryJSON())
	r, err := LoadRegistry(path)
	if err != nil {
		t.Fatalf("初始加载失败: %v", err)
	}

	oldCount := len(r.GetCollectorNames())

	// 写入无效 JSON
	if err := os.WriteFile(path, []byte("broken{{{"), 0644); err != nil {
		t.Fatalf("覆盖文件失败: %v", err)
	}

	err = r.Reload()
	if err == nil {
		t.Error("Reload 应返回错误")
	}

	// 旧数据应保留
	newCount := len(r.GetCollectorNames())
	if newCount != oldCount {
		t.Errorf("重载失败后采集器数从 %d 变为 %d，旧数据应保留", oldCount, newCount)
	}
}

func TestReload_NoPath(t *testing.T) {
	r := BuildDefaultRegistry() // 没有设置 path
	err := r.Reload()
	if err == nil {
		t.Error("未设置路径时 Reload 应返回错误")
	}
}

// =============================================================================
// SetDefaultRegistry + 向后兼容包装函数测试
// =============================================================================

func TestSetDefaultRegistry(t *testing.T) {
	r := BuildDefaultRegistry()
	SetDefaultRegistry(r)

	// 通过旧 API GetESField 查询
	esField, ok := GetESField("cpu")
	if !ok {
		t.Error("GetESField(cpu) 应返回 true")
	}
	if esField != "data.summary.cpuPercent" {
		t.Errorf("GetESField(cpu) = %q", esField)
	}

	// 不存在的别名
	_, ok = GetESField("nonexistent")
	if ok {
		t.Error("GetESField(nonexistent) 应返回 false")
	}
}

func TestGetAliasesByCollector_BackwardCompat(t *testing.T) {
	r := BuildDefaultRegistry()
	SetDefaultRegistry(r)

	aliases := GetAliasesByCollector("cpumem")
	if len(aliases) < 3 {
		t.Errorf("GetAliasesByCollector(cpumem) = %d, want >= 3", len(aliases))
	}
}

func TestAllAliases_BackwardCompat(t *testing.T) {
	r := BuildDefaultRegistry()
	SetDefaultRegistry(r)

	aliases := AllAliases()
	if len(aliases) < 7 {
		t.Errorf("AllAliases = %d, want >= 7", len(aliases))
	}
}

func TestGetESField_NoDefaultRegistry(t *testing.T) {
	// 重置默认注册中心
	old := defaultRegistry
	defaultRegistry = nil
	defer func() { defaultRegistry = old }()

	_, ok := GetESField("cpu")
	if ok {
		t.Error("未设置默认注册中心时 GetESField 应返回 false")
	}
}

// =============================================================================
// 并发安全测试
// =============================================================================

func TestRegistryConcurrency(t *testing.T) {
	r := BuildDefaultRegistry()

	done := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			r.GetESField("cpu")
			r.GetCollectorNames()
			r.GetAliasesByCollector("cpumem")
			r.AllAliases()
			r.InferCollectorFromMetric("cpu")
			r.GetIndexPattern("cpumem")
		}
		close(done)
	}()

	for i := 0; i < 1000; i++ {
		r.GetESField("mem")
		r.GetCollectorNames()
		r.RenderIndexName("cpumem", "2026.05.12")
	}

	<-done
	// 无 race 即通过
}
