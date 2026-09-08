package model

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
)

// RegistryFile JSON注册文件顶层结构
type RegistryFile struct {
	Version       int              `json:"version"`
	Collectors    []CollectorEntry `json:"collectors"`
	GlobalAliases []FieldAlias     `json:"global_aliases"`
}

// CollectorEntry 单个采集器定义
type CollectorEntry struct {
	Name         string       `json:"name"`                   // 采集器名（如 "gpu"）
	Description  string       `json:"description,omitempty"`  // 描述
	IndexPattern string       `json:"index_pattern,omitempty"` // 索引模式，默认 "{collector}_collector_{date}"
	AgentName    string       `json:"agent_name,omitempty"`   // 节点侧采集器实例名（/collect 的 Lens 传递），默认等于 name
	Aliases      []FieldAlias `json:"aliases,omitempty"`      // 该采集器专属别名
}

// FieldAlias 字段别名
type FieldAlias struct {
	Alias       string `json:"alias"`                 // 别名（如 "cpu"）
	ESField     string `json:"es_field"`              // ES字段路径（如 "data.summary.cpuPercent"）
	Type        string `json:"type"`                  // 数据类型（如 "float"）
	Description string `json:"description,omitempty"` // 描述
	// SummaryAgg 可选：声明该别名参与 /data/summary 聚合
	// "extended_stats" → stats[alias] = {max, avg, p99}
	// "sum"/"max"/"min"/"avg" → stats[alias] = {value}
	SummaryAgg string `json:"summary_agg,omitempty"`
	// RecordField 可选：声明该别名提取为 /data/raw Record 顶层快捷字段
	// 合法值: "cpu"/"mem"/"name"/"io_bytes"（对应 Record 结构体字段）
	RecordField string `json:"record_field,omitempty"`
	// NestedPath 可选：ES nested mapping 路径（如 "data.files"）
	// 声明后时序聚合会包裹 nested aggregation，要求 ES mapping 中该路径为 nested 类型
	NestedPath string `json:"nested_path,omitempty"`
}

// 索引模式占位符
const (
	collectorPlaceholder = "{collector}"
	datePlaceholder      = "{date}"
	defaultIndexPattern  = "{collector}_collector_{date}"
)

// CollectorRegistry 采集器注册中心（线程安全、支持SIGHUP热重载）
type CollectorRegistry struct {
	mu         sync.RWMutex
	path       string // 注册文件路径（用于热重载）
	collectors map[string]*CollectorEntry
	// 别名索引，合并 collector aliases + global aliases
	aliasMap map[string]FieldAlias
	// 别名 → collector 名映射（用于推断采集器）
	aliasCollector map[string]string
	// 声明了 summary_agg 的别名（按注册顺序，供 Summary 查询驱动）
	summaryAliases []FieldAlias
	// 声明了 record_field 的别名（供 Record 快捷字段提取驱动）
	recordAliases []FieldAlias
}

// staticDefault 无注册文件时使用的内置默认配置
func staticDefault() *CollectorRegistry {
	r := &CollectorRegistry{
		collectors:     make(map[string]*CollectorEntry),
		aliasMap:       make(map[string]FieldAlias),
		aliasCollector: make(map[string]string),
	}

	// 内置采集器
	entries := []struct {
		name      string
		agentName string
		aliases   []FieldAlias
	}{
		{
			name:      "cpumem",
			agentName: "cpumem_collector",
			aliases: []FieldAlias{
				{Alias: "cpu", ESField: "data.summary.cpuPercent", Type: "float", SummaryAgg: "extended_stats", RecordField: "cpu"},
				{Alias: "mem", ESField: "data.summary.mem_rss_kb", Type: "long", SummaryAgg: "extended_stats", RecordField: "mem"},
				{Alias: "mem_peak", ESField: "data.summary.mem_peak_rss_kb", Type: "long"},
				{Alias: "name", ESField: "data.summary.name.keyword", Type: "keyword", RecordField: "name"},
			},
		},
		{
			// 旧版 IO 采集器，仅用于历史数据查询（新作业请使用 new_io_usage）
			name:      "io",
			agentName: "io_collector",
			aliases: []FieldAlias{
				{Alias: "io_legacy", ESField: "data.summary.read_bytes", Type: "long", Description: "旧版io累计读字节（历史数据）"},
			},
		},
		{
			name:      "new_io_usage",
			agentName: "new_io_usage_collector",
			aliases: []FieldAlias{
				{Alias: "io_bytes", ESField: "data.job_total.rchar", Type: "long", SummaryAgg: "sum", RecordField: "io_bytes", Description: "job累计读字节"},
				{Alias: "io_write_bytes", ESField: "data.job_total.wchar", Type: "long", SummaryAgg: "sum"},
				{Alias: "io_read_speed", ESField: "data.job_total.rchar_speed", Type: "float"},
				{Alias: "io_write_speed", ESField: "data.job_total.wchar_speed", Type: "float"},
				{Alias: "io_read_ops", ESField: "data.job_total.syscr", Type: "long"},
				{Alias: "io_write_ops", ESField: "data.job_total.syscw", Type: "long"},
				{Alias: "file_rchar", ESField: "data.files.total.rchar", Type: "long", NestedPath: "data.files"},
				{Alias: "proc_rchar", ESField: "data.processes.rchar", Type: "long", NestedPath: "data.processes"},
			},
		},
		{
			name:      "fs_metadata",
			agentName: "fs_metadata_collector",
			aliases: []FieldAlias{
				{Alias: "metadata_ops", ESField: "data.job_metadata_ops_rate", Type: "float", SummaryAgg: "max"},
				{Alias: "metadata_ops_total", ESField: "data.job_metadata_ops_total", Type: "long", SummaryAgg: "sum"},
			},
		},
		{
			name:      "net",
			agentName: "net_collector",
		},
	}

	for _, e := range entries {
		ce := &CollectorEntry{
			Name:         e.name,
			AgentName:    e.agentName,
			IndexPattern: defaultIndexPattern,
		}
		r.collectors[e.name] = ce
		for _, fa := range e.aliases {
			fa.Description = fa.Alias
			r.aliasMap[fa.Alias] = fa
			r.aliasCollector[fa.Alias] = e.name
			if fa.SummaryAgg != "" {
				r.summaryAliases = append(r.summaryAliases, fa)
			}
			if fa.RecordField != "" {
				r.recordAliases = append(r.recordAliases, fa)
			}
		}
	}

	// 全局别名
	global := []FieldAlias{
		{Alias: "host", ESField: "hostname.keyword", Type: "keyword", Description: "host"},
		{Alias: "time", ESField: "@timestamp", Type: "date", Description: "time"},
	}
	for _, fa := range global {
		r.aliasMap[fa.Alias] = fa
		r.aliasCollector[fa.Alias] = ""
	}

	return r
}

// LoadRegistry 从JSON文件加载注册信息，失败返回error
func LoadRegistry(path string) (*CollectorRegistry, error) {
	slog.Info("loading collector registry", "path", path)

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("读取注册文件失败 %s: %w", path, err)
	}

	var file RegistryFile
	if err := json.Unmarshal(data, &file); err != nil {
		return nil, fmt.Errorf("解析注册文件JSON失败 %s: %w", path, err)
	}

	if file.Version != 1 {
		return nil, fmt.Errorf("注册文件版本 %d 不支持（仅支持版本1）", file.Version)
	}

	if len(file.Collectors) == 0 {
		return nil, fmt.Errorf("注册文件中未定义任何采集器")
	}

	r := &CollectorRegistry{
		path:           path,
		collectors:     make(map[string]*CollectorEntry, len(file.Collectors)),
		aliasMap:       make(map[string]FieldAlias),
		aliasCollector: make(map[string]string),
	}

	// 解析采集器
	for _, entry := range file.Collectors {
		name := strings.TrimSpace(entry.Name)
		if name == "" {
			return nil, fmt.Errorf("采集器名称为空")
		}
		if _, exists := r.collectors[name]; exists {
			return nil, fmt.Errorf("采集器重复定义: %s", name)
		}

		pattern := strings.TrimSpace(entry.IndexPattern)
		if pattern == "" {
			pattern = defaultIndexPattern
		}

		agentName := strings.TrimSpace(entry.AgentName)
		if agentName == "" {
			agentName = name
		}

		ce := &CollectorEntry{
			Name:         name,
			Description:  entry.Description,
			IndexPattern: pattern,
			AgentName:    agentName,
			Aliases:      entry.Aliases,
		}
		r.collectors[name] = ce

		// 解析采集器专属别名
		for _, fa := range entry.Aliases {
			if fa.Alias == "" {
				continue
			}
			if _, exists := r.aliasMap[fa.Alias]; exists {
				return nil, fmt.Errorf("别名重复定义: %s (采集器 %s)", fa.Alias, name)
			}
			if fa.SummaryAgg != "" {
				if err := validateSummaryAgg(fa.SummaryAgg); err != nil {
					return nil, fmt.Errorf("别名 %s (采集器 %s): %w", fa.Alias, name, err)
				}
				r.summaryAliases = append(r.summaryAliases, fa)
			}
			if fa.RecordField != "" {
				if err := validateRecordField(fa.RecordField); err != nil {
					return nil, fmt.Errorf("别名 %s (采集器 %s): %w", fa.Alias, name, err)
				}
				r.recordAliases = append(r.recordAliases, fa)
			}
			r.aliasMap[fa.Alias] = fa
			r.aliasCollector[fa.Alias] = name
		}
	}

	// 解析全局别名
	for _, fa := range file.GlobalAliases {
		if fa.Alias == "" {
			continue
		}
		if _, exists := r.aliasMap[fa.Alias]; exists {
			return nil, fmt.Errorf("全局别名重复定义: %s", fa.Alias)
		}
		r.aliasMap[fa.Alias] = fa
		r.aliasCollector[fa.Alias] = ""
	}

	slog.Info("collector registry loaded",
		"path", path,
		"collectors", len(r.collectors),
		"aliases", len(r.aliasMap),
	)

	return r, nil
}

// BuildDefaultRegistry 构建内置默认注册中心（无外部文件时使用）
func BuildDefaultRegistry() *CollectorRegistry {
	return staticDefault()
}

// Reload 热重载注册文件（SIGHUP触发），原子替换内部数据
func (r *CollectorRegistry) Reload() error {
	if r.path == "" {
		return fmt.Errorf("注册文件路径未设置，无法热重载")
	}

	newR, err := LoadRegistry(r.path)
	if err != nil {
		return err
	}

	r.mu.Lock()
	r.collectors = newR.collectors
	r.aliasMap = newR.aliasMap
	r.aliasCollector = newR.aliasCollector
	r.summaryAliases = newR.summaryAliases
	r.recordAliases = newR.recordAliases
	// path 不变
	r.mu.Unlock()

	slog.Info("collector registry hot-reloaded", "collectors", len(r.collectors), "aliases", len(r.aliasMap))
	return nil
}

// GetESField 根据别名获取ES字段路径
func (r *CollectorRegistry) GetESField(alias string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	fa, ok := r.aliasMap[alias]
	if !ok {
		return "", false
	}
	return fa.ESField, true
}

// GetAliasDef 根据别名获取完整定义（含 summary_agg/record_field/nested_path）
func (r *CollectorRegistry) GetAliasDef(alias string) (FieldAlias, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	fa, ok := r.aliasMap[alias]
	if !ok {
		return FieldAlias{}, false
	}
	return fa, true
}

// SummaryAliases 返回声明了 summary_agg 的别名（按注册顺序）
func (r *CollectorRegistry) SummaryAliases() []FieldAlias {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return append([]FieldAlias(nil), r.summaryAliases...)
}

// RecordAliases 返回声明了 record_field 的别名（按注册顺序）
func (r *CollectorRegistry) RecordAliases() []FieldAlias {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return append([]FieldAlias(nil), r.recordAliases...)
}

// GetAgentName 获取采集器的节点侧实例名（未注册或未声明时返回 collector 本身）
func (r *CollectorRegistry) GetAgentName(collector string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ce, ok := r.collectors[collector]
	if !ok || ce.AgentName == "" {
		return collector
	}
	return ce.AgentName
}

// HasCollector 判断采集器是否已注册
func (r *CollectorRegistry) HasCollector(collector string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.collectors[collector]
	return ok
}

// validateSummaryAgg 校验 summary_agg 取值
func validateSummaryAgg(agg string) error {
	switch agg {
	case "extended_stats", "sum", "max", "min", "avg":
		return nil
	default:
		return fmt.Errorf("summary_agg %q 不支持（可选 extended_stats/sum/max/min/avg）", agg)
	}
}

// validateRecordField 校验 record_field 取值（对应 Record 结构体固定字段）
func validateRecordField(field string) error {
	switch field {
	case "cpu", "mem", "name", "io_bytes":
		return nil
	default:
		return fmt.Errorf("record_field %q 不支持（可选 cpu/mem/name/io_bytes）", field)
	}
}

// GetCollectorNames 获取所有采集器名称列表
func (r *CollectorRegistry) GetCollectorNames() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.collectors))
	for name := range r.collectors {
		names = append(names, name)
	}
	return names
}

// GetAliasesByCollector 获取指定采集器的所有别名
// collector 为空时返回全局别名
func (r *CollectorRegistry) GetAliasesByCollector(collector string) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var aliases []string
	for alias, c := range r.aliasCollector {
		if collector == "" && c == "" {
			aliases = append(aliases, alias)
		} else if c == collector {
			aliases = append(aliases, alias)
		}
	}
	return aliases
}

// AllAliases 获取所有别名列表
func (r *CollectorRegistry) AllAliases() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	aliases := make([]string, 0, len(r.aliasMap))
	for alias := range r.aliasMap {
		aliases = append(aliases, alias)
	}
	return aliases
}

// InferCollectorFromMetric 根据指标别名推断所属采集器
func (r *CollectorRegistry) InferCollectorFromMetric(metric string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.aliasCollector[metric]
}

// GetAllCollectors 返回所有采集器完整信息（线程安全）
func (r *CollectorRegistry) GetAllCollectors() []CollectorEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]CollectorEntry, 0, len(r.collectors))
	for _, ce := range r.collectors {
		result = append(result, *ce)
	}
	return result
}

// GetGlobalAliases 返回全局别名列表（线程安全）
func (r *CollectorRegistry) GetGlobalAliases() []FieldAlias {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var result []FieldAlias
	for alias := range r.aliasMap {
		if r.aliasCollector[alias] == "" {
			result = append(result, r.aliasMap[alias])
		}
	}
	return result
}

// GetIndexPattern 获取采集器的索引模式（默认 "{collector}_collector_{date}"）
func (r *CollectorRegistry) GetIndexPattern(collector string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ce, ok := r.collectors[collector]
	if !ok || ce.IndexPattern == "" {
		return defaultIndexPattern
	}
	return ce.IndexPattern
}

// RenderIndexName 根据采集器和日期渲染索引名称
// dateStr 格式: "2006.01.02"
func (r *CollectorRegistry) RenderIndexName(collector, dateStr string) string {
	pattern := r.GetIndexPattern(collector)
	s := strings.ReplaceAll(pattern, collectorPlaceholder, collector)
	s = strings.ReplaceAll(s, datePlaceholder, dateStr)
	return s
}
