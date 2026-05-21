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
	Name         string       `json:"name"`          // 采集器名（如 "gpu"）
	Description  string       `json:"description"`   // 描述
	IndexPattern string       `json:"index_pattern"` // 索引模式，默认 "{collector}_collector_{date}"
	Aliases      []FieldAlias `json:"aliases"`       // 该采集器专属别名
}

// FieldAlias 字段别名
type FieldAlias struct {
	Alias       string `json:"alias"`       // 别名（如 "cpu"）
	ESField     string `json:"es_field"`    // ES字段路径（如 "data.summary.cpuPercent"）
	Type        string `json:"type"`        // 数据类型（如 "float"）
	Description string `json:"description"` // 描述
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
		name    string
		aliases []FieldAlias
	}{
		{
			name: "cpumem",
			aliases: []FieldAlias{
				{Alias: "cpu", ESField: "data.summary.cpuPercent", Type: "float"},
				{Alias: "mem", ESField: "data.summary.mem_rss_kb", Type: "long"},
				{Alias: "mem_peak", ESField: "data.summary.mem_peak_rss_kb", Type: "long"},
				{Alias: "name", ESField: "data.summary.name.keyword", Type: "keyword"},
			},
		},
		{
			name: "io",
			aliases: []FieldAlias{
				{Alias: "io_bytes", ESField: "data.summary.read_bytes", Type: "long"},
			},
		},
		{name: "net"},
	}

	for _, e := range entries {
		ce := &CollectorEntry{
			Name:         e.name,
			IndexPattern: defaultIndexPattern,
		}
		r.collectors[e.name] = ce
		for _, fa := range e.aliases {
			fa.Description = fa.Alias
			r.aliasMap[fa.Alias] = fa
			r.aliasCollector[fa.Alias] = e.name
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

		ce := &CollectorEntry{
			Name:         name,
			Description:  entry.Description,
			IndexPattern: pattern,
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
