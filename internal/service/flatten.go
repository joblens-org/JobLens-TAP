package service

import (
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/joblens/tap/internal/model"
	"github.com/joblens/tap/internal/repository"
)

// FlattenHit 将 ES 原始文档转换为扁平 Record
func FlattenHit(hit repository.SearchHit, clusterID string, flatten bool, _ []string) *model.Record {
	record := &model.Record{
		Cluster: clusterID,
		Fields:  make(map[string]any),
	}

	source := hit.Source
	if source == nil {
		return record
	}

	// 提取顶层标准字段
	if v, ok := source["hostname"].(string); ok {
		record.Host = v
	}
	if v, ok := source["@timestamp"].(string); ok {
		record.Time = v
	}
	if hit.Index != "" {
		record.Collector = extractCollectorFromIndex(hit.Index)
	}

	// 提取 job_info
	if jobInfo, ok := source["job_info"].(map[string]any); ok {
		// 优先使用 NativeJobID 直接匹配前端 job 参数
		if nativeID, ok := jobInfo["NativeJobID"].(string); ok {
			record.Job = nativeID
		} else if jobID, ok := jobInfo["JobID"]; ok {
			// 兼容旧数据（无 NativeJobID 时 fallback 到 JobID）
			switch id := jobID.(type) {
			case float64:
				record.Job = int64(id)
			case int64:
				record.Job = id
			case int:
				record.Job = int64(id)
			default:
				record.Job = jobID
			}
		}
	}

	if !flatten {
		if data, ok := source["data"].(map[string]any); ok {
			record.Data = data
		}
		return record
	}

	// 扁平化模式：展开 data 下所有嵌套字段
	if data, ok := source["data"].(map[string]any); ok {
		flattenNested("data", data, record.Fields)

		// 从 data.summary 提取快捷字段
		if summary, ok := data["summary"].(map[string]any); ok {
			if cpu, ok := summary["cpuPercent"].(float64); ok {
				record.CPU = &cpu
			}
			if mem, ok := summary["mem_rss_kb"]; ok {
				if m, ok := toInt64Val(mem); ok {
					record.Mem = &m
				}
			}
			if name, ok := summary["name"].(string); ok {
				record.Name = &name
			}
		}
		// 从 data.summary 提取 io_bytes（IO 索引）
		if _, hasCPU := record.Fields["data.summary.cpuPercent"]; !hasCPU {
			if summary, ok := data["summary"].(map[string]any); ok {
				if readBytes, ok := summary["read_bytes"]; ok {
					if m, ok := toInt64Val(readBytes); ok {
						record.IOBytes = &m
					}
				}
			}
		}
	}

	return record
}

// toInt64Val 将 any 转换为 int64
func toInt64Val(v any) (int64, bool) {
	switch val := v.(type) {
	case float64:
		return int64(val), true
	case int64:
		return val, true
	case int:
		return int64(val), true
	default:
		return 0, false
	}
}

// toInt64 将 any 转换为 int64
func toInt64(v any) (int64, bool) {
	return toInt64Val(v)
}

// extractCollectorFromIndex 从 ES 索引名中提取 collector 名称
// 索引格式: {collector}_collector_{date} 例如 cpumem_collector_2026.04.27
func extractCollectorFromIndex(index string) string {
	idx := strings.Index(index, "_collector_")
	if idx == -1 {
		return ""
	}
	prefix := index[:idx]
	parts := strings.SplitN(prefix, "_", 2)
	if len(parts) == 1 {
		// 新模式: {collector}_collector_{date}，prefix 直接就是 collector 名
		return parts[0]
	}
	if len(parts) == 2 {
		// 旧模式: {prefix}_{collector}_collector_{date}
		return parts[1]
	}
	return ""
}

// flattenNested 递归扁平化嵌套结构
// - map[string]any: 递归处理子键，使用 parent.child 前缀
// - []interface{}: 按索引递归处理，使用 prefix.index 格式
// - 基本类型: 直接设置到 fields map
func flattenNested(prefix string, value any, fields map[string]any) {
	switch v := value.(type) {
	case map[string]any:
		for k, childVal := range v {
			childKey := prefix + "." + k
			flattenNested(childKey, childVal, fields)
		}
	case []interface{}:
		for i, childVal := range v {
			childKey := fmt.Sprintf("%s.%d", prefix, i)
			flattenNested(childKey, childVal, fields)
		}
	default:
		fields[prefix] = value
	}
}

// FlattenHits 批量扁平化 ES 响应
func FlattenHits(hits []repository.SearchHit, clusterID string, flatten bool, fields []string) []model.Record {
	// LOG_REASON: 扁平化是数据处理的关键节点，记录命中数与期望值对比，便于发现数据截断或丢失
	slog.Debug("[FlattenHits] flattening hits",
		"cluster", clusterID,
		"hits_count", len(hits),
	)

	records := make([]model.Record, 0, len(hits))
	for _, hit := range hits {
		record := FlattenHit(hit, clusterID, flatten, fields)
		records = append(records, *record)
	}
	return records
}

// MergeResults 合并多集群结果（K-way merge 按时间降序排序）
func MergeResults(results []*model.RawQueryResponse, maxSize int) *model.RawQueryResponse {
	// LOG_REASON: 多集群合并是关键汇总节点，记录各集群返回条数和合并后总条数，便于排查数据不完整
	if len(results) > 1 {
		var clusterHits []int
		for i, r := range results {
			total := 0
			if r.Pagination != nil {
				total = r.Pagination.Total
			}
			clusterHits = append(clusterHits, len(r.Records))
			slog.Debug("[MergeResults] cluster result",
				"cluster_index", i,
				"hits", len(r.Records),
				"total", total,
			)
		}
		slog.Debug("[MergeResults] merging results",
			"cluster_count", len(results),
			"hits_per_cluster", clusterHits,
			"max_size", maxSize,
		)
	}

	if len(results) == 0 {
		return &model.RawQueryResponse{
			Records:    []model.Record{},
			Pagination: &model.Pagination{HasMore: false},
		}
	}

	if len(results) == 1 {
		return results[0]
	}

	// 收集所有记录和索引列表
	var allRecords []model.Record
	var allIndices []string
	var total int
	seenIdx := make(map[string]bool)
	for _, r := range results {
		allRecords = append(allRecords, r.Records...)
		if r.Pagination != nil {
			total += r.Pagination.Total
		}
		for _, idx := range r.IndicesResolved {
			if !seenIdx[idx] {
				seenIdx[idx] = true
				allIndices = append(allIndices, idx)
			}
		}
	}

	// 按时间降序排序
	sort.Slice(allRecords, func(i, j int) bool {
		return compareTime(allRecords[i].Time, allRecords[j].Time) > 0
	})

	// 限制返回数量
	hasMore := len(allRecords) > maxSize
	if len(allRecords) > maxSize {
		allRecords = allRecords[:maxSize]
	}

	// LOG_REASON: 记录合并后最终返回的记录数和是否还有更多数据，便于判断分页是否正确
	slog.Debug("[MergeResults] merge completed",
		"returned", len(allRecords),
		"total_all_clusters", total,
		"has_more", hasMore,
	)

	return &model.RawQueryResponse{
		Records:         allRecords,
		IndicesResolved: allIndices,
		Pagination: &model.Pagination{
			HasMore:  hasMore,
			Returned: len(allRecords),
			Total:    total,
		},
	}
}

// compareTime 比较两个时间字符串，返回 1 表示 a > b，-1 表示 a < b，0 表示相等
func compareTime(a, b string) int {
	// 解析时间
	tA, errA := time.Parse(time.RFC3339, a)
	tB, errB := time.Parse(time.RFC3339, b)

	if errA != nil && errB != nil {
		return strings.Compare(a, b)
	}
	if errA != nil {
		return -1
	}
	if errB != nil {
		return 1
	}

	if tA.After(tB) {
		return 1
	} else if tA.Before(tB) {
		return -1
	}
	return 0
}

// ExtractFields 从 ES 源数据中提取指定字段
func ExtractFields(source map[string]any, fields []string) map[string]any {
	result := make(map[string]any)
	for _, alias := range fields {
		if esField, ok := model.GetESField(alias); ok {
			value := getNestedValue(source, esField)
			if value != nil {
				result[alias] = value
			}
		}
	}
	return result
}

// getNestedValue 从嵌套 map 中获取指定路径的值
func getNestedValue(m map[string]any, path string) any {
	parts := strings.Split(path, ".")
	var current any = m

	for _, part := range parts {
		switch v := current.(type) {
		case map[string]any:
			var ok bool
			current, ok = v[part]
			if !ok {
				return nil
			}
		default:
			return nil
		}
	}

	return current
}
