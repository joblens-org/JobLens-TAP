package model

// ESFieldAlias 字段别名映射（已废弃，使用 CollectorRegistry）
// 保留此类型仅为向后兼容
type ESFieldAlias struct {
	Alias       string
	ESField     string
	Type        string
	Collector   string
	Description string
}

// defaultRegistry 全局默认注册中心（由 config.Load() 在启动时设置）
var defaultRegistry *CollectorRegistry

// SetDefaultRegistry 设置全局默认注册中心
func SetDefaultRegistry(r *CollectorRegistry) {
	defaultRegistry = r
}

// FieldAliasMap 全局字段别名映射表（已废弃，使用 CollectorRegistry）
// 保留此字段仅为向后兼容，不再维护
var FieldAliasMap map[string]ESFieldAlias

// GetESField 根据别名获取 ES 字段路径（委托给默认注册中心）
func GetESField(alias string) (string, bool) {
	if defaultRegistry != nil {
		return defaultRegistry.GetESField(alias)
	}
	return "", false
}

// GetAliasesByCollector 获取指定采集器的所有别名（委托给默认注册中心）
func GetAliasesByCollector(collector string) []string {
	if defaultRegistry != nil {
		return defaultRegistry.GetAliasesByCollector(collector)
	}
	return nil
}

// AllAliases 获取所有别名列表（委托给默认注册中心）
func AllAliases() []string {
	if defaultRegistry != nil {
		return defaultRegistry.AllAliases()
	}
	return nil
}
