// Package config 管理应用配置，从环境变量读取
package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/joblens/tap/internal/model"
)

// Config 应用全局配置
type Config struct {
	Port         int           `env:"TAP_PORT" envDefault:"8080"`
	LogLevel     string        `env:"TAP_LOG_LEVEL" envDefault:"info"`
	ReadTimeout  time.Duration `env:"TAP_READ_TIMEOUT" envDefault:"30s"`
	WriteTimeout time.Duration `env:"TAP_WRITE_TIMEOUT" envDefault:"30s"`

	// 查询限制
	MaxSize          int    `env:"TAP_MAX_SIZE" envDefault:"10000"`
	DefaultSize      int    `env:"TAP_DEFAULT_SIZE" envDefault:"100"`
	MaxTimeRangeDays int    `env:"TAP_MAX_TIME_RANGE_DAYS" envDefault:"7"`
	DefaultInterval  string `env:"TAP_DEFAULT_INTERVAL" envDefault:"1m"`

	// 管理 API（集群元数据来源）
	ManagementAPIURL   string        `env:"TAP_MANAGEMENT_API_URL"`
	ManagementCacheTTL time.Duration `env:"TAP_MANAGEMENT_CACHE_TTL" envDefault:"5m"`

	// 采集器注册文件路径（优先级高于 TAP_DEFAULT_COLLECTORS）
	CollectorRegistryPath string `env:"TAP_COLLECTOR_REGISTRY_PATH"`

	// 默认采集器列表（已废弃，优先使用 Registry；仅在未设置 TAP_COLLECTOR_REGISTRY_PATH 时生效）
	DefaultCollectorsRaw string   `env:"TAP_DEFAULT_COLLECTORS"`
	DefaultCollectors    []string // 解析后

	// Skill API 基础 URL（用于 /skill 接口中填充文档访问地址）
	SkillAPIBaseURL string `env:"TAP_SKILL_API_BASE_URL"`

	// 采集器注册中心（线程安全，支持 SIGHUP 热重载）
	Registry *model.CollectorRegistry

	// 注册中心配置（采集触发用）
	ServiceRegistryURL     string        `env:"TAP_SERVICE_REGISTRY_URL"`
	ServiceRegistryTimeout time.Duration `env:"TAP_SERVICE_REGISTRY_TIMEOUT" envDefault:"5s"`

	// Agent 重试配置（采集触发用）
	AgentRetryInitialDelay time.Duration `env:"TAP_AGENT_RETRY_INITIAL_DELAY" envDefault:"500ms"`
	AgentRetryMaxAttempts  int           `env:"TAP_AGENT_RETRY_MAX_ATTEMPTS" envDefault:"3"`
	AgentRetryMultiplier   float64       `env:"TAP_AGENT_RETRY_MULTIPLIER" envDefault:"2.0"`
}

// Load 从环境变量加载配置
func Load() (*Config, error) {
	cfg := &Config{}
	if err := env.Parse(cfg); err != nil {
		return nil, fmt.Errorf("parse env config: %w", err)
	}

	// 初始化采集器注册中心
	if cfg.CollectorRegistryPath != "" {
		// 优先：从外部注册文件加载
		registry, err := model.LoadRegistry(cfg.CollectorRegistryPath)
		if err != nil {
			return nil, fmt.Errorf("加载采集器注册文件失败: %w", err)
		}
		cfg.Registry = registry
		cfg.DefaultCollectors = registry.GetCollectorNames()
	} else {
		// 回退：使用 TAP_DEFAULT_COLLECTORS 环境变量或内置默认值
		if cfg.DefaultCollectorsRaw == "" {
			cfg.DefaultCollectorsRaw = "cpumem,io,net"
		}
		for _, c := range strings.Split(cfg.DefaultCollectorsRaw, ",") {
			c = strings.TrimSpace(c)
			if c != "" {
				cfg.DefaultCollectors = append(cfg.DefaultCollectors, c)
			}
		}
		// 内置默认注册中心
		cfg.Registry = model.BuildDefaultRegistry()
	}

	// 设置全局默认注册中心（供 model 包向后兼容包装函数使用）
	model.SetDefaultRegistry(cfg.Registry)

	return cfg, nil
}

// ParseClusterFilter 解析 cluster 参数
// 支持两种格式: "cluster_name" 或 "cluster_name:cluster_tag"
func ParseClusterFilter(param string) (clusterName, clusterTag string) {
	if param == "" {
		return "", ""
	}
	idx := strings.LastIndex(param, ":")
	if idx == -1 {
		return param, ""
	}
	return param[:idx], param[idx+1:]
}
