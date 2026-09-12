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

	// 查询资源护栏（Phase 0）
	QueryTimeout         time.Duration `env:"TAP_QUERY_TIMEOUT" envDefault:"60s"`         // 单次 ES 查询超时
	MaxESBuckets         int           `env:"TAP_MAX_ES_BUCKETS" envDefault:"65536"`      // 单次查询桶数硬上限（对齐 ES search.max_buckets）
	MaxMetrics           int           `env:"TAP_MAX_METRICS" envDefault:"20"`            // 单次 metric 数上限
	MaxConcurrentStreams int           `env:"TAP_MAX_CONCURRENT_STREAMS" envDefault:"16"` // 并发流式上限
	MaxFlattenFields     int           `env:"TAP_MAX_FLATTEN_FIELDS" envDefault:"2000"`   // 单条记录扁平化最大键数

	// 流式查询（Phase 1/2）
	StreamPageSize      int           `env:"TAP_STREAM_PAGE_SIZE" envDefault:"500"`       // raw 流单页大小
	StreamWindowBuckets int           `env:"TAP_STREAM_WINDOW_BUCKETS" envDefault:"5000"` // 时序流单窗最大桶数
	StreamMaxRecords    int           `env:"TAP_STREAM_MAX_RECORDS" envDefault:"100000"`  // 流式总量软上限
	StreamMaxWindows    int           `env:"TAP_STREAM_MAX_WINDOWS" envDefault:"2000"`    // 时序流最大窗口数
	StreamTimeout       time.Duration `env:"TAP_STREAM_TIMEOUT" envDefault:"10m"`         // 单个流的整体超时
	SSEHeartbeat        time.Duration `env:"TAP_SSE_HEARTBEAT" envDefault:"15s"`          // SSE 心跳间隔

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
		// 回退：使用内置默认注册中心
		cfg.Registry = model.BuildDefaultRegistry()

		if cfg.DefaultCollectorsRaw != "" {
			// 用户显式设置 TAP_DEFAULT_COLLECTORS 时尊重其取值
			for _, c := range strings.Split(cfg.DefaultCollectorsRaw, ",") {
				c = strings.TrimSpace(c)
				if c != "" {
					cfg.DefaultCollectors = append(cfg.DefaultCollectors, c)
				}
			}
		} else {
			cfg.DefaultCollectors = cfg.Registry.GetCollectorNames()
		}
	}

	// 设置全局默认注册中心（供 model 包向后兼容包装函数使用）
	model.SetDefaultRegistry(cfg.Registry)

	cfg.Normalize()

	return cfg, nil
}

// Normalize 将未设置（零值）的护栏参数填充为内置默认值
func (c *Config) Normalize() {
	if c.MaxSize <= 0 {
		c.MaxSize = 10000
	}
	if c.DefaultSize <= 0 {
		c.DefaultSize = 100
	}
	if c.MaxTimeRangeDays <= 0 {
		c.MaxTimeRangeDays = 7
	}
	if c.QueryTimeout <= 0 {
		c.QueryTimeout = 60 * time.Second
	}
	if c.MaxESBuckets <= 0 {
		c.MaxESBuckets = 65536
	}
	if c.MaxMetrics <= 0 {
		c.MaxMetrics = 20
	}
	if c.MaxConcurrentStreams <= 0 {
		c.MaxConcurrentStreams = 16
	}
	if c.MaxFlattenFields <= 0 {
		c.MaxFlattenFields = 2000
	}
	if c.StreamPageSize <= 0 {
		c.StreamPageSize = 500
	}
	if c.StreamWindowBuckets <= 0 {
		c.StreamWindowBuckets = 5000
	}
	if c.StreamMaxRecords <= 0 {
		c.StreamMaxRecords = 100000
	}
	if c.StreamMaxWindows <= 0 {
		c.StreamMaxWindows = 2000
	}
	if c.StreamTimeout <= 0 {
		c.StreamTimeout = 10 * time.Minute
	}
	if c.SSEHeartbeat <= 0 {
		c.SSEHeartbeat = 15 * time.Second
	}
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
