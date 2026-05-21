// Package model 定义请求和响应的数据模型
package model

// RawQueryRequest /data/raw 端点请求参数
type RawQueryRequest struct {
	Cluster   string `form:"cluster" binding:"required"`
	Job       string `form:"job" binding:"required"` // 原生格式: Condor "172.0", Slurm "67890"
	From      string `form:"from"`
	To        string `form:"to" default:"now"`
	Collector string `form:"collector" default:""`
	Fields    string `form:"fields" default:""`
	Size      int    `form:"size" binding:"max=10000" default:"100"`
	Cursor    string `form:"cursor" default:""`
	Flatten   bool   `form:"flatten" default:"true"`
	FullRange bool   `form:"full_range" default:"false"`
}

// TimeSeriesRequest /data/timeseries 端点请求参数
type TimeSeriesRequest struct {
	Cluster  string `form:"cluster" binding:"required"`
	Job      string `form:"job" binding:"required"` // 原生格式
	Metric   string `form:"metric" binding:"required"`
	Interval string `form:"interval" binding:"required"`
	From     string `form:"from" binding:"required"`
	To       string `form:"to" default:"now"`
	Agg      string `form:"agg" default:"avg"`
	By       string `form:"by" default:""`
}

// SummaryRequest /data/summary 端点请求参数
type SummaryRequest struct {
	Cluster    string `form:"cluster" binding:"required"`
	Job        string `form:"job" binding:"required"` // 原生格式
	Collectors string `form:"collectors" default:""`
}

// SchemaRequest /schema 端点请求参数
type SchemaRequest struct {
	Cluster   string `form:"cluster" default:""`
	Collector string `form:"collector" default:""`
}

// Cursor 分页游标结构
type Cursor struct {
	Cluster     string `json:"cluster"`
	SearchAfter []any  `json:"search_after"`
	QueryHash   string `json:"query_hash"`
	KeepAlive   string `json:"keep_alive,omitempty"`
}

// TriggerCollectionRequest 触发采集请求
type TriggerCollectionRequest struct {
	ClusterName string `json:"cluster_name" binding:"required"` // 集群名称
	ClusterTag  string `json:"cluster_tag" binding:"required"`  // 集群标签
	JobID       string `json:"job_id" binding:"required"`       // 原生 JobID 字符串（如 "172.0"）
	Collector   string `json:"collector" binding:"required"`    // 采集器名称，支持逗号分隔多值（如 "cpumem,io,net"）
}

// CheckJobRequest 检查 Job 数据是否存在请求
type CheckJobRequest struct {
	ClusterName string `form:"cluster_name" binding:"required"` // 集群名称
	ClusterTag  string `form:"cluster_tag"`                     // 集群标签（可选）
	JobID       string `form:"job_id" binding:"required"`       // 原生 JobID 字符串（如 "172.0"）
}

// DirectTriggerCollectionRequest 直接触发采集请求（跳过脚本查询，用户直接提供节点信息）
type DirectTriggerCollectionRequest struct {
	ClusterName string `json:"cluster_name" binding:"required"` // 集群名称
	ClusterTag  string `json:"cluster_tag" binding:"required"`  // 集群标签
	JobID       string `json:"job_id" binding:"required"`       // 原生 JobID 字符串（如 "172.0"）
	Collector   string `json:"collector" binding:"required"`    // 采集器名称，支持逗号分隔多值（如 "cpumem,io,net"）
	Node        string `json:"node" binding:"required"`         // 节点主机名
	Slot        string `json:"slot"`                            // HTCondor 槽位（HTCondor 集群必填）
}
