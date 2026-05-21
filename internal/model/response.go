package model

// Response 统一响应结构
type Response struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
	Meta    *Meta  `json:"meta,omitempty"`
}

// Meta 响应元信息
type Meta struct {
	QueryTimeMs     int      `json:"query_time_ms"`
	ClustersQueried []string `json:"clusters_queried"`
	IndicesHit      []string `json:"indices_hit"`
}

// Pagination 分页信息
type Pagination struct {
	HasMore    bool   `json:"has_more"`
	NextCursor string `json:"next_cursor,omitempty"`
	Returned   int    `json:"returned"`
	Total      int    `json:"total,omitempty"`
}

// RawQueryResponse /data/raw 响应
type RawQueryResponse struct {
	Records         []Record    `json:"records"`
	Pagination      *Pagination `json:"pagination"`
	IndicesResolved []string    `json:"indices_resolved,omitempty"` // 实际查询的索引列表
}

// Record 原始数据记录
type Record struct {
	Cluster   string         `json:"cluster"`
	Collector string         `json:"collector"`
	Time      string         `json:"time"`
	Host      string         `json:"host"`
	Job       any            `json:"job"` // 原生格式: string("172.0") 或 int64(67890)
	Fields    map[string]any `json:"fields,omitempty"`
	// Data 嵌套原始数据（flatten=false 时使用）
	Data map[string]any `json:"data,omitempty"`
	// 以下字段为常见字段从 data.summary 提取的快捷访问
	CPU     *float64 `json:"cpu,omitempty"`
	Mem     *int64   `json:"mem,omitempty"`
	Name    *string  `json:"name,omitempty"`
	IOBytes *int64   `json:"io_bytes,omitempty"`
}

// TimeRange 时间范围
type TimeRange struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// TimeSeriesResponse /data/timeseries 响应
type TimeSeriesResponse struct {
	Metrics   []string                    `json:"metrics"`
	Interval  string                      `json:"interval"`
	TimeRange *TimeRange                  `json:"timerange"`
	Records   []TimeSeriesRecord          `json:"records"`
	Stats     map[string]*TimeSeriesStats `json:"stats"`
}

// TimeSeriesRecord 单条时序数据点（Grafana 扁平格式）
type TimeSeriesRecord struct {
	Metric    string  `json:"metric"`
	Label     string  `json:"label"`
	Timestamp string  `json:"timestamp"`
	Value     float64 `json:"value"`
}

// TimeSeriesStats 时序统计
type TimeSeriesStats struct {
	GlobalMax float64 `json:"global_max"`
	GlobalAvg float64 `json:"global_avg"`
}

// SummaryResponse /data/summary 响应
type SummaryResponse struct {
	Job     any            `json:"job"`
	Cluster string         `json:"cluster"`
	Time    *JobTimeRange  `json:"time"`
	Scope   *JobScope      `json:"scope"`
	Stats   map[string]any `json:"stats"`
}

// JobTimeRange 任务时间范围
type JobTimeRange struct {
	FirstSeen   string `json:"first_seen"`
	LastSeen    string `json:"last_seen"`
	DurationSec int64  `json:"duration_sec"`
}

// JobScope 任务数据范围
type JobScope struct {
	Hosts        []string `json:"hosts"`
	Collectors   []string `json:"collectors"`
	SamplesCount int64    `json:"samples_count"`
}

// SchemaResponse /schema 响应
type SchemaResponse struct {
	Clusters      []ClusterInfo   `json:"clusters"`
	Collectors    []CollectorInfo `json:"collectors"`
	CommonAliases []FieldAlias    `json:"common_aliases"`
}

// ClusterInfo 集群信息
type ClusterInfo struct {
	ID         string   `json:"id"`
	Type       string   `json:"type"`
	Alias      string   `json:"alias"`
	Enabled    bool     `json:"enabled"`
	Collectors []string `json:"collectors"` // 该集群支持的采集器（来自 Registry）
}

// CollectorInfo 采集器详细信息（来自 Registry）
type CollectorInfo struct {
	Name         string       `json:"name"`
	Description  string       `json:"description"`
	IndexPattern string       `json:"index_pattern"`
	Aliases      []FieldAlias `json:"aliases"`
}

// TriggerCollectionResponse 触发采集响应
type TriggerCollectionResponse struct {
	Status      string `json:"status"`                   // success/partial_success/failed
	ClusterName string `json:"cluster_name"`             // 集群名称
	JobID       string `json:"job_id"`                   // 原生 JobID 字符串
	ClusterTag  string `json:"cluster_tag"`              // 集群标签
	NodeType    string `json:"node_type"`                // htcondor/slurm
	NodeName    string `json:"node_name"`                // 主节点名称
	Slot        string `json:"slot,omitempty"`           // Condor 特有：槽位
	JobInfo     any    `json:"job_info"`                 // 根据集群类型动态类型
	AgentResp   any    `json:"agent_response,omitempty"` // agent 返回的响应
	Message     string `json:"message"`
}

// HTCondorJobInfo HTCondor 特定的作业信息
type HTCondorJobInfo struct {
	ClusterID string `json:"cluster_id"`
	JobID     string `json:"job_id"`
	NodeName  string `json:"node_name"`
	Slot      string `json:"slot"` // Condor 特有：槽位
	Universe  string `json:"universe"`
	CMD       string `json:"cmd"`
	Args      string `json:"args"`
	Status    string `json:"status"`
}

// SlurmJobInfo Slurm 特定的作业信息
type SlurmJobInfo struct {
	ClusterID string `json:"cluster_id"`
	JobID     string `json:"job_id"`
	NodeName  string `json:"node_name"` // 主节点名称
	NodeList  string `json:"node_list"` // 所有节点列表，逗号分隔
	Partition string `json:"partition"`
	JobState  string `json:"job_state"`
}

// CheckJobResponse Job 存在性检查响应
type CheckJobResponse struct {
	Exists  bool          `json:"exists"`
	Count   int64         `json:"count"`
	JobID   string        `json:"job_id"`
	Cluster string        `json:"cluster"`
	Time    *JobTimeRange `json:"time,omitempty"` // 数据存在时填充时间范围
}
