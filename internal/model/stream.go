package model

const (
	StreamFormatNDJSON = "ndjson"
	StreamFormatSSE    = "sse"

	StreamTypeMeta    = "meta"
	StreamTypeRecords = "records"
	StreamTypeDone    = "done"
	StreamTypeError   = "error"
	StreamTypePing    = "ping"
)

// RawStreamRequest /data/raw/stream 请求参数
type RawStreamRequest struct {
	Cluster    string `form:"cluster" binding:"required"`
	Job        string `form:"job" binding:"required"`
	From       string `form:"from"`
	To         string `form:"to" default:"now"`
	Collector  string `form:"collector"`
	Fields     string `form:"fields"`
	Flatten    bool   `form:"flatten" default:"false"`
	FullRange  bool   `form:"full_range" default:"false"`
	PageSize   string `form:"page_size"`
	MaxRecords int    `form:"max_records"`
	Format     string `form:"format"`
	Cursor     string `form:"cursor"`
}

// TimeSeriesStreamRequest /data/timeseries/stream 请求参数
type TimeSeriesStreamRequest struct {
	Cluster       string `form:"cluster" binding:"required"`
	Job           string `form:"job" binding:"required"`
	Metric        string `form:"metric" binding:"required"`
	Interval      string `form:"interval" binding:"required"`
	From          string `form:"from" binding:"required"`
	To            string `form:"to" default:"now"`
	Agg           string `form:"agg" default:"avg"`
	By            string `form:"by"`
	WindowBuckets int    `form:"window_buckets"`
	MaxRecords    int    `form:"max_records"`
	Format        string `form:"format"`
}

// StreamMessage 流式消息统一信封
type StreamMessage struct {
	Type string `json:"type"`
	ID   string `json:"id,omitempty"`
	Data any    `json:"data"`
}

// StreamMeta 流起始元信息
type StreamMeta struct {
	Clusters      []string `json:"clusters,omitempty"`
	Indices       []string `json:"indices,omitempty"`
	PageSize      int      `json:"page_size,omitempty"`
	WindowBuckets int      `json:"window_buckets,omitempty"`
	Flatten       bool     `json:"flatten,omitempty"`
	Metrics       []string `json:"metrics,omitempty"`
	Interval      string   `json:"interval,omitempty"`
	From          string   `json:"from,omitempty"`
	To            string   `json:"to,omitempty"`
	Windows       int      `json:"windows,omitempty"`
}

// StreamRecords 一批记录
type StreamRecords struct {
	Records  any        `json:"records"`
	Returned int        `json:"returned"`
	Window   *TimeRange `json:"window,omitempty"`
	Cluster  string     `json:"cluster,omitempty"`
}

// StreamDone 流结束汇总
type StreamDone struct {
	Status         string                      `json:"status"`
	Complete       bool                        `json:"complete"`
	StopReason     string                      `json:"stop_reason"`
	Cursor         string                      `json:"cursor,omitempty"`
	FailedClusters []string                    `json:"failed_clusters,omitempty"`
	Bytes          int64                       `json:"bytes"`
	Returned       int                         `json:"returned"`
	DurationMs     int64                       `json:"duration_ms"`
	Truncated      bool                        `json:"truncated"`
	Stats          map[string]*TimeSeriesStats `json:"stats,omitempty"`
}

// StreamError 流中途错误
type StreamError struct {
	Code    int    `json:"code"`
	Kind    string `json:"kind"`
	Message string `json:"message"`
	Cluster string `json:"cluster,omitempty"`
}
