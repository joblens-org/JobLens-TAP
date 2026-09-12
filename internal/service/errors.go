package service

import "fmt"

// HTTP 状态码常量，避免 service 层直接依赖 net/http
const (
	StatusBadRequest      = 400
	StatusTooManyRequests = 429
	StatusGatewayTimeout  = 504
)

// 查询错误种类，供 handler 映射为 HTTP 状态码与 error_kind
const (
	ErrKindInvalidInterval = "invalid_interval"
	ErrKindTooManyBuckets  = "too_many_buckets"
	ErrKindTooManyMetrics  = "too_many_metrics"
	ErrKindTooManyWindows  = "too_many_windows"
	ErrKindTooManyRequests = "too_many_requests"
	ErrKindQueryTimeout    = "query_timeout"
	ErrKindInvalidRequest  = "invalid_request"
)

// QueryError 带种类与 HTTP 状态的查询错误
type QueryError struct {
	Status int
	Kind   string
	Msg    string
}

func (e *QueryError) Error() string {
	return e.Msg
}

// NewQueryError 构造 QueryError
func NewQueryError(status int, kind, format string, args ...any) *QueryError {
	return &QueryError{
		Status: status,
		Kind:   kind,
		Msg:    fmt.Sprintf(format, args...),
	}
}
