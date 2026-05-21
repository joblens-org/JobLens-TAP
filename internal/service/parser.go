package service

import (
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// ParserService 解析服务（时间、字段等）
type ParserService struct {
	nowFn func() time.Time
}

// NewParserService 创建解析服务
func NewParserService() *ParserService {
	return &ParserService{
		nowFn: time.Now,
	}
}

// SetNowFunc 设置自定义的 now 函数（用于测试）
func (s *ParserService) SetNowFunc(fn func() time.Time) {
	s.nowFn = fn
}

// ParseTime 解析时间字符串
// 支持格式：
// - ISO8601: 2026-04-02T22:00:00Z
// - 相对时间: now, now-1h, now-1d, now-30m
// - 简化格式: 1h, 1d（表示 now-1h, now-1d）
func (s *ParserService) ParseTime(input string) (time.Time, error) {
	input = strings.TrimSpace(input)

	// 处理 now
	if input == "now" {
		now := s.nowFn()
		// LOG_REASON: now 是特殊时间锚点，记录实际值可排查时间偏差问题
		slog.Debug("[ParseTime] resolved now", "input", input, "resolved", now.Format(time.RFC3339))
		return now, nil
	}

	// 处理相对时间 now-1h, now-1d
	if strings.HasPrefix(input, "now") {
		duration, err := s.parseDuration(input[3:])
		if err != nil {
			return time.Time{}, fmt.Errorf("invalid relative time: %s", input)
		}
		result := s.nowFn().Add(duration)
		// LOG_REASON: 相对时间解析是常见错误来源，记录输入和解析结果便于排查时间范围异常
		slog.Debug("[ParseTime] resolved relative", "input", input, "duration", duration, "resolved", result.Format(time.RFC3339))
		return result, nil
	}

	// 处理简化格式 1h, 1d
	if matched, _ := regexp.MatchString(`^\d+[smhdw]$`, input); matched {
		duration, err := s.parseDuration("-" + input)
		if err != nil {
			return time.Time{}, fmt.Errorf("invalid duration: %s", input)
		}
		result := s.nowFn().Add(duration)
		slog.Debug("[ParseTime] resolved shorthand", "input", input, "resolved", result.Format(time.RFC3339))
		return result, nil
	}
	// 尝试 ISO8601 格式
	formats := []string{
		time.RFC3339,
		"2006-01-02T15:04:05",
		"2006-01-02T15:04",
		"2006-01-02 15:04:05",
		"2006-01-02 15:04",
		"2006-01-02",
	}

	for _, format := range formats {
		if t, err := time.Parse(format, input); err == nil {
			return t, nil
		}
	}

	return time.Time{}, fmt.Errorf("unable to parse time: %s", input)
}

// parseDuration 解析时长字符串（如 -1h, +30m）
func (s *ParserService) parseDuration(input string) (time.Duration, error) {
	input = strings.TrimSpace(input)

	// 提取符号
	sign := 1
	if strings.HasPrefix(input, "-") {
		sign = -1
		input = input[1:]
	} else if strings.HasPrefix(input, "+") {
		input = input[1:]
	}

	// 解析数值和单位
	re := regexp.MustCompile(`^(\d+)([smhdw])$`)
	matches := re.FindStringSubmatch(input)
	if matches == nil {
		return 0, fmt.Errorf("invalid duration format: %s", input)
	}

	value, _ := strconv.Atoi(matches[1])
	unit := matches[2]

	var multiplier time.Duration
	switch unit {
	case "s":
		multiplier = time.Second
	case "m":
		multiplier = time.Minute
	case "h":
		multiplier = time.Hour
	case "d":
		multiplier = 24 * time.Hour
	case "w":
		multiplier = 7 * 24 * time.Hour
	default:
		return 0, fmt.Errorf("unknown time unit: %s", unit)
	}

	return time.Duration(sign*value) * multiplier, nil
}

// ParseFields 解析字段列表（逗号分隔）
func (s *ParserService) ParseFields(input string) []string {
	if input == "" {
		return nil
	}

	var fields []string
	parts := strings.Split(input, ",")
	for _, part := range parts {
		field := strings.TrimSpace(part)
		if field != "" {
			fields = append(fields, field)
		}
	}
	return fields
}

// BuildJobFilter 根据原生 JobID 字符串构建 ES term filter
// 统一使用 job_info.NativeJobID.keyword 做精确匹配
func (s *ParserService) BuildJobFilter(jobRaw string) ([]map[string]any, error) {
	if jobRaw == "" {
		return nil, fmt.Errorf("job id is empty")
	}
	return []map[string]any{
		{"term": map[string]any{"job_info.NativeJobID.keyword": jobRaw}},
	}, nil
}

// ValidateTimeRange 验证时间范围
func (s *ParserService) ValidateTimeRange(from, to time.Time, maxDays int) error {
	if from.After(to) {
		slog.Debug("[ValidateTimeRange] from after to",
			"from", from.Format(time.RFC3339),
			"to", to.Format(time.RFC3339),
		)
		return fmt.Errorf("from time must be before to time")
	}

	duration := to.Sub(from)
	if duration > time.Duration(maxDays)*24*time.Hour {
		slog.Debug("[ValidateTimeRange] exceeds max days",
			"from", from.Format(time.RFC3339),
			"to", to.Format(time.RFC3339),
			"duration", duration,
			"max_days", maxDays,
		)
		return fmt.Errorf("time range exceeds maximum allowed %d days", maxDays)
	}

	return nil
}
