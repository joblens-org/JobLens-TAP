package service

import (
	"strconv"
	"strings"
	"time"

	"github.com/joblens/tap/internal/model"
)

func (s *StreamService) normalizeRaw(req *model.RawStreamRequest) (*model.RawStreamRequest, error) {
	copy := *req
	if copy.To == "" {
		copy.To = "now"
	}
	if copy.PageSize < 0 || copy.MaxRecords < 0 {
		return nil, NewQueryError(400, ErrKindInvalidRequest, "record limits must not be negative")
	}
	if copy.Cluster == "" || copy.Job == "" {
		return nil, NewQueryError(400, ErrKindInvalidRequest, "cluster and job are required")
	}
	if !copy.FullRange && copy.From == "" {
		return nil, NewQueryError(400, ErrKindInvalidRequest, "from is required")
	}
	return &copy, nil
}

func (s *StreamService) parseStreamRange(fromText, toText string) (time.Time, time.Time, error) {
	now := s.q.parserSvc.nowFn()
	parser := NewParserService()
	parser.SetNowFunc(func() time.Time { return now })
	from, err := parser.ParseTime(fromText)
	if err != nil {
		return time.Time{}, time.Time{}, NewQueryError(400, ErrKindInvalidRequest, "invalid from time")
	}
	to, err := parser.ParseTime(toText)
	if err != nil {
		return time.Time{}, time.Time{}, NewQueryError(400, ErrKindInvalidRequest, "invalid to time")
	}
	if err := parser.ValidateTimeRange(from, to, s.q.cfg.MaxTimeRangeDays); err != nil {
		return time.Time{}, time.Time{}, NewQueryError(400, ErrKindInvalidRequest, "%v", err)
	}
	return from, to, nil
}

func (s *StreamService) normalizeTimeSeries(req *model.TimeSeriesStreamRequest) (*model.TimeSeriesStreamRequest, error) {
	copy := *req
	if copy.To == "" {
		copy.To = "now"
	}
	if copy.Agg == "" {
		copy.Agg = "avg"
	}
	switch copy.Agg {
	case "avg", "min", "max", "sum":
	default:
		return nil, NewQueryError(400, ErrKindInvalidRequest, "unsupported aggregation")
	}
	if copy.WindowBuckets < 0 || copy.MaxRecords < 0 {
		return nil, NewQueryError(400, ErrKindInvalidRequest, "record and window limits must not be negative")
	}
	from, to, err := s.parseStreamRange(copy.From, copy.To)
	if err != nil {
		return nil, err
	}
	copy.From, copy.To = from.Format(time.RFC3339Nano), to.Format(time.RFC3339Nano)
	interval, err := s.q.parserSvc.ParseInterval(copy.Interval)
	if err != nil {
		return nil, NewQueryError(400, ErrKindInvalidInterval, "%v", err)
	}
	copy.Interval = intervalToES(interval)
	metrics := s.q.parserSvc.ParseFields(copy.Metric)
	seen := make(map[string]bool, len(metrics))
	unique := make([]string, 0, len(metrics))
	for _, metric := range metrics {
		if !seen[metric] {
			unique = append(unique, metric)
			seen[metric] = true
		}
	}
	copy.Metric = strings.Join(unique, ",")
	return &copy, nil
}

func intervalToES(interval time.Duration) string {
	return strconv.FormatInt(interval.Milliseconds(), 10) + "ms"
}
