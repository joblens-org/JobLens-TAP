package service

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/joblens/tap/internal/config"
	"github.com/joblens/tap/internal/model"
)

// StreamTimeSeries 按时间窗分片流式输出时序聚合，全局统计在 done 中给出
func (s *StreamService) StreamTimeSeries(ctx context.Context, req *model.TimeSeriesStreamRequest, emit EmitFunc) (*model.StreamDone, error) {
	var err error
	req, err = s.normalizeTimeSeries(req)
	if err != nil {
		return nil, err
	}
	q := s.q
	started := time.Now()

	metrics := q.parserSvc.ParseFields(req.Metric)
	if len(metrics) == 0 {
		return nil, NewQueryError(StatusBadRequest, ErrKindInvalidRequest, "metric is required")
	}
	if len(metrics) > q.cfg.MaxMetrics {
		return nil, NewQueryError(StatusBadRequest, ErrKindTooManyMetrics,
			"too many metrics: %d (max %d)", len(metrics), q.cfg.MaxMetrics)
	}

	cn, ct := config.ParseClusterFilter(req.Cluster)
	esClient, clusterInfo, err := q.esManager.GetClientForCluster(cn)
	if err != nil {
		return nil, err
	}
	if ct == "" && len(clusterInfo.Tags) == 1 {
		ct = clusterInfo.Tags[0]
	}
	cn = clusterInfo.Name

	jobFilters, err := q.parserSvc.BuildJobFilter(req.Job)
	if err != nil {
		return nil, fmt.Errorf("invalid job id: %w", err)
	}

	from, err := q.parserSvc.ParseTime(req.From)
	if err != nil {
		return nil, fmt.Errorf("parse from time: %w", err)
	}
	to, err := q.parserSvc.ParseTime(req.To)
	if err != nil {
		return nil, fmt.Errorf("parse to time: %w", err)
	}
	if err := q.parserSvc.ValidateTimeRange(from, to, q.cfg.MaxTimeRangeDays); err != nil {
		return nil, NewQueryError(StatusBadRequest, ErrKindInvalidRequest, "%v", err)
	}

	interval, err := q.parserSvc.ParseInterval(req.Interval)
	if err != nil {
		return nil, NewQueryError(StatusBadRequest, ErrKindInvalidInterval, "invalid interval: %v", err)
	}

	windowBuckets := req.WindowBuckets
	if windowBuckets <= 0 {
		windowBuckets = q.cfg.StreamWindowBuckets
	}
	perWindow := windowDateBuckets(q.cfg.MaxESBuckets, windowBuckets, len(metrics), req.By != "")
	windows, err := planTimeWindows(from, to, interval, perWindow, q.cfg.StreamMaxWindows)
	if err != nil {
		return nil, err
	}

	maxRecords := req.MaxRecords
	if maxRecords <= 0 || maxRecords > q.cfg.StreamMaxRecords {
		maxRecords = q.cfg.StreamMaxRecords
	}

	tsReq := &model.TimeSeriesRequest{
		Cluster:  req.Cluster,
		Job:      req.Job,
		Metric:   req.Metric,
		Interval: req.Interval,
		From:     req.From,
		To:       req.To,
		Agg:      req.Agg,
		By:       req.By,
	}

	routing := ""
	if ct != "" {
		routing = ct
	}
	metricsByCollector := GroupMetricsByCollector(metrics, q.cfg.Registry)
	collectors := make([]string, 0, len(metricsByCollector))
	for collector := range metricsByCollector {
		collectors = append(collectors, collector)
	}
	sort.Strings(collectors)

	if err := emit(model.StreamMessage{Type: model.StreamTypeMeta, Data: model.StreamMeta{
		Clusters:      []string{cn},
		Metrics:       metrics,
		Interval:      req.Interval,
		From:          from.Format(time.RFC3339),
		To:            to.Format(time.RFC3339),
		Windows:       len(windows),
		WindowBuckets: windowBuckets,
	}}); err != nil {
		return nil, err
	}

	acc := make(map[string]*metricState, len(metrics))
	for _, m := range metrics {
		acc[m] = &metricState{}
	}

	returned := 0
	truncated := false
	for windowIndex, w := range windows {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		windowRange := &model.TimeRange{From: w.from.Format(time.RFC3339), To: w.to.Format(time.RFC3339)}
		for collectorIndex, collector := range collectors {
			collectorMetrics := metricsByCollector[collector]
			windowResp, states, err := q.streamAggregationWindow(ctx, esClient, streamAggregation{req: tsReq, metrics: collectorMetrics, collector: collector, cluster: cn, tag: ct, filters: jobFilters, routing: routing}, w)
			if err != nil {
				return nil, err
			}
			if len(windowResp.Records) == 0 {
				continue
			}
			for metric, state := range states {
				acc[metric].merge(state)
			}
			if len(windowResp.Records) > maxRecords-returned {
				windowResp.Records = windowResp.Records[:maxRecords-returned]
				truncated = true
			}
			if err := emit(model.StreamMessage{Type: model.StreamTypeRecords, Data: model.StreamRecords{
				Records:  windowResp.Records,
				Returned: len(windowResp.Records),
				Window:   windowRange,
				Cluster:  cn,
			}}); err != nil {
				return nil, err
			}
			returned += len(windowResp.Records)
			if returned >= maxRecords {
				truncated = truncated || windowIndex < len(windows)-1 || collectorIndex < len(collectors)-1
				break
			}
		}
		if truncated {
			break
		}
	}

	stats := make(map[string]*model.TimeSeriesStats, len(acc))
	for metric, a := range acc {
		if a.count > 0 {
			stats[metric] = &model.TimeSeriesStats{GlobalMax: a.max, GlobalAvg: a.sum / a.count}
		}
	}

	elapsed := time.Since(started).Milliseconds()
	slog.Info("[StreamTimeSeries] completed",
		"cluster", cn,
		"job", req.Job,
		"metrics", metrics,
		"windows", len(windows),
		"returned", returned,
		"truncated", truncated,
		"duration_ms", elapsed,
	)

	if truncated {
		stats = nil
	}
	return &model.StreamDone{Returned: returned, DurationMs: elapsed, Truncated: truncated, Stats: stats, Status: "success", Complete: !truncated, StopReason: streamStopReason(truncated)}, nil
}
