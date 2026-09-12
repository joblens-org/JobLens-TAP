package service

import (
	"container/heap"
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/joblens/tap/internal/config"
	"github.com/joblens/tap/internal/model"
	"github.com/joblens/tap/internal/repository"
)

// EmitFunc 推送一条流式消息；返回错误表示客户端已断开，应中止
type EmitFunc func(msg model.StreamMessage) error

// StreamService 分片 + 流式查询服务
type StreamService struct {
	q *QueryService
}

func NewStreamService(q *QueryService) *StreamService {
	return &StreamService{q: q}
}

type rawStreamIter struct {
	svc    *QueryService
	plan   *rawQueryPlan
	client *repository.ESClient
	req    *model.RawQueryRequest
	id     string

	searchAfter     []any
	buffer          []model.Record
	pos             int
	done            bool
	nextCursor      string
	committedCursor string
}

func (it *rawStreamIter) hasHead() bool {
	return it.pos < len(it.buffer)
}

func (it *rawStreamIter) head() model.Record {
	return it.buffer[it.pos]
}

func (it *rawStreamIter) advance() {
	it.pos++
	if it.pos >= len(it.buffer) {
		it.committedCursor = it.nextCursor
	}
}

func (it *rawStreamIter) ensure(ctx context.Context) error {
	for it.pos >= len(it.buffer) && !it.done {
		resp, err := it.svc.executeRawPage(ctx, it.plan, it.client, it.req, it.searchAfter)
		if err != nil {
			return err
		}
		it.buffer = resp.Records
		it.pos = 0
		if resp.Pagination.HasMore && resp.Pagination.NextCursor != "" {
			it.nextCursor = resp.Pagination.NextCursor
			if nc := decodeCursor(resp.Pagination.NextCursor); nc != nil {
				it.searchAfter = nc.SearchAfter
			}
		} else {
			it.nextCursor = ""
			it.done = true
		}
		if len(it.buffer) == 0 {
			it.done = true
		}
	}
	return nil
}

type rawIterHeap []*rawStreamIter

func (h rawIterHeap) Len() int { return len(h) }
func (h rawIterHeap) Less(i, j int) bool {
	return compareTime(h[i].head().Time, h[j].head().Time) > 0
}
func (h rawIterHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *rawIterHeap) Push(x any)   { *h = append(*h, x.(*rawStreamIter)) }
func (h *rawIterHeap) Pop() any {
	old := *h
	n := len(old)
	it := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return it
}

// StreamRaw 分页 + 多集群 k-way merge 流式输出，保持全局时间降序
func (s *StreamService) StreamRaw(ctx context.Context, req *model.RawStreamRequest, clusterIDs []string, emit EmitFunc) (*model.StreamDone, error) {
	q := s.q
	started := time.Now()

	pageSize := req.PageSize
	if pageSize <= 0 {
		pageSize = q.cfg.StreamPageSize
	}
	if q.cfg.MaxSize > 0 && pageSize > q.cfg.MaxSize {
		pageSize = q.cfg.MaxSize
	}
	maxRecords := req.MaxRecords
	if maxRecords <= 0 {
		maxRecords = q.cfg.StreamMaxRecords
	}

	var cursor *model.Cursor
	if req.Cursor != "" {
		cursor = decodeCursor(req.Cursor)
	}
	if cursor != nil && cursor.Cluster != "" {
		clusterIDs = []string{cursor.Cluster}
	}
	if len(clusterIDs) == 0 {
		return nil, NewQueryError(StatusBadRequest, ErrKindInvalidRequest, "no clusters to query")
	}

	rawReq := &model.RawQueryRequest{
		Cluster:   req.Cluster,
		Job:       req.Job,
		From:      req.From,
		To:        req.To,
		Collector: req.Collector,
		Fields:    req.Fields,
		Size:      pageSize,
		Flatten:   req.Flatten,
		FullRange: req.FullRange,
	}

	iters := make([]*rawStreamIter, 0, len(clusterIDs))
	allIndices := make([]string, 0)
	seenIdx := make(map[string]bool)
	var buildErrs []error
	for _, id := range clusterIDs {
		plan, client, err := q.buildRawPlan(ctx, id, rawReq)
		if err != nil {
			buildErrs = append(buildErrs, fmt.Errorf("cluster %s: %w", id, err))
			continue
		}
		iters = append(iters, &rawStreamIter{svc: q, plan: plan, client: client, req: rawReq, id: id})
		for _, idx := range plan.indices {
			if !seenIdx[idx] {
				seenIdx[idx] = true
				allIndices = append(allIndices, idx)
			}
		}
	}
	if len(iters) == 0 {
		if len(buildErrs) > 0 {
			return nil, buildErrs[0]
		}
		return nil, NewQueryError(StatusBadRequest, ErrKindInvalidRequest, "no clusters to query")
	}

	meta := model.StreamMeta{
		Clusters: clusterIDs,
		Indices:  allIndices,
		PageSize: pageSize,
		Flatten:  req.Flatten,
	}
	if err := emit(model.StreamMessage{Type: model.StreamTypeMeta, Data: meta}); err != nil {
		return nil, err
	}
	for _, err := range buildErrs {
		_ = emit(model.StreamMessage{Type: model.StreamTypeError, Data: model.StreamError{
			Code: StatusBadRequest, Kind: ErrKindInvalidRequest, Message: err.Error(),
		}})
	}

	returned, truncated, err := mergeRawStream(ctx, iters, pageSize, maxRecords, emit)
	if err != nil {
		return nil, err
	}

	elapsed := time.Since(started).Milliseconds()
	slog.Info("[StreamRaw] completed",
		"clusters", clusterIDs,
		"returned", returned,
		"truncated", truncated,
		"duration_ms", elapsed,
	)

	return &model.StreamDone{Returned: returned, DurationMs: elapsed, Truncated: truncated}, nil
}

// mergeRawStream 对多个按时间降序的迭代器做 k-way merge，按批推送记录
func mergeRawStream(ctx context.Context, iters []*rawStreamIter, pageSize, maxRecords int, emit EmitFunc) (int, bool, error) {
	h := &rawIterHeap{}
	heap.Init(h)
	for _, it := range iters {
		if err := it.ensure(ctx); err != nil {
			_ = emit(model.StreamMessage{Type: model.StreamTypeError, Data: model.StreamError{
				Code: StatusGatewayTimeout, Kind: ErrKindQueryTimeout, Message: err.Error(), Cluster: it.id,
			}})
			continue
		}
		if it.hasHead() {
			heap.Push(h, it)
		}
	}

	returned := 0
	truncated := false
	batch := make([]model.Record, 0, pageSize)

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		msg := model.StreamMessage{
			Type: model.StreamTypeRecords,
			Data: model.StreamRecords{Records: batch, Returned: len(batch)},
		}
		if len(iters) == 1 {
			msg.ID = iters[0].committedCursor
		}
		if err := emit(msg); err != nil {
			return err
		}
		returned += len(batch)
		batch = make([]model.Record, 0, pageSize)
		return nil
	}

	for h.Len() > 0 {
		if err := ctx.Err(); err != nil {
			return returned, truncated, err
		}
		it := (*h)[0]
		batch = append(batch, it.head())
		it.advance()

		if err := it.ensure(ctx); err != nil {
			_ = emit(model.StreamMessage{Type: model.StreamTypeError, Data: model.StreamError{
				Code: StatusGatewayTimeout, Kind: ErrKindQueryTimeout, Message: err.Error(), Cluster: it.id,
			}})
			heap.Pop(h)
		} else if it.hasHead() {
			heap.Fix(h, 0)
		} else {
			heap.Pop(h)
		}

		if len(batch) >= pageSize {
			if err := flush(); err != nil {
				return returned, truncated, err
			}
			if returned >= maxRecords {
				truncated = h.Len() > 0
				break
			}
		}
	}

	if err := flush(); err != nil {
		return returned, truncated, err
	}
	return returned, truncated, nil
}

type timeWindow struct {
	from time.Time
	to   time.Time
}

func splitTimeWindows(from, to time.Time, span time.Duration) []timeWindow {
	if span <= 0 {
		span = time.Hour
	}
	var windows []timeWindow
	for start := from; start.Before(to); start = start.Add(span) {
		end := start.Add(span)
		if end.After(to) {
			end = to
		}
		windows = append(windows, timeWindow{from: start, to: end})
	}
	return windows
}

// StreamTimeSeries 按时间窗分片流式输出时序聚合，全局统计在 done 中给出
func (s *StreamService) StreamTimeSeries(ctx context.Context, req *model.TimeSeriesStreamRequest, emit EmitFunc) (*model.StreamDone, error) {
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
		return nil, err
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
	span := interval * time.Duration(perWindow)
	windows := splitTimeWindows(from, to, span)
	if len(windows) > q.cfg.StreamMaxWindows {
		return nil, NewQueryError(StatusBadRequest, ErrKindTooManyWindows,
			"%d windows exceeds limit %d, increase interval or narrow time range", len(windows), q.cfg.StreamMaxWindows)
	}

	maxRecords := req.MaxRecords
	if maxRecords <= 0 {
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

	acc := make(map[string]*tsAccumulator, len(metrics))
	for _, m := range metrics {
		acc[m] = &tsAccumulator{}
	}

	returned := 0
	truncated := false
	for _, w := range windows {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		windowRange := &model.TimeRange{From: w.from.Format(time.RFC3339), To: w.to.Format(time.RFC3339)}
		for collector, collectorMetrics := range metricsByCollector {
			windowResp, _, _, err := q.executeTimeSeriesWindow(ctx, esClient, tsReq, collectorMetrics, collector, cn, ct, w.from, w.to, jobFilters, routing)
			if err != nil {
				return nil, err
			}
			if len(windowResp.Records) == 0 {
				continue
			}
			for _, r := range windowResp.Records {
				if a, ok := acc[r.Metric]; ok {
					a.add(r.Value)
				}
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
				truncated = true
				break
			}
		}
		if truncated {
			break
		}
	}

	stats := make(map[string]*model.TimeSeriesStats, len(acc))
	for metric, a := range acc {
		stats[metric] = a.stats()
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

	return &model.StreamDone{Returned: returned, DurationMs: elapsed, Truncated: truncated, Stats: stats}, nil
}

type tsAccumulator struct {
	max   float64
	sum   float64
	count int
}

func (a *tsAccumulator) add(v float64) {
	if v > a.max {
		a.max = v
	}
	a.sum += v
	a.count++
}

func (a *tsAccumulator) stats() *model.TimeSeriesStats {
	st := &model.TimeSeriesStats{GlobalMax: a.max}
	if a.count > 0 {
		st.GlobalAvg = a.sum / float64(a.count)
	}
	return st
}
