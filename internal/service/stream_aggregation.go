package service

import (
	"context"
	"time"

	"github.com/joblens/tap/internal/model"
	"github.com/joblens/tap/internal/repository"
)

type metricState struct {
	sum   float64
	count float64
	max   float64
}

func (a *metricState) merge(stats map[string]any) {
	count, _ := stats["count"].(float64)
	if count == 0 {
		return
	}
	sum, _ := stats["sum"].(float64)
	max, _ := stats["max"].(float64)
	if a.count == 0 || max > a.max {
		a.max = max
	}
	a.sum += sum
	a.count += count
}

type streamAggregation struct {
	req       *model.TimeSeriesRequest
	metrics   []string
	collector string
	cluster   string
	tag       string
	filters   []map[string]any
	routing   string
}

func (s *QueryService) streamAggregationWindow(ctx context.Context, client *repository.ESClient, spec streamAggregation, w timeWindow) (*model.TimeSeriesResponse, map[string]map[string]any, error) {
	query := s.BuildMultiMetricTimeSeriesQuery(spec.req, w.from, w.to, spec.metrics, spec.collector, spec.cluster, spec.tag, spec.filters)
	filters := query["query"].(map[string]any)["bool"].(map[string]any)["filter"].([]map[string]any)
	for _, filter := range filters {
		if r, ok := filter["range"].(map[string]any); ok {
			bounds := r["@timestamp"].(map[string]any)
			bounds["gte"] = w.from.Format(time.RFC3339Nano)
			bounds["lte"] = w.to.Format(time.RFC3339Nano)
			if !w.inclusive {
				delete(bounds, "lte")
				bounds["lt"] = w.to.Format(time.RFC3339Nano)
			}
		}
	}
	aggs := query["aggs"].(map[string]any)
	tsAggs := aggs
	if spec.req.By != "" {
		tsAggs = aggs["group_by_"+spec.req.By].(map[string]any)["aggs"].(map[string]any)
	}
	hist := tsAggs["timeseries"].(map[string]any)["date_histogram"].(map[string]any)
	upper := w.to
	if !w.inclusive {
		upper = upper.Add(-time.Millisecond)
	}
	hist["extended_bounds"] = map[string]any{"min": w.from.Format(time.RFC3339Nano), "max": upper.Format(time.RFC3339Nano)}
	for _, metric := range spec.metrics {
		field, ok := s.cfg.Registry.GetESField(metric)
		if !ok {
			field = metric
		}
		aggs["stats_"+metric] = s.buildStatsAggWithNested(metric, field)
	}
	indices, err := s.indexSvc.ResolveIndices(spec.collector, w.from, w.to, s.cfg.DefaultCollectors)
	if err != nil {
		return nil, nil, err
	}
	qctx, cancel := s.queryCtx(ctx)
	defer cancel()
	result, err := client.Search(qctx, indices, query, spec.routing, s.cfg.StreamPageMaxBytes)
	if err != nil {
		return nil, nil, err
	}
	if result.TimedOut {
		return nil, nil, context.DeadlineExceeded
	}
	response := &model.TimeSeriesResponse{Metrics: spec.metrics, Interval: spec.req.Interval, Records: []model.TimeSeriesRecord{}, Stats: map[string]*model.TimeSeriesStats{}}
	s.parseMultiMetricAggregation(result.Aggregations, spec.req, spec.metrics, response)
	states := make(map[string]map[string]any)
	for _, metric := range spec.metrics {
		if stats, ok := result.Aggregations["stats_"+metric].(map[string]any); ok {
			states[metric] = s.unwrapNestedAgg(stats, metric)
		}
	}
	if spec.req.By != "" {
		group, _ := result.Aggregations["group_by_"+spec.req.By].(map[string]any)
		if other, _ := group["sum_other_doc_count"].(float64); other > 0 {
			return nil, nil, NewQueryError(422, "group_limit", "grouped result exceeds group limit; narrow the query")
		}
	}
	return response, states, nil
}
