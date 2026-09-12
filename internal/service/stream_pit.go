package service

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/joblens/tap/internal/repository"
)

func streamStopReason(truncated bool) string {
	if truncated {
		return "record_limit"
	}
	return "exhausted"
}

func (it *rawStreamIter) ensurePIT(ctx context.Context) error {
	if it.hasHead() || it.done {
		return nil
	}
	query := it.svc.BuildRawQuery(it.req, it.plan.from, it.plan.to, it.searchAfter, it.plan.fields, it.plan.clusterName, it.plan.clusterTag, it.plan.jobFilters)
	query["sort"] = []map[string]any{{"@timestamp": map[string]any{"order": "desc", "format": "strict_date_optional_time_nanos"}}, {"_shard_doc": "asc"}}
	query["pit"] = map[string]any{"id": it.pitID, "keep_alive": intervalToES(it.svc.cfg.StreamPITKeepAlive)}
	query["track_total_hits"] = false
	qctx, cancel := it.svc.queryCtx(ctx)
	result, err := it.client.SearchPIT(qctx, query, it.svc.cfg.StreamPageMaxBytes)
	cancel()
	if err != nil {
		var se *repository.SearchError
		if errors.As(err, &se) && se.Status == 404 {
			return NewQueryError(410, "cursor_expired", "snapshot expired; restart query")
		}
		return err
	}
	if result.PITID != "" {
		it.pitID = result.PITID
	}
	it.leaseUntil = it.svc.parserSvc.nowFn().Add(it.svc.cfg.StreamPITKeepAlive)
	it.buffer = FlattenHits(result.Hits, it.plan.clusterName, it.req.Flatten, it.svc.cfg.Registry, it.svc.cfg.MaxFlattenFields)
	it.sorts = make([][]any, len(result.Hits))
	for i, hit := range result.Hits {
		if len(hit.Sort) != 2 {
			return NewQueryError(502, "invalid_sort", "snapshot response missing stable sort values")
		}
		it.sorts[i] = hit.Sort
	}
	it.pos = 0
	it.done = len(result.Hits) < it.req.Size
	if len(result.Hits) > 0 {
		it.searchAfter = result.Hits[len(result.Hits)-1].Sort
	}
	return nil
}

func (it *rawStreamIter) closePIT(ctx context.Context) {
	if it.pitID == "" {
		return
	}
	// 单集群游标保留短租约以允许重放最后一个已确认批次；ES到期自动清理。
	if len(it.committedAfter) > 0 && it.resumable {
		return
	}
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := it.client.ClosePIT(cleanup, it.pitID); err != nil {
		slog.Warn("stream PIT cleanup failed", "cluster", it.id, "error", err)
	}
}
