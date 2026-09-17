package service

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/joblens/tap/internal/model"
	"github.com/joblens/tap/internal/repository"
)

type pitPage struct {
	hits      []repository.SearchHit
	records   []model.Record
	sorts     [][]any
	pitID     string
	esMs      int64
	flattenMs int64
	rawBytes  int64
	err       error
}

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
	if it.pending != nil {
		page := it.pending
		it.pending = nil
		if err := it.applyPITPage(page); err != nil {
			return err
		}
		it.startPITPrefetch(ctx)
		return nil
	}
	if it.fetching {
		select {
		case page := <-it.prefetchCh:
			it.fetching = false
			if err := it.applyPITPage(page); err != nil {
				return err
			}
			it.startPITPrefetch(ctx)
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err := it.applyPITPage(it.fetchPITPage(ctx, it.pitID, it.searchAfter)); err != nil {
		return err
	}
	it.startPITPrefetch(ctx)
	return nil
}

func (it *rawStreamIter) applyPITPage(page *pitPage) error {
	if page.err != nil {
		return page.err
	}
	it.buffer = page.records
	it.sorts = page.sorts
	if page.pitID != "" {
		it.pitID = page.pitID
	}
	it.leaseUntil = it.svc.parserSvc.nowFn().Add(it.svc.cfg.StreamPITKeepAlive)
	it.pos = 0
	it.done = len(page.hits) < it.req.Size
	if len(page.hits) > 0 {
		it.searchAfter = page.hits[len(page.hits)-1].Sort
	}
	slog.Info("PERF page",
		"cluster", it.id,
		"es_ms", page.esMs,
		"flatten_ms", page.flattenMs,
		"raw_bytes", page.rawBytes,
		"hits", len(page.hits),
	)
	return nil
}

func (it *rawStreamIter) startPITPrefetch(ctx context.Context) {
	if it.done || it.fetching || it.pending != nil {
		return
	}
	if it.prefetchCh == nil {
		it.prefetchCh = make(chan *pitPage, 1)
	}
	it.fetching = true
	pitID := it.pitID
	searchAfter := it.searchAfter
	go func() {
		page := it.fetchPITPage(ctx, pitID, searchAfter)
		select {
		case it.prefetchCh <- page:
		case <-ctx.Done():
		}
	}()
}

func (it *rawStreamIter) fetchPITPage(ctx context.Context, pitID string, searchAfter []any) *pitPage {
	query := it.svc.BuildRawQuery(it.req, it.plan.from, it.plan.to, searchAfter, it.plan.fields, it.plan.clusterName, it.plan.clusterTag, it.plan.jobFilters)
	query["sort"] = []map[string]any{{"@timestamp": map[string]any{"order": "desc", "format": "strict_date_optional_time_nanos"}}, {"_shard_doc": "asc"}}
	query["pit"] = map[string]any{"id": pitID, "keep_alive": intervalToES(it.svc.cfg.StreamPITKeepAlive)}
	query["track_total_hits"] = false

	qctx, cancel := it.svc.queryCtx(ctx)
	tEsStart := time.Now()
	result, err := it.client.SearchPIT(qctx, query, it.svc.cfg.StreamPageMaxBytes)
	tEs := time.Since(tEsStart)
	cancel()
	if err != nil {
		var se *repository.SearchError
		if errors.As(err, &se) && se.Status == 404 {
			return &pitPage{err: NewQueryError(410, "cursor_expired", "snapshot expired; restart query")}
		}
		return &pitPage{err: err}
	}
	tFlStart := time.Now()
	records := FlattenHits(result.Hits, it.plan.clusterName, it.req.Flatten, it.svc.cfg.Registry, it.svc.cfg.MaxFlattenFields)
	tFl := time.Since(tFlStart)
	sorts := make([][]any, len(result.Hits))
	for i, hit := range result.Hits {
		if len(hit.Sort) != 2 {
			return &pitPage{err: NewQueryError(502, "invalid_sort", "snapshot response missing stable sort values")}
		}
		sorts[i] = hit.Sort
	}
	return &pitPage{
		hits:      result.Hits,
		records:   records,
		sorts:     sorts,
		pitID:     result.PITID,
		esMs:      tEs.Milliseconds(),
		flattenMs: tFl.Milliseconds(),
		rawBytes:  result.RawBytes,
	}
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
