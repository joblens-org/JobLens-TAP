package service

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/joblens/tap/internal/model"
	"github.com/joblens/tap/internal/repository"
)

// EmitFunc 推送一条流式消息；返回错误表示客户端已断开，应中止
type EmitFunc func(msg model.StreamMessage) error

// StreamService 分片 + 流式查询服务
type StreamService struct {
	q      *QueryService
	signer cursorSigner
}

func NewStreamService(q *QueryService) *StreamService {
	return &StreamService{q: q, signer: newCursorSigner(q.cfg.StreamCursorKey)}
}

// StreamRaw 分页 + 多集群 k-way merge 流式输出，保持全局时间降序
func (s *StreamService) StreamRaw(ctx context.Context, req *model.RawStreamRequest, clusterIDs []string, emit EmitFunc) (*model.StreamDone, error) {
	var err error
	req, err = s.normalizeRaw(req)
	if err != nil {
		return nil, err
	}
	q := s.q
	started := time.Now()

	pageSize, err := s.resolveStreamPageSize(req)
	if err != nil {
		return nil, err
	}
	if q.cfg.MaxSize > 0 && pageSize > q.cfg.MaxSize {
		pageSize = q.cfg.MaxSize
	}
	maxRecords := req.MaxRecords
	if maxRecords <= 0 || maxRecords > q.cfg.StreamMaxRecords {
		maxRecords = q.cfg.StreamMaxRecords
	}

	var resume *rawResumeCursor
	if req.Cursor != "" {
		var err error
		resume, err = s.signer.decode(req.Cursor, rawStreamHash(req), q.parserSvc.nowFn())
		if err != nil {
			return nil, err
		}
		if len(clusterIDs) != 1 || clusterIDs[0] != resume.Cluster {
			return nil, NewQueryError(400, ErrKindInvalidRequest, "resume requires the original single cluster")
		}
		if resume.Done {
			return &model.StreamDone{Status: "success", Complete: true, StopReason: "exhausted", Cursor: req.Cursor}, nil
		}
	}
	if len(clusterIDs) > q.cfg.StreamMaxClusters {
		return nil, NewQueryError(400, ErrKindInvalidRequest, "too many clusters")
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
	if resume == nil && !rawReq.FullRange {
		from, to, err := s.parseStreamRange(rawReq.From, rawReq.To)
		if err != nil {
			return nil, err
		}
		rawReq.From, rawReq.To = from.Format(time.RFC3339Nano), to.Format(time.RFC3339Nano)
	}

	iters := make([]*rawStreamIter, 0, len(clusterIDs))
	defer func() {
		for _, it := range iters {
			it.closePIT(ctx)
		}
	}()
	allIndices := make([]string, 0)
	seenIdx := make(map[string]bool)
	var buildErrs []error
	for _, id := range clusterIDs {
		if resume != nil {
			rawReq.From, rawReq.To = resume.From.Format(time.RFC3339Nano), resume.To.Format(time.RFC3339Nano)
		}
		var plan *rawQueryPlan
		var client *repository.ESClient
		var err error
		if resume != nil {
			plan, client, err = s.resumeRawPlan(id, rawReq, resume)
		} else {
			plan, client, err = q.buildRawPlan(ctx, id, rawReq)
		}
		if err != nil {
			buildErrs = append(buildErrs, fmt.Errorf("cluster %s: %w", id, err))
			continue
		}
		it := &rawStreamIter{svc: q, plan: plan, client: client, req: rawReq, id: id, hash: rawStreamHash(req), signer: s.signer, resumable: len(clusterIDs) == 1}
		switch {
		case plan.empty:
			it.done = true
		case resume != nil:
			it.pitID = resume.PIT
			it.leaseUntil = resume.Expires
			for _, v := range resume.After {
				it.searchAfter = append(it.searchAfter, v)
			}
		default:
			qctx, cancel := q.queryCtx(ctx)
			it.pitID, err = client.OpenPIT(qctx, plan.indices, plan.routing, q.cfg.StreamPITKeepAlive)
			cancel()
			it.leaseUntil = q.parserSvc.nowFn().Add(q.cfg.StreamPITKeepAlive)
			if err != nil {
				buildErrs = append(buildErrs, err)
				continue
			}
		}
		iters = append(iters, it)
		if len(clusterIDs) > 1 {
			it.hash = ""
		}
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
	if len(buildErrs) > 0 {
		return nil, buildErrs[0]
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

	done := &model.StreamDone{Returned: returned, DurationMs: elapsed, Truncated: truncated, Complete: !truncated, Status: "success", StopReason: streamStopReason(truncated)}
	if len(iters) == 1 && len(iters[0].committedAfter) > 0 {
		done.Cursor, err = iters[0].resumeToken(!truncated)
		if err != nil {
			return nil, err
		}
	}
	return done, nil
}
