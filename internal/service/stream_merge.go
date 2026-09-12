package service

import (
	"container/heap"
	"context"
	"time"

	"github.com/joblens/tap/internal/model"
	"github.com/joblens/tap/internal/repository"
)

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
	pitID           string
	sorts           [][]any
	committedAfter  []any
	hash            string
	signer          cursorSigner
	leaseUntil      time.Time
	resumable       bool
}

func (it *rawStreamIter) hasHead() bool {
	return it.pos < len(it.buffer)
}

func (it *rawStreamIter) head() model.Record {
	return it.buffer[it.pos]
}

func (it *rawStreamIter) advance() {
	if it.pos < len(it.sorts) {
		it.committedAfter = it.sorts[it.pos]
	}
	it.pos++
	if it.pos >= len(it.buffer) {
		it.committedCursor = it.nextCursor
	}
}

func (it *rawStreamIter) ensure(ctx context.Context) error {
	if it.pitID != "" {
		return it.ensurePIT(ctx)
	}
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

func mergeRawStream(ctx context.Context, iters []*rawStreamIter, pageSize, maxRecords int, emit EmitFunc) (int, bool, error) {
	h := &rawIterHeap{}
	heap.Init(h)
	for _, it := range iters {
		if err := it.ensure(ctx); err != nil {
			return 0, false, err
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
			if iters[0].pitID != "" {
				id, err := iters[0].resumeToken(false)
				if err != nil {
					return err
				}
				msg.ID = id
			} else {
				msg.ID = iters[0].committedCursor
			}
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
			if flushErr := flush(); flushErr != nil {
				return returned, truncated, flushErr
			}
			return returned, false, err
		} else if it.hasHead() {
			heap.Fix(h, 0)
		} else {
			heap.Pop(h)
		}

		if len(batch) >= pageSize || returned+len(batch) >= maxRecords {
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
