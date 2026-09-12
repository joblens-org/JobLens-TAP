package service

import (
	"context"
	"testing"

	"github.com/joblens/tap/internal/model"
)

func recAt(ts string) model.Record {
	return model.Record{Time: ts}
}

func collectEmitter(dst *[]model.Record) EmitFunc {
	return func(msg model.StreamMessage) error {
		if msg.Type != model.StreamTypeRecords {
			return nil
		}
		if sr, ok := msg.Data.(model.StreamRecords); ok {
			*dst = append(*dst, sr.Records.([]model.Record)...)
		}
		return nil
	}
}

func TestMergeRawStream_GlobalDescendingOrder(t *testing.T) {
	it1 := &rawStreamIter{done: true, buffer: []model.Record{recAt("2026-01-01T00:00:02Z"), recAt("2026-01-01T00:00:00Z")}}
	it2 := &rawStreamIter{done: true, buffer: []model.Record{recAt("2026-01-01T00:00:03Z"), recAt("2026-01-01T00:00:01Z")}}

	var got []model.Record
	returned, truncated, err := mergeRawStream(context.Background(), []*rawStreamIter{it1, it2}, 10, 100, collectEmitter(&got))
	if err != nil {
		t.Fatalf("mergeRawStream failed: %v", err)
	}
	if truncated {
		t.Error("did not expect truncation")
	}
	if returned != 4 || len(got) != 4 {
		t.Fatalf("returned=%d len=%d, want 4", returned, len(got))
	}
	want := []string{"2026-01-01T00:00:03Z", "2026-01-01T00:00:02Z", "2026-01-01T00:00:01Z", "2026-01-01T00:00:00Z"}
	for i := range want {
		if got[i].Time != want[i] {
			t.Fatalf("order[%d]=%s, want %s (full=%v)", i, got[i].Time, want[i], got)
		}
	}
}

func TestMergeRawStream_TruncatedAtMaxRecords(t *testing.T) {
	it1 := &rawStreamIter{done: true, buffer: []model.Record{recAt("2026-01-01T00:00:02Z"), recAt("2026-01-01T00:00:00Z")}}
	it2 := &rawStreamIter{done: true, buffer: []model.Record{recAt("2026-01-01T00:00:03Z"), recAt("2026-01-01T00:00:01Z")}}

	var got []model.Record
	returned, truncated, err := mergeRawStream(context.Background(), []*rawStreamIter{it1, it2}, 1, 2, collectEmitter(&got))
	if err != nil {
		t.Fatalf("mergeRawStream failed: %v", err)
	}
	if returned != 2 {
		t.Errorf("returned=%d, want 2", returned)
	}
	if !truncated {
		t.Error("expected truncation flag when hitting maxRecords with data remaining")
	}
	if got[0].Time != "2026-01-01T00:00:03Z" || got[1].Time != "2026-01-01T00:00:02Z" {
		t.Errorf("unexpected order: %v", got)
	}
}

func TestMergeRawStream_ContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	it := &rawStreamIter{done: true, buffer: []model.Record{recAt("2026-01-01T00:00:00Z")}}
	_, _, err := mergeRawStream(ctx, []*rawStreamIter{it}, 10, 100, func(model.StreamMessage) error { return nil })
	if err == nil {
		t.Error("expected context cancellation error")
	}
}
