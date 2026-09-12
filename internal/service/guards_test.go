package service

import (
	"container/heap"
	"testing"
	"time"

	"github.com/joblens/tap/internal/model"
)

func TestParseInterval(t *testing.T) {
	p := NewParserService()
	tests := []struct {
		input   string
		want    time.Duration
		wantErr bool
	}{
		{"1m", time.Minute, false},
		{"30s", 30 * time.Second, false},
		{"500ms", 500 * time.Millisecond, false},
		{"2h", 2 * time.Hour, false},
		{"1d", 24 * time.Hour, false},
		{"1w", 7 * 24 * time.Hour, false},
		{"0s", 0, true},
		{"", 0, true},
		{"abc", 0, true},
	}
	for _, tt := range tests {
		got, err := p.ParseInterval(tt.input)
		if tt.wantErr {
			if err == nil {
				t.Errorf("ParseInterval(%q) expected error", tt.input)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseInterval(%q) unexpected error: %v", tt.input, err)
			continue
		}
		if got != tt.want {
			t.Errorf("ParseInterval(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestDateBucketCount(t *testing.T) {
	from := time.Unix(0, 0)
	to := from.Add(10 * time.Minute)
	if got := dateBucketCount(from, to, time.Minute); got != 11 {
		t.Errorf("dateBucketCount = %d, want 11", got)
	}
	if got := dateBucketCount(from, to, 0); got != 0 {
		t.Errorf("dateBucketCount with zero interval = %d, want 0", got)
	}
	if got := dateBucketCount(to, from, time.Minute); got != 0 {
		t.Errorf("dateBucketCount reversed = %d, want 0", got)
	}
}

func TestEstimateBuckets(t *testing.T) {
	if got := estimateBuckets(10, 2, false); got != 30 {
		t.Errorf("estimateBuckets ungrouped = %d, want 30", got)
	}
	if got := estimateBuckets(10, 2, true); got != 3000 {
		t.Errorf("estimateBuckets grouped = %d, want 3000", got)
	}
}

func TestWindowDateBuckets(t *testing.T) {
	if got := windowDateBuckets(100, 10, 1, false); got != 10 {
		t.Errorf("windowDateBuckets ungrouped = %d, want 10", got)
	}
	if got := windowDateBuckets(100, 10, 1, true); got != 1 {
		t.Errorf("windowDateBuckets grouped = %d, want 1", got)
	}
	if got := windowDateBuckets(0, 10, 1, false); got != 1 {
		t.Errorf("windowDateBuckets zero max = %d, want 1", got)
	}
}

func TestSplitTimeWindows(t *testing.T) {
	from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	to := from.Add(10 * time.Minute)
	windows := splitTimeWindows(from, to, 3*time.Minute)
	if len(windows) != 4 {
		t.Fatalf("splitTimeWindows count = %d, want 4", len(windows))
	}
	if !windows[0].from.Equal(from) || !windows[0].to.Equal(from.Add(3*time.Minute)) {
		t.Errorf("window[0] = %v..%v", windows[0].from, windows[0].to)
	}
	last := windows[len(windows)-1]
	if !last.to.Equal(to) {
		t.Errorf("last window end = %v, want %v", last.to, to)
	}
}

func TestTSAccumulator(t *testing.T) {
	a := &tsAccumulator{}
	a.add(2)
	a.add(4)
	a.add(0)
	st := a.stats()
	if st.GlobalMax != 4 {
		t.Errorf("GlobalMax = %v, want 4", st.GlobalMax)
	}
	if st.GlobalAvg != 2 {
		t.Errorf("GlobalAvg = %v, want 2", st.GlobalAvg)
	}
	empty := (&tsAccumulator{}).stats()
	if empty.GlobalAvg != 0 || empty.GlobalMax != 0 {
		t.Errorf("empty accumulator = %+v", empty)
	}
}

func TestRawIterHeapOrdering(t *testing.T) {
	mk := func(ts string) *rawStreamIter {
		return &rawStreamIter{buffer: []model.Record{{Time: ts}}}
	}
	h := &rawIterHeap{}
	heap.Init(h)
	heap.Push(h, mk("2026-01-01T00:00:00Z"))
	heap.Push(h, mk("2026-01-01T00:00:02Z"))
	heap.Push(h, mk("2026-01-01T00:00:01Z"))

	var got []string
	for h.Len() > 0 {
		it := heap.Pop(h).(*rawStreamIter)
		got = append(got, it.head().Time)
	}
	want := []string{"2026-01-01T00:00:02Z", "2026-01-01T00:00:01Z", "2026-01-01T00:00:00Z"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("heap order = %v, want %v", got, want)
		}
	}
}
