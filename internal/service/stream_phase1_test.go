package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/joblens/tap/internal/model"
)

func TestStreamWindowPlanRejectsExcessBeforeAllocation(t *testing.T) {
	// 给定远超窗口预算的请求。
	from := time.Unix(0, 0).UTC()
	// 当规划窗口时，应在分配前拒绝。
	_, err := planTimeWindows(from, from.Add(time.Hour), time.Millisecond, 1, 2)
	var qe *QueryError
	if !errors.As(err, &qe) || qe.Kind != ErrKindTooManyWindows {
		t.Fatalf("期望窗口预算错误，实际 %v", err)
	}
}

func TestStreamWindowPlanOwnsAlignedBuckets(t *testing.T) {
	// 给定不在分钟网格上的首尾时间。
	from := time.Unix(30, 0).UTC()
	to := time.Unix(150, 0).UTC()
	windows, err := planTimeWindows(from, to, time.Minute, 2, 10)
	// 当按两个桶一窗规划，接缝必须为120秒而不是150秒。
	if err != nil || len(windows) != 2 {
		t.Fatalf("窗口 %v，错误 %v", windows, err)
	}
	if !windows[0].to.Equal(time.Unix(120, 0)) || windows[0].inclusive || !windows[1].inclusive {
		t.Fatalf("错误的桶归属：%+v", windows)
	}
}

func TestParseIntervalRejectsUnsupportedPrecision(t *testing.T) {
	for _, input := range []string{"1ns", "1us", "0s", "999999999999999999999w"} {
		t.Run(input, func(t *testing.T) {
			// 当输入不能无损转成ES固定间隔时，必须拒绝。
			if _, err := NewParserService().ParseInterval(input); err == nil {
				t.Fatal("接受了非法间隔")
			}
		})
	}
}

func TestMergeRawStreamHonorsPartialPageLimit(t *testing.T) {
	// 给定一页大于请求记录上限。
	it := &rawStreamIter{done: true, buffer: []model.Record{{Time: "2026-01-01T00:00:03Z"}, {Time: "2026-01-01T00:00:02Z"}, {Time: "2026-01-01T00:00:01Z"}}}
	returned, truncated, err := mergeRawStream(context.Background(), []*rawStreamIter{it}, 10, 1, func(model.StreamMessage) error { return nil })
	// 当输出时不得整页超发。
	if err != nil || returned != 1 || !truncated {
		t.Fatalf("returned=%d truncated=%v err=%v", returned, truncated, err)
	}
}
