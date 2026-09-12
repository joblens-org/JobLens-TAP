package service

import (
	"slices"
	"testing"
	"time"

	"github.com/joblens/tap/internal/config"
	"github.com/joblens/tap/internal/model"
)

func TestResolveIndicesUTC_whenOffsetsDiffer(t *testing.T) {
	for _, tc := range []struct {
		name, from, to string
		days           []string
	}{
		{"reported", "2026-09-12T02:00:00+00:00", "2026-09-12T11:00:00+08:00", []string{"2026.09.12"}},
		{"same_instants_utc", "2026-09-12T02:00:00Z", "2026-09-12T03:00:00Z", []string{"2026.09.12"}},
		{"same_instants_east", "2026-09-12T10:00:00+08:00", "2026-09-12T11:00:00+08:00", []string{"2026.09.12"}},
		{"east_midnight", "2026-09-12T23:30:00+08:00", "2026-09-13T00:30:00+08:00", []string{"2026.09.12"}},
		{"utc_midnight", "2026-09-13T07:30:00+08:00", "2026-09-13T08:30:00+08:00", []string{"2026.09.12", "2026.09.13"}},
		{"instant", "2026-09-13T00:00:00+08:00", "2026-09-12T16:00:00Z", []string{"2026.09.12"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// 给定：同一 writer UTC 日约定与显式偏移输入。
			from, err := time.Parse(time.RFC3339, tc.from)
			if err != nil {
				t.Fatal(err)
			}
			to, err := time.Parse(time.RFC3339, tc.to)
			if err != nil {
				t.Fatal(err)
			}
			svc := NewIndexService(&config.Config{Registry: model.BuildDefaultRegistry()}, nil)
			want := make([]string, len(tc.days))
			for i, day := range tc.days {
				want[i] = "cpumem_collector_" + day
			}
			// 当：经公共解析入口选择索引。
			got, err := svc.ResolveIndices("cpumem", from, to, nil)
			// 则：只枚举覆盖采样时刻的 UTC 日。
			if err != nil || !slices.Equal(got, want) {
				t.Fatalf("indices=%v error=%v want=%v", got, err, want)
			}
		})
	}
}

func TestResolveIndicesRejectsEmpty_whenRangeReversedOrCollectorsAbsent(t *testing.T) {
	for _, tc := range []struct {
		name, collector string
		from, to        time.Time
	}{
		{"reversed_same_day", "cpumem", time.Unix(3600, 0), time.Unix(0, 0)},
		{"no_collectors", "", time.Unix(0, 0), time.Unix(3600, 0)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// 给定：无法形成有效索引范围的输入。
			svc := NewIndexService(&config.Config{Registry: model.BuildDefaultRegistry()}, nil)
			// 当：解析索引。
			got, err := svc.ResolveIndices(tc.collector, tc.from, tc.to, nil)
			// 则：显式报错，不能把空列表交给 ES 当成全索引。
			if err == nil {
				t.Fatalf("indices=%v error=nil; want explicit error", got)
			}
		})
	}
}
