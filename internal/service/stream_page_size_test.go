package service

import (
	"testing"

	"github.com/joblens/tap/internal/model"
)

func TestResolveStreamPageSize(t *testing.T) {
	_, ssvc := newQueryServiceForTest(t, "http://127.0.0.1:1")

	cases := []struct {
		name      string
		collector string
		pageSize  string
		want      int
		wantErr   bool
	}{
		{name: "auto cpumem", collector: "cpumem", pageSize: "auto", want: 2000},
		{name: "empty same as auto", collector: "cpumem", pageSize: "", want: 2000},
		{name: "explicit value honored", collector: "cpumem", pageSize: "512", want: 512},
		{name: "auto wide collector", collector: "fs_metadata", pageSize: "auto", want: 4},
		{name: "auto multi takes min", collector: "cpumem,fs_metadata", pageSize: "auto", want: 4},
		{name: "unknown collector falls back", collector: "unknown", pageSize: "auto", want: 500},
		{name: "no collector falls back", collector: "", pageSize: "auto", want: 500},
		{name: "invalid text", collector: "cpumem", pageSize: "abc", wantErr: true},
		{name: "zero rejected", collector: "cpumem", pageSize: "0", wantErr: true},
		{name: "negative rejected", collector: "cpumem", pageSize: "-1", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := &model.RawStreamRequest{Collector: tc.collector, PageSize: tc.pageSize}
			got, err := ssvc.resolveStreamPageSize(req)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("want error, got %d", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("got %d, want %d", got, tc.want)
			}
		})
	}
}
