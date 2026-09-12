package repository

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"
)

func TestOpenPITMissingPolicy_whenIndicesUnavailable(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
	}{
		{"missing_today_with_history", 200}, {"all_missing", 404},
		{"unauthorized", 401}, {"forbidden", 403}, {"unavailable_shards", 503},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// 给定：识别 PIT 查询参数的真实 HTTP ES 替身。
			client := pitClient(t, func(w http.ResponseWriter, r *http.Request) {
				q := r.URL.Query()
				if q.Has("allow_no_indices") || q.Get("allow_partial_search_results") != "false" {
					t.Errorf("invalid PIT policy: %s", r.URL.RawQuery)
					w.WriteHeader(400)
					return
				}
				status := tc.status
				if status == 200 && q.Get("ignore_unavailable") != "true" {
					status = 404
				}
				w.WriteHeader(status)
				if status == 200 {
					if _, err := w.Write([]byte(`{"id":"history-pit"}`)); err != nil {
						t.Error(err)
					}
				}
			})
			// 当：打开包含缺失日期的 PIT。
			id, err := client.OpenPIT(context.Background(), []string{"history", "today"}, "", time.Minute)
			// 则：仅部分索引缺失可以成功；其余 ES 错误原样保留状态。
			if tc.status == 200 {
				if err != nil || id != "history-pit" {
					t.Fatalf("id=%q error=%v", id, err)
				}
				return
			}
			var backend *SearchError
			if !errors.As(err, &backend) || backend.Status != tc.status {
				t.Fatalf("error=%v want status=%d", err, tc.status)
			}
		})
	}
}
