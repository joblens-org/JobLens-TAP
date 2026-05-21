package tests

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/joblens/tap/internal/handler"
	"github.com/joblens/tap/internal/model"
)

func init() {
	gin.SetMode(gin.TestMode)
}

// setupRouter 创建测试路由
func setupRouter() *gin.Engine {
	initQueryService()
	router := gin.New()

	// 创建 handlers
	healthHandler := handler.NewHealthHandler(esManager, "test", "abc1234", "2026-01-01T00:00:00Z")
	rawHandler := handler.NewRawHandler(esManager, querySvc)
	timeseriesHandler := handler.NewTimeSeriesHandler(esManager, querySvc)
	summaryHandler := handler.NewSummaryHandler(esManager, querySvc)

	// 注册路由
	router.GET("/health", healthHandler.Health)
	router.GET("/ready", healthHandler.Ready)
	router.GET("/data/raw", rawHandler.Query)
	router.GET("/data/timeseries", timeseriesHandler.Query)
	router.GET("/data/summary", summaryHandler.Query)

	return router
}

// TestHealthHandler_Health 测试健康检查
func TestHealthHandler_Health(t *testing.T) {
	router := setupRouter()

	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	var resp model.Response
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to parse response: %v", err)
	}

	if resp.Code != 0 {
		t.Errorf("Expected code 0, got %d", resp.Code)
	}

	t.Logf("Health response: %s", w.Body.String())
}

// TestHealthHandler_Ready 测试就绪探针
func TestHealthHandler_Ready(t *testing.T) {
	skipIfShort(t)
	router := setupRouter()

	req := httptest.NewRequest("GET", "/ready", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	var resp model.Response
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to parse response: %v", err)
	}

	t.Logf("Ready response: %s", w.Body.String())
}

// TestRawHandler_Query 测试 Raw 接口
func TestRawHandler_Query(t *testing.T) {
	skipIfShort(t)
	skipIfNoJobID(t)
	router := setupRouter()

	url := "/data/raw?cluster=" + testCfg.ClusterID +
		"&job=" + testCfg.JobID +
		"&from=2026-03-28T00:00:00Z" +
		"&to=2026-03-28T23:59:59Z" +
		"&size=10"

	req := httptest.NewRequest("GET", url, nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d, body: %s", w.Code, w.Body.String())
	}

	var resp model.Response
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to parse response: %v", err)
	}

	if resp.Code != 0 {
		t.Errorf("Expected code 0, got %d, message: %s", resp.Code, resp.Message)
	}

	t.Logf("Raw query response: code=%d, message=%s", resp.Code, resp.Message)

	if data, ok := resp.Data.(map[string]any); ok {
		if records, ok := data["records"].([]any); ok {
			t.Logf("Returned %d records", len(records))
		}
		if pagination, ok := data["pagination"].(map[string]any); ok {
			t.Logf("Pagination: %+v", pagination)
		}
	}
}

// TestRawHandler_Query_WithFields 测试带字段的 Raw 接口
func TestRawHandler_Query_WithFields(t *testing.T) {
	skipIfShort(t)
	skipIfNoJobID(t)
	router := setupRouter()

	url := "/data/raw?cluster=" + testCfg.ClusterID +
		"&job=" + testCfg.JobID +
		"&from=2026-03-28T00:00:00Z" +
		"&to=2026-03-28T23:59:59Z" +
		"&fields=cpu,mem,host" +
		"&size=5"

	req := httptest.NewRequest("GET", url, nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	t.Logf("Raw query with fields response: %s", w.Body.String())
}

// TestRawHandler_Query_InvalidRequest 测试无效请求
func TestRawHandler_Query_InvalidRequest(t *testing.T) {
	router := setupRouter()

	tests := []struct {
		name   string
		url    string
		expect int
	}{
		{"missing cluster", "/data/raw?job=12345&from=now-1h", http.StatusBadRequest},
		{"missing job", "/data/raw?cluster=test&from=now-1h", http.StatusBadRequest},
		{"missing from", "/data/raw?cluster=test&job=12345", http.StatusBadRequest},
		{"invalid cluster", "/data/raw?cluster=nonexistent&job=12345&from=now-1h", http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", tt.url, nil)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)

			if w.Code != tt.expect {
				t.Errorf("Expected status %d, got %d, body: %s", tt.expect, w.Code, w.Body.String())
			}
		})
	}
}

// TestTimeSeriesHandler_Query 测试 Timeseries 接口
func TestTimeSeriesHandler_Query(t *testing.T) {
	skipIfShort(t)
	skipIfNoJobID(t)
	router := setupRouter()

	url := "/data/timeseries?cluster=" + testCfg.ClusterID +
		"&job=" + testCfg.JobID +
		"&metric=cpu" +
		"&interval=5m" +
		"&from=2026-03-28T00:00:00Z" +
		"&to=2026-03-28T23:59:59Z"

	req := httptest.NewRequest("GET", url, nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d, body: %s", w.Code, w.Body.String())
	}

	var resp model.Response
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to parse response: %v", err)
	}

	t.Logf("Timeseries response: code=%d", resp.Code)

	if data, ok := resp.Data.(map[string]any); ok {
		if metrics, ok := data["metrics"].([]any); ok {
			t.Logf("Metrics: %v", metrics)
		}
		if records, ok := data["records"].([]any); ok {
			t.Logf("Record count: %d", len(records))
		}
	}
}

// TestTimeSeriesHandler_Query_MultiMetric 测试多 metric
func TestTimeSeriesHandler_Query_MultiMetric(t *testing.T) {
	skipIfShort(t)
	skipIfNoJobID(t)
	router := setupRouter()

	url := "/data/timeseries?cluster=" + testCfg.ClusterID +
		"&job=" + testCfg.JobID +
		"&metric=cpu,mem" +
		"&interval=5m" +
		"&from=2026-03-28T00:00:00Z" +
		"&to=2026-03-28T23:59:59Z"

	req := httptest.NewRequest("GET", url, nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d, body: %s", w.Code, w.Body.String())
	}

	t.Logf("Timeseries multi-metric response: %s", w.Body.String())
}

// TestTimeSeriesHandler_Query_GroupByHost 测试按主机分组
func TestTimeSeriesHandler_Query_GroupByHost(t *testing.T) {
	skipIfShort(t)
	skipIfNoJobID(t)
	router := setupRouter()

	url := "/data/timeseries?cluster=" + testCfg.ClusterID +
		"&job=" + testCfg.JobID +
		"&metric=cpu" +
		"&interval=5m" +
		"&from=2026-03-28T00:00:00Z" +
		"&to=2026-03-28T23:59:59Z" +
		"&by=host"

	req := httptest.NewRequest("GET", url, nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d, body: %s", w.Code, w.Body.String())
	}

	t.Logf("Timeseries group by host response: %s", w.Body.String())
}

// TestTimeSeriesHandler_Query_InvalidRequest 测试无效请求
func TestTimeSeriesHandler_Query_InvalidRequest(t *testing.T) {
	router := setupRouter()

	tests := []struct {
		name   string
		url    string
		expect int
	}{
		{"missing metric", "/data/timeseries?cluster=test&job=12345&interval=1m&from=now-1h", http.StatusBadRequest},
		{"missing interval", "/data/timeseries?cluster=test&job=12345&metric=cpu&from=now-1h", http.StatusBadRequest},
		{"multi cluster", "/data/timeseries?cluster=test,test2&job=12345&metric=cpu&interval=1m&from=now-1h", http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", tt.url, nil)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)

			if w.Code != tt.expect {
				t.Errorf("Expected status %d, got %d, body: %s", tt.expect, w.Code, w.Body.String())
			}
		})
	}
}

// TestSummaryHandler_Query 测试 Summary 接口
func TestSummaryHandler_Query(t *testing.T) {
	skipIfShort(t)
	skipIfNoJobID(t)
	t.Skip("Skipping summary query test - actual data time range is not recent enough")
	router := setupRouter()

	url := "/data/summary?cluster=" + testCfg.ClusterID +
		"&job=" + testCfg.JobID

	req := httptest.NewRequest("GET", url, nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d, body: %s", w.Code, w.Body.String())
	}

	var resp model.Response
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to parse response: %v", err)
	}

	t.Logf("Summary response: code=%d", resp.Code)

	if data, ok := resp.Data.(map[string]any); ok {
		if job, ok := data["job"]; ok {
			t.Logf("Job: %v", job)
		}
		if time, ok := data["time"].(map[string]any); ok {
			t.Logf("Time: %+v", time)
		}
		if stats, ok := data["stats"].(map[string]any); ok {
			t.Logf("Stats collectors: %v", getKeys(stats))
		}
	}
}

// TestSummaryHandler_Query_WithCollectors 测试指定采集器
func TestSummaryHandler_Query_WithCollectors(t *testing.T) {
	skipIfShort(t)
	skipIfNoJobID(t)
	t.Skip("Skipping summary query test - actual data time range is not recent enough")
	router := setupRouter()

	url := "/data/summary?cluster=" + testCfg.ClusterID +
		"&job=" + testCfg.JobID +
		"&collectors=cpumem_collector"

	req := httptest.NewRequest("GET", url, nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d, body: %s", w.Code, w.Body.String())
	}

	t.Logf("Summary with collectors response: %s", w.Body.String())
}

// TestSummaryHandler_Query_InvalidRequest 测试无效请求
func TestSummaryHandler_Query_InvalidRequest(t *testing.T) {
	router := setupRouter()

	tests := []struct {
		name   string
		url    string
		expect int
	}{
		{"missing cluster", "/data/summary?job=12345", http.StatusBadRequest},
		{"missing job", "/data/summary?cluster=test", http.StatusBadRequest},
		{"multi cluster", "/data/summary?cluster=test,test2&job=12345", http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", tt.url, nil)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)

			if w.Code != tt.expect {
				t.Errorf("Expected status %d, got %d, body: %s", tt.expect, w.Code, w.Body.String())
			}
		})
	}
}

// getKeys 获取 map 的键列表
func getKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
