package tests

import (
	"context"
	"testing"
	"time"
)

// TestClientManager_GetClient 测试获取客户端
func TestClientManager_GetClient(t *testing.T) {
	skipIfShort(t)

	// 测试获取存在的客户端
	client, _, err := esManager.GetClientForCluster(testCfg.ClusterID)
	if err != nil {
		t.Fatalf("Expected to get client for cluster %s, got error: %v", testCfg.ClusterID, err)
	}
	if client == nil {
		t.Fatal("Expected non-nil client")
	}

	// 测试获取不存在的客户端
	_, _, err = esManager.GetClientForCluster("nonexistent")
	if err == nil {
		t.Error("Expected error for nonexistent cluster")
	}
}

// TestClientManager_GetAllClients 测试获取所有客户端
func TestClientManager_GetAllClients(t *testing.T) {
	skipIfShort(t)

	clients := esManager.GetAllClients()
	// 注意：集群管理器可能为空（没有管理 API），此时客户端集合为空
	if len(clients) > 0 {
		if _, ok := clients[testCfg.ClusterID]; !ok {
			t.Logf("Client for cluster %s not found in GetAllClients (may need explicit GetClientForCluster first)", testCfg.ClusterID)
		}
	}
}

// TestESClient_Ping 测试连接健康检查
func TestESClient_Ping(t *testing.T) {
	skipIfShort(t)

	client, _, err := esManager.GetClientForCluster(testCfg.ClusterID)
	if err != nil {
		t.Fatalf("Failed to get client for cluster %s: %v", testCfg.ClusterID, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx); err != nil {
		t.Errorf("Ping failed: %v", err)
	}
}

// TestESClient_Search 测试搜索功能
func TestESClient_Search(t *testing.T) {
	skipIfShort(t)
	skipIfNoJobID(t)

	client, _, err := esManager.GetClientForCluster(testCfg.ClusterID)
	if err != nil {
		t.Fatalf("Failed to get client for cluster %s: %v", testCfg.ClusterID, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 构建简单查询
	query := map[string]any{
		"query": map[string]any{
			"match_all": map[string]any{},
		},
		"size": 1,
	}

	// 使用通配符索引
	indices := []string{"collector_*"}

	result, err := client.Search(ctx, indices, query, "")
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if result == nil {
		t.Fatal("Expected non-nil result")
	}

	t.Logf("Search returned %d total hits", result.Total)
}

// TestESClient_Search_WithFilter 测试带过滤条件的搜索
func TestESClient_Search_WithFilter(t *testing.T) {
	skipIfShort(t)
	skipIfNoJobID(t)

	client, _, err := esManager.GetClientForCluster(testCfg.ClusterID)
	if err != nil {
		t.Fatalf("Failed to get client for cluster %s: %v", testCfg.ClusterID, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 构建 JobID 过滤查询
	query := map[string]any{
		"query": map[string]any{
			"bool": map[string]any{
				"filter": []map[string]any{
					{
						"term": map[string]any{
							"job_info.JobID": testCfg.JobID,
						},
					},
				},
			},
		},
		"size": 10,
		"sort": []map[string]any{
			{"@timestamp": "desc"},
		},
	}

	// 使用通配符索引
	indices := []string{"collector_*"}

	result, err := client.Search(ctx, indices, query, "")
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	t.Logf("Search with JobID filter returned %d hits", result.Total)

	// 验证结果中的文档
	for i, hit := range result.Hits {
		t.Logf("Hit %d: index=%s, id=%s", i, hit.Index, hit.ID)
		if hit.Source != nil {
			t.Logf("  Source keys: %v", getKeys(hit.Source))
		}
	}
}

// TestESClient_Search_Aggregation 测试聚合查询
func TestESClient_Search_Aggregation(t *testing.T) {
	skipIfShort(t)
	skipIfNoJobID(t)

	client, _, err := esManager.GetClientForCluster(testCfg.ClusterID)
	if err != nil {
		t.Fatalf("Failed to get client for cluster %s: %v", testCfg.ClusterID, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 构建聚合查询
	query := map[string]any{
		"query": map[string]any{
			"bool": map[string]any{
				"filter": []map[string]any{
					{
						"term": map[string]any{
							"job_info.JobID": testCfg.JobID,
						},
					},
				},
			},
		},
		"size": 0,
		"aggs": map[string]any{
			"collectors": map[string]any{
				"terms": map[string]any{
					"field": "collector",
					"size":  10,
				},
			},
			"first_seen": map[string]any{
				"min": map[string]any{
					"field": "@timestamp",
				},
			},
			"last_seen": map[string]any{
				"max": map[string]any{
					"field": "@timestamp",
				},
			},
		},
	}

	indices := []string{"collector_*"}

	result, err := client.Search(ctx, indices, query, "")
	if err != nil {
		t.Fatalf("Search with aggregation failed: %v", err)
	}

	t.Logf("Aggregation query took %d ms", result.Took)

	// 验证聚合结果
	if result.Aggregations != nil {
		if collectors, ok := result.Aggregations["collectors"].(map[string]any); ok {
			if buckets, ok := collectors["buckets"].([]any); ok {
				t.Logf("Found %d collectors", len(buckets))
				for _, b := range buckets {
					if bucket, ok := b.(map[string]any); ok {
						t.Logf("  - %v: %v docs", bucket["key"], bucket["doc_count"])
					}
				}
			}
		}
	}
}

// TestESClient_Search_EmptyResult 测试空结果
func TestESClient_Search_EmptyResult(t *testing.T) {
	skipIfShort(t)

	client, _, err := esManager.GetClientForCluster(testCfg.ClusterID)
	if err != nil {
		t.Fatalf("Failed to get client for cluster %s: %v", testCfg.ClusterID, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 使用不存在的 JobID
	query := map[string]any{
		"query": map[string]any{
			"bool": map[string]any{
				"filter": []map[string]any{
					{
						"term": map[string]any{
							"job_info.JobID": -999999,
						},
					},
				},
			},
		},
		"size": 10,
	}

	indices := []string{"collector_*"}

	result, err := client.Search(ctx, indices, query, "")
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if result.Total != 0 {
		t.Errorf("Expected 0 results, got %d", result.Total)
	}

	if len(result.Hits) != 0 {
		t.Errorf("Expected 0 hits, got %d", len(result.Hits))
	}
}
