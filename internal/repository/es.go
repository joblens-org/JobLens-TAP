package repository

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/joblens/tap/internal/cluster"
	"github.com/joblens/tap/internal/model"
)

// ESClient Elasticsearch 客户端封装
type ESClient struct {
	client      *elasticsearch.Client
	clusterName string
	esURL       string
}

// ClientManager 管理多个集群的 ES 客户端（惰性创建）
type ClientManager struct {
	mu         sync.RWMutex
	clients    map[string]*ESClient // cluster_name → client
	clusterMgr *cluster.Manager
}

// NewClientManager 创建客户端管理器
func NewClientManager(clusterMgr *cluster.Manager) *ClientManager {
	return &ClientManager{
		clients:    make(map[string]*ESClient),
		clusterMgr: clusterMgr,
	}
}

// GetClientForCluster 根据 clusterName 获取 ES 客户端和集群信息
// 返回: client, clusterInfo, error
func (m *ClientManager) GetClientForCluster(clusterName string) (*ESClient, *model.ClusterMeta, error) {
	// 1. 获取集群信息（触发惰性加载）
	info, ok := m.clusterMgr.Get(clusterName)
	if !ok {
		return nil, nil, fmt.Errorf("cluster not found: %s", clusterName)
	}

	// 2. 检查缓存
	m.mu.RLock()
	existing, exists := m.clients[clusterName]
	m.mu.RUnlock()

	// 已存在且 ES URL 未变化 → 直接返回
	if exists && existing.esURL == info.ESURL {
		return existing, info, nil
	}

	// 3. 创建新客户端（或替换）
	m.mu.Lock()
	defer m.mu.Unlock()

	// 双重检查
	if existing, exists := m.clients[clusterName]; exists && existing.esURL == info.ESURL {
		return existing, info, nil
	}

	client, err := newESClientFromInfo(info)
	if err != nil {
		return nil, nil, fmt.Errorf("create es client for cluster %s: %w", clusterName, err)
	}

	esClient := &ESClient{
		client:      client,
		clusterName: clusterName,
		esURL:       info.ESURL,
	}
	m.clients[clusterName] = esClient

	slog.Info("es client initialized (lazy)", "cluster", clusterName, "url", info.ESURL)

	return esClient, info, nil
}

// newESClientFromInfo 从 ClusterInfo 创建 ES 客户端
func newESClientFromInfo(info *model.ClusterMeta) (*elasticsearch.Client, error) {
	esCfg := elasticsearch.Config{
		Addresses: []string{info.ESURL},
		Transport: &http.Transport{
			MaxIdleConnsPerHost:   10,
			ResponseHeaderTimeout: 30 * time.Second,
			DialContext: (&net.Dialer{
				Timeout: 10 * time.Second,
			}).DialContext,
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}

	if info.ESUsername != "" && info.ESPassword != "" {
		esCfg.Username = info.ESUsername
		esCfg.Password = info.ESPassword
	}

	client, err := elasticsearch.NewClient(esCfg)
	if err != nil {
		return nil, err
	}

	// 测试连接
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.Info(client.Info.WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("connect to es: %w", err)
	}
	defer resp.Body.Close()

	if resp.IsError() {
		return nil, fmt.Errorf("es info error: %s", resp.String())
	}

	return client, nil
}

// GetAllClients 获取所有客户端（用于健康检查）
func (m *ClientManager) GetAllClients() map[string]*ESClient {
	m.mu.RLock()
	defer m.mu.RUnlock()
	// 浅拷贝
	result := make(map[string]*ESClient, len(m.clients))
	for k, v := range m.clients {
		result[k] = v
	}
	return result
}

// Ping 检查集群健康状态
func (c *ESClient) Ping(ctx context.Context) error {
	resp, err := c.client.Ping(c.client.Ping.WithContext(ctx))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.IsError() {
		return fmt.Errorf("es ping error: %s", resp.String())
	}
	return nil
}

// SearchResult ES 搜索结果
type SearchResult struct {
	Took         int64
	TimedOut     bool
	Total        int64
	MaxScore     float64
	Hits         []SearchHit
	Aggregations map[string]any
}

// SearchHit 单条搜索结果
type SearchHit struct {
	ID     string
	Index  string
	Score  float64
	Sort   []any
	Source map[string]any
}

// Search 执行 ES 搜索查询
func (c *ESClient) Search(ctx context.Context, indices []string, query map[string]any, routing string) (*SearchResult, error) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return nil, fmt.Errorf("encode query: %w", err)
	}

	queryBody := buf.String()
	querySizeBytes := buf.Len()
	slog.Debug("[Search] executing search",
		"cluster", c.clusterName,
		"indices_count", len(indices),
		"indices", indices,
		"query_size_bytes", querySizeBytes,
		"query_body", queryBody,
		"routing", routing,
	)

	opts := []func(*esapi.SearchRequest){
		c.client.Search.WithContext(ctx),
		c.client.Search.WithIndex(indices...),
		c.client.Search.WithBody(&buf),
		c.client.Search.WithTrackTotalHits(true),
		c.client.Search.WithAllowNoIndices(true),
		c.client.Search.WithIgnoreUnavailable(true),
	}
	if routing != "" {
		opts = append(opts, c.client.Search.WithRouting(routing))
	}

	resp, err := c.client.Search(opts...)
	if err != nil {
		slog.Debug("[Search] search request failed",
			"cluster", c.clusterName,
			"error", err,
			"indices", indices,
		)
		return nil, fmt.Errorf("execute search: %w", err)
	}
	defer resp.Body.Close()

	if resp.IsError() {
		var e map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&e); err != nil {
			return nil, fmt.Errorf("parse error response: %w", err)
		}
		slog.Debug("[Search] search response error",
			"cluster", c.clusterName,
			"error", e,
			"indices", indices,
		)
		return nil, fmt.Errorf("es search error: %v", e)
	}

	var r map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}

	// 打印原始响应摘要，便于排查空结果问题
	respPreview := fmt.Sprintf("keys=%v", mapKeys(r))
	if hits, ok := r["hits"].(map[string]any); ok {
		if total, ok := hits["total"].(map[string]any); ok {
			respPreview = fmt.Sprintf("total=%v hits=%d", total["value"], len(hits))
		}
	}
	slog.Debug("[Search] raw response received",
		"cluster", c.clusterName,
		"preview", respPreview,
	)

	result, parseErr := parseSearchResponse(r)
	if parseErr != nil {
		slog.Debug("[Search] parse response failed",
			"cluster", c.clusterName,
			"error", parseErr,
		)
		return nil, parseErr
	}

	slog.Debug("[Search] search completed",
		"cluster", c.clusterName,
		"took_ms", result.Took,
		"total_hits", result.Total,
		"returned_hits", len(result.Hits),
		"timed_out", result.TimedOut,
		"has_aggs", result.Aggregations != nil,
		"response_preview", fmt.Sprintf("took=%d total=%d hits=%d", result.Took, result.Total, len(result.Hits)),
	)

	return result, nil
}

// Count 执行 ES _count 查询，仅返回匹配文档数量，不返回文档体
// 用于轻量级存在性检查，对 ES 集群负担最小
func (c *ESClient) Count(ctx context.Context, indices []string, query map[string]any, routing string) (int64, error) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return 0, fmt.Errorf("encode query: %w", err)
	}

	slog.Debug("[Count] executing count",
		"cluster", c.clusterName,
		"indices_count", len(indices),
		"indices", indices,
		"routing", routing,
	)

	opts := []func(*esapi.CountRequest){
		c.client.Count.WithContext(ctx),
		c.client.Count.WithIndex(indices...),
		c.client.Count.WithBody(&buf),
		c.client.Count.WithAllowNoIndices(true),
		c.client.Count.WithIgnoreUnavailable(true),
	}
	if routing != "" {
		opts = append(opts, c.client.Count.WithRouting(routing))
	}

	resp, err := c.client.Count(opts...)
	if err != nil {
		slog.Debug("[Count] count request failed",
			"cluster", c.clusterName,
			"error", err,
		)
		return 0, fmt.Errorf("execute count: %w", err)
	}
	defer resp.Body.Close()

	if resp.IsError() {
		body, _ := io.ReadAll(resp.Body)
		slog.Debug("[Count] count response error",
			"cluster", c.clusterName,
			"status", resp.StatusCode,
			"body", string(body),
		)
		return 0, fmt.Errorf("es count error (status=%d): %s", resp.StatusCode, string(body))
	}

	var r map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return 0, fmt.Errorf("parse count response: %w", err)
	}

	var count int64
	if v, ok := r["count"].(float64); ok {
		count = int64(v)
	}

	slog.Debug("[Count] count completed",
		"cluster", c.clusterName,
		"count", count,
	)

	return count, nil
}

// parseSearchResponse 解析 ES 搜索响应
func parseSearchResponse(r map[string]any) (*SearchResult, error) {
	result := &SearchResult{}

	if took, ok := r["took"]; ok {
		if v, ok := took.(float64); ok {
			result.Took = int64(v)
		}
	}

	if timedOut, ok := r["timed_out"]; ok {
		if v, ok := timedOut.(bool); ok {
			result.TimedOut = v
		}
	}

	hits, ok := r["hits"].(map[string]any)
	if !ok {
		return result, nil
	}

	if total, ok := hits["total"].(map[string]any); ok {
		if v, ok := total["value"].(float64); ok {
			result.Total = int64(v)
		}
	}

	if maxScore, ok := hits["max_score"]; ok {
		if v, ok := maxScore.(float64); ok {
			result.MaxScore = v
		}
	}

	if hitList, ok := hits["hits"].([]any); ok {
		result.Hits = make([]SearchHit, 0, len(hitList))
		for _, h := range hitList {
			hit, ok := h.(map[string]any)
			if !ok {
				continue
			}
			sh := SearchHit{}
			if id, ok := hit["_id"].(string); ok {
				sh.ID = id
			}
			if idx, ok := hit["_index"].(string); ok {
				sh.Index = idx
			}
			if score, ok := hit["_score"].(float64); ok {
				sh.Score = score
			}
			if sort, ok := hit["sort"].([]any); ok {
				sh.Sort = sort
			}
			if source, ok := hit["_source"].(map[string]any); ok {
				sh.Source = source
			}
			result.Hits = append(result.Hits, sh)
		}
	}

	if aggs, ok := r["aggregations"].(map[string]any); ok {
		result.Aggregations = aggs
	}

	return result, nil
}

// mapKeys 返回 map 的所有 key（调试用）
func mapKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
