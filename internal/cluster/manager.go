// Package cluster 管理集群元数据（惰性加载 + 缓存）
package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/joblens/tap/internal/model"
)

// Manager 集群元数据管理器
type Manager struct {
	apiURL      string
	cacheTTL    time.Duration
	minFetchGap time.Duration
	httpClient  *http.Client

	mu        sync.RWMutex
	clusters  map[string]*model.ClusterMeta
	lastFetch time.Time
	fetching  bool
	fetchCond *sync.Cond
}

// NewManager 创建集群管理器
func NewManager(apiURL string, cacheTTL time.Duration) *Manager {
	mgr := &Manager{
		apiURL:      apiURL,
		cacheTTL:    cacheTTL,
		minFetchGap: 30 * time.Second,
		httpClient:  &http.Client{Timeout: 10 * time.Second},
		clusters:    make(map[string]*model.ClusterMeta),
	}
	mgr.fetchCond = sync.NewCond(&mgr.mu)
	return mgr
}

// InitialFetch 启动时首次拉取（阻塞）
func (m *Manager) InitialFetch(ctx context.Context) error {
	return m.doFetch(ctx)
}

// BackgroundRefresh 后台定时刷新
func (m *Manager) BackgroundRefresh() {
	ticker := time.NewTicker(m.cacheTTL)
	defer ticker.Stop()
	for range ticker.C {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := m.doFetch(ctx); err != nil {
			slog.Warn("cluster manager background refresh failed", "error", err)
		}
		cancel()
	}
}

// Get 获取单个集群信息（触发惰性加载）
func (m *Manager) Get(name string) (*model.ClusterMeta, bool) {
	m.mu.RLock()
	info, ok := m.clusters[name]
	isStale := time.Since(m.lastFetch) > m.cacheTTL
	canRefetch := time.Since(m.lastFetch) > m.minFetchGap
	m.mu.RUnlock()

	if ok && info.Enabled && !isStale {
		return info, true
	}

	if ok && info.Enabled && isStale {
		go m.safeFetch()
		return info, true
	}

	if canRefetch {
		m.mu.Lock()
		if m.fetching {
			for m.fetching {
				m.fetchCond.Wait()
			}
			m.mu.Unlock()
			return m.lookup(name)
		}
		m.fetching = true
		m.mu.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := m.doFetch(ctx)
		cancel()

		m.mu.Lock()
		m.fetching = false
		m.fetchCond.Broadcast()
		m.mu.Unlock()

		if err != nil {
			slog.Warn("cluster manager lazy fetch failed", "error", err)
		}
	}

	return m.lookup(name)
}

func (m *Manager) lookup(name string) (*model.ClusterMeta, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	info, ok := m.clusters[name]
	if !ok || !info.Enabled {
		// 未命中 Name，尝试别名匹配
		return m.findByAliasLocked(name)
	}
	return info, true
}

// findByAliasLocked 按别名查找集群（调用方需持有 RLock）
func (m *Manager) findByAliasLocked(alias string) (*model.ClusterMeta, bool) {
	for _, info := range m.clusters {
		if info.Alias != "" && info.Alias == alias && info.Enabled {
			return info, true
		}
	}
	return nil, false
}

// GetAll 获取全部启用的集群
func (m *Manager) GetAll() []*model.ClusterMeta {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var result []*model.ClusterMeta
	for _, info := range m.clusters {
		if info.Enabled {
			result = append(result, info)
		}
	}
	return result
}

// GetAllNames 获取全部启用的集群名
func (m *Manager) GetAllNames() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var names []string
	for _, info := range m.clusters {
		if info.Enabled {
			names = append(names, info.Name)
		}
	}
	return names
}

func (m *Manager) safeFetch() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := m.doFetch(ctx); err != nil {
		slog.Warn("cluster manager background refresh failed", "error", err)
	}
}

func (m *Manager) doFetch(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, m.apiURL+"/api/clusters/scheme", nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := m.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("fetch clusters: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
	}

	var apiResp model.ManagementAPIResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}

	newClusters := make(map[string]*model.ClusterMeta, len(apiResp.Clusters))
	for i := range apiResp.Clusters {
		info := &apiResp.Clusters[i]
		info.NormalizeExtra()
		if info.ESURL == "" {
			slog.Warn("cluster has no es_url, skipping", "cluster_name", info.Name)
			continue
		}
		newClusters[info.Name] = info
	}

	m.mu.Lock()
	m.clusters = newClusters
	m.lastFetch = time.Now()
	m.mu.Unlock()

	slog.Info("cluster manager cache refreshed",
		"total", apiResp.Total,
		"valid", len(newClusters),
	)

	return nil
}
