package service

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// ServiceRegistryClient 服务注册中心客户端
type ServiceRegistryClient struct {
	baseURL string
	client  *http.Client
}

// NewServiceRegistryClient 创建服务注册中心客户端
func NewServiceRegistryClient(baseURL string, timeout time.Duration) *ServiceRegistryClient {
	return &ServiceRegistryClient{
		baseURL: baseURL,
		client: &http.Client{
			Timeout: timeout,
		},
	}
}

// NodeInfo 注册中心返回的节点信息
type NodeInfo struct {
	NodeName string `json:"node_name"`
	BaseURL  string `json:"base_url"`
	Status   string `json:"status"`
}

// GetNodeURLWithFallback 根据节点名称查询节点的 base_url，失败时使用默认 URL
func (c *ServiceRegistryClient) GetNodeURLWithFallback(nodeName string, defaultPort int) (string, error) {
	// 1. 尝试从注册中心查询
	nodeBaseURL, err := c.GetNodeURL(nodeName)
	if err == nil {
		return nodeBaseURL, nil
	}

	// 2. 注册中心查询失败，使用默认 URL
	defaultURL := fmt.Sprintf("http://%s:%d", nodeName, defaultPort)

	return defaultURL, nil
}

// GetNodeURL 根据节点名称查询节点的 base_url（原始方法）
func (c *ServiceRegistryClient) GetNodeURL(nodeName string) (string, error) {
	if c.baseURL == "" {
		return "", fmt.Errorf("service registry URL not configured")
	}

	// 使用正确的 API 端点
	url := fmt.Sprintf("%s/services/by-host/%s", c.baseURL, nodeName)

	resp, err := c.client.Get(url)
	if err != nil {
		return "", fmt.Errorf("query service registry failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("service registry returned status %d: %s", resp.StatusCode, string(body))
	}

	// 解析服务列表
	var serviceList []ServiceInfo
	if err := json.NewDecoder(resp.Body).Decode(&serviceList); err != nil {
		return "", fmt.Errorf("parse service list failed: %w", err)
	}

	// 找到第一个 healthy 状态的服务
	for _, service := range serviceList {
		if service.Status == "healthy" && service.BaseURL != "" {
			return service.BaseURL, nil
		}
	}

	// 如果没有 healthy 的服务，返回第一个可用的服务
	if len(serviceList) > 0 && serviceList[0].BaseURL != "" {
		return serviceList[0].BaseURL, nil
	}

	return "", fmt.Errorf("no valid service found for node: %s", nodeName)
}

// ServiceInfo 服务信息模型（对应注册中心返回的结构）
type ServiceInfo struct {
	ServiceID     string                 `json:"service_id"`
	Host          string                 `json:"host"`
	Port          int                    `json:"port"`
	BaseURL       string                 `json:"base_url"`
	Name          *string                `json:"name,omitempty"`
	Version       *string                `json:"version,omitempty"`
	Mode          string                 `json:"mode"`
	Role          string                 `json:"role"`
	RegisteredAt  string                 `json:"registered_at"`
	LastHeartbeat string                 `json:"last_heartbeat"`
	Status        string                 `json:"status"`
	DirectoryPath string                 `json:"directory_path"`
	Metadata      map[string]interface{} `json:"metadata,omitempty"`
}
