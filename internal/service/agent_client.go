package service

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// AgentClient Agent 客户端
type AgentClient struct {
	client          *http.Client
	initialDelay    time.Duration
	maxAttempts     int
	retryMultiplier float64
}

// NewAgentClient 创建 Agent 客户端
func NewAgentClient(initialDelay time.Duration, maxAttempts int, retryMultiplier float64) *AgentClient {
	return &AgentClient{
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
		initialDelay:    initialDelay,
		maxAttempts:     maxAttempts,
		retryMultiplier: retryMultiplier,
	}
}

// parseJobIDToUint64 将原生 JobID 字符串转换为 uint64
// "172.0" → 1720   "67890" → 67890
func parseJobIDToUint64(jobID string) (uint64, error) {
	// 去掉小数点，将两边拼接
	cleaned := strings.ReplaceAll(jobID, ".", "")
	numericID, err := strconv.ParseUint(cleaned, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid job id %q: %w", jobID, err)
	}
	slog.Debug("[parseJobIDToUint64] converted", "input", jobID, "output", numericID)
	return numericID, nil
}

// parseCondorJobID 解析 Condor JobID 为 cluster_id 和 proc_id
// "172.0" → clusterID=172, procID=0
func parseCondorJobID(jobID string) (clusterID, procID uint64, err error) {
	parts := strings.SplitN(jobID, ".", 2)
	if len(parts) < 2 {
		return 0, 0, fmt.Errorf("invalid condor job id %q: expected format \"cluster.proc\"", jobID)
	}
	clusterID, err = strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid condor cluster_id in %q: %w", jobID, err)
	}
	procID, err = strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid condor proc_id in %q: %w", jobID, err)
	}
	return clusterID, procID, nil
}

// parseSlurmJobID 解析 Slurm JobID 为 job_id 和 step_id
// "67890" → jobID=67890, stepID=0
// "67890.0" → jobID=67890, stepID=0
func parseSlurmJobID(jobID string) (jobIDVal, stepID uint64, err error) {
	parts := strings.SplitN(jobID, ".", 2)
	jobIDVal, err = strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid slurm job_id in %q: %w", jobID, err)
	}
	if len(parts) == 2 {
		stepID, err = strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			return 0, 0, fmt.Errorf("invalid slurm step_id in %q: %w", jobID, err)
		}
	}
	return jobIDVal, stepID, nil
}

// TriggerAgentCollection 向节点 agent 发送采集请求（带重试）
func (c *AgentClient) TriggerAgentCollection(nodeBaseURL string, clusterType string, clusterTag string, jobID string, slot string, collectors []string, jobInfo any) (any, error) {
	slog.Debug("[TriggerAgentCollection] entry",
		"node_base_url", nodeBaseURL,
		"cluster_type", clusterType,
		"cluster_tag", clusterTag,
		"job_id", jobID,
		"slot", slot,
		"collectors", collectors,
	)

	// 字符串 JobID → uint64
	numericJobID, err := parseJobIDToUint64(jobID)
	if err != nil {
		return nil, err
	}

	// 根据集群类型构建请求体和 URL
	var agentURL string
	var requestBody map[string]any

	switch clusterType {
	case "condor":
		cid, proc, err := parseCondorJobID(jobID)
		if err != nil {
			return nil, err
		}
		agentURL = fmt.Sprintf("%s/joblens/condor_job", nodeBaseURL)
		requestBody = map[string]any{
			"opt":   "add",
			"JobID": numericJobID,
			"slot":  slot,
			"Lens":  collectors,
			"sub_attr": map[string]any{
				"cluster_id": cid,
				"proc_id":    proc,
			},
		}
	case "slurm":
		jid, sid, err := parseSlurmJobID(jobID)
		if err != nil {
			return nil, err
		}
		agentURL = fmt.Sprintf("%s/joblens/slurm_job", nodeBaseURL)
		requestBody = map[string]any{
			"opt":   "add",
			"JobID": numericJobID,
			"Lens":  collectors,
			"sub_attr": map[string]any{
				"job_id":  jid,
				"step_id": sid,
			},
		}
	default:
		return nil, fmt.Errorf("unsupported cluster type: %s", clusterType)
	}

	slog.Debug("[TriggerAgentCollection] request built",
		"agent_url", agentURL,
		"numeric_job_id", numericJobID,
		"request_body", requestBody,
	)

	// 序列化请求体
	jsonData, err := json.Marshal(requestBody)
	if err != nil {
		return nil, fmt.Errorf("marshal request failed: %w", err)
	}

	// 带指数退避的重试逻辑
	var lastError error
	var agentResp any

	for attempt := 0; attempt < c.maxAttempts; attempt++ {
		slog.Debug("agent request attempt",
			"attempt", attempt+1,
			"max_attempts", c.maxAttempts,
			"url", agentURL,
		)

		resp, err := c.client.Post(agentURL, "application/json", bytes.NewBuffer(jsonData))
		if err == nil {
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				if err := json.NewDecoder(resp.Body).Decode(&agentResp); err != nil {
					lastError = fmt.Errorf("parse agent response failed: %w", err)
				} else {
					slog.Debug("[TriggerAgentCollection] success",
						"node_base_url", nodeBaseURL,
						"job_id", jobID,
						"numeric_job_id", numericJobID,
					)
					return agentResp, nil
				}
			} else {
				body, _ := io.ReadAll(resp.Body)
				lastError = fmt.Errorf("agent returned status %d: %s", resp.StatusCode, string(body))
			}
		} else {
			lastError = fmt.Errorf("call agent failed: %w", err)
		}

		if attempt < c.maxAttempts-1 {
			delay := time.Duration(float64(c.initialDelay) * math.Pow(c.retryMultiplier, float64(attempt)))
			slog.Warn("agent request failed, retrying",
				"attempt", attempt+1,
				"error", lastError,
				"retry_after", delay,
			)
			time.Sleep(delay)
		}
	}

	slog.Error("[TriggerAgentCollection] all attempts failed",
		"node_base_url", nodeBaseURL,
		"job_id", jobID,
		"max_attempts", c.maxAttempts,
		"last_error", lastError,
	)

	return nil, fmt.Errorf("agent request failed after %d attempts: %w", c.maxAttempts, lastError)
}
