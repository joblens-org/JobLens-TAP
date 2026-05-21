package service

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os/exec"

	"github.com/joblens/tap/internal/cluster"
)

// ClusterScriptService 集群脚本服务
type ClusterScriptService struct {
	clusterMgr *cluster.Manager
}

// NewClusterScriptService 创建集群脚本服务
func NewClusterScriptService(clusterMgr *cluster.Manager) *ClusterScriptService {
	return &ClusterScriptService{clusterMgr: clusterMgr}
}

// ScriptOutput shell 脚本的标准输出格式
type ScriptOutput struct {
	JobID   string   `json:"job_id"`
	Nodes   []string `json:"nodes"`
	Slot    string   `json:"slot,omitempty"`
	Error   string   `json:"error,omitempty"`
	JobInfo any      `json:"job_info,omitempty"`
}

// ExecuteScript 执行集群脚本并解析输出
func (s *ClusterScriptService) ExecuteScript(clusterName string, clusterTag string, jobID string) (*ScriptOutput, error) {
	info, ok := s.clusterMgr.Get(clusterName)
	if !ok {
		return nil, fmt.Errorf("cluster not found: %s", clusterName)
	}

	if info.ScriptPath == "" {
		return nil, fmt.Errorf("script path not configured for cluster: %s", clusterName)
	}

	slog.Debug("[ExecuteScript] running script",
		"cluster", clusterName,
		"cluster_tag", clusterTag,
		"script", info.ScriptPath,
		"job_id", jobID,
	)

	cmd := exec.Command(info.ScriptPath, jobID, clusterTag)

	output, err := cmd.CombinedOutput()
	if err != nil {
		slog.Warn("[ExecuteScript] script failed",
			"cluster", clusterName,
			"script", info.ScriptPath,
			"job_id", jobID,
			"error", err,
			"output", string(output),
		)
		return nil, fmt.Errorf("script execution failed: %w, output: %s", err, string(output))
	}

	var result ScriptOutput
	if err := json.Unmarshal(output, &result); err != nil {
		slog.Warn("[ExecuteScript] parse output failed",
			"cluster", clusterName,
			"job_id", jobID,
			"error", err,
			"raw_output", string(output),
		)
		return nil, fmt.Errorf("parse script output failed: %w", err)
	}

	if result.Error != "" {
		return nil, fmt.Errorf("script error: %s", result.Error)
	}

	slog.Debug("[ExecuteScript] completed",
		"cluster", clusterName,
		"job_id", result.JobID,
		"nodes", result.Nodes,
		"node_count", len(result.Nodes),
	)

	return &result, nil
}
