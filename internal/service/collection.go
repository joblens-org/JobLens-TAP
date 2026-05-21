package service

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/joblens/tap/internal/cluster"
	"github.com/joblens/tap/internal/config"
	"github.com/joblens/tap/internal/model"
)

// CollectionService 采集服务（协调层）
type CollectionService struct {
	cfg         *config.Config
	clusterMgr  *cluster.Manager
	scriptSvc   *ClusterScriptService
	registrySvc *ServiceRegistryClient
	agentClient *AgentClient
}

// NewCollectionService 创建采集服务
func NewCollectionService(cfg *config.Config, clusterMgr *cluster.Manager) *CollectionService {
	return &CollectionService{
		cfg:        cfg,
		clusterMgr: clusterMgr,
		scriptSvc:  NewClusterScriptService(clusterMgr),
		registrySvc: NewServiceRegistryClient(
			cfg.ServiceRegistryURL,
			cfg.ServiceRegistryTimeout,
		),
		agentClient: NewAgentClient(
			cfg.AgentRetryInitialDelay,
			cfg.AgentRetryMaxAttempts,
			cfg.AgentRetryMultiplier,
		),
	}
}

// TriggerCollection 触发作业采集
func (s *CollectionService) TriggerCollection(ctx context.Context, clusterName string, clusterTag string, jobID string, collector string) (*model.TriggerCollectionResponse, error) {
	slog.Debug("[TriggerCollection] entry",
		"cluster_name", clusterName,
		"cluster_tag", clusterTag,
		"job_id", jobID,
		"collector", collector,
	)

	cluster, ok := s.clusterMgr.Get(clusterName)
	if !ok {
		slog.Warn("[TriggerCollection] cluster not found", "cluster_name", clusterName)
		return &model.TriggerCollectionResponse{
			Status:      "failed",
			ClusterName: clusterName,
			JobID:       jobID,
			ClusterTag:  clusterTag,
			Message:     fmt.Sprintf("cluster not found: %s", clusterName),
		}, nil
	}

	scriptOutput, err := s.scriptSvc.ExecuteScript(clusterName, clusterTag, jobID)
	if err != nil {
		slog.Warn("[TriggerCollection] script failed",
			"cluster_name", clusterName,
			"job_id", jobID,
			"error", err,
		)
		return &model.TriggerCollectionResponse{
			Status:      "failed",
			ClusterName: clusterName,
			JobID:       jobID,
			ClusterTag:  clusterTag,
			NodeType:    cluster.Type,
			Message:     fmt.Sprintf("script execution failed: %s", err.Error()),
		}, nil
	}

	nodeList := scriptOutput.Nodes
	var primaryNode string
	if len(nodeList) > 0 {
		primaryNode = nodeList[0]
	}

	if len(nodeList) == 0 {
		slog.Warn("[TriggerCollection] no nodes found",
			"cluster_name", clusterName,
			"job_id", jobID,
		)
		return &model.TriggerCollectionResponse{
			Status:      "failed",
			ClusterName: clusterName,
			JobID:       jobID,
			ClusterTag:  clusterTag,
			NodeType:    cluster.Type,
			Message:     "no nodes found for job",
		}, nil
	}

	var jobInfo any
	switch cluster.Type {
	case "condor":
		if scriptOutput.JobInfo != nil {
			jobInfo = scriptOutput.JobInfo
		} else {
			jobInfo = &model.HTCondorJobInfo{
				ClusterID: clusterName,
				JobID:     jobID,
				NodeName:  primaryNode,
				Slot:      scriptOutput.Slot,
			}
		}
	case "slurm":
		if scriptOutput.JobInfo != nil {
			jobInfo = scriptOutput.JobInfo
			if jobInfoMap, ok := jobInfo.(map[string]any); ok {
				if _, exists := jobInfoMap["node_list"]; !exists {
					jobInfoMap["node_list"] = strings.Join(nodeList, ",")
				}
			}
		} else {
			jobInfo = &model.SlurmJobInfo{
				ClusterID: clusterName,
				JobID:     jobID,
				NodeName:  primaryNode,
				NodeList:  strings.Join(nodeList, ","),
			}
		}
	}

	slog.Debug("[TriggerCollection] dispatching to agents",
		"cluster_name", clusterName,
		"job_id", jobID,
		"node_count", len(nodeList),
		"nodes", nodeList,
	)

	// 解析逗号分隔的采集器列表
	rawCollectors := strings.Split(collector, ",")
	collectors := make([]string, 0, len(rawCollectors))
	for _, c := range rawCollectors {
		c = strings.TrimSpace(c)
		if c != "" {
			collectors = append(collectors, c)
		}
	}

	var agentResponses []map[string]any
	var errors []string

	for _, nodeName := range nodeList {
		nodeBaseURL, err := s.registrySvc.GetNodeURLWithFallback(nodeName, cluster.DefaultNodePort)
		if err != nil {
			slog.Warn("[TriggerCollection] resolve node URL failed",
				"node", nodeName,
				"error", err,
			)
			errors = append(errors, fmt.Sprintf("node %s: %s", nodeName, err.Error()))
			continue
		}

		agentResp, err := s.agentClient.TriggerAgentCollection(
			nodeBaseURL,
			cluster.Type,
			clusterTag,
			jobID,
			scriptOutput.Slot,
			collectors,
			jobInfo,
		)
		if err != nil {
			slog.Warn("[TriggerCollection] agent request failed",
				"node", nodeName,
				"job_id", jobID,
				"error", err,
			)
			errors = append(errors, fmt.Sprintf("node %s: %s", nodeName, err.Error()))
			continue
		}

		if respMap, ok := agentResp.(map[string]any); ok {
			respMap["node_name"] = nodeName
			agentResponses = append(agentResponses, respMap)
		}
	}

	var status string
	var message string

	totalCalls := len(nodeList) * len(collectors)
	if len(agentResponses) == totalCalls && totalCalls > 0 {
		status = "success"
		message = fmt.Sprintf("collection triggered successfully on %d node(s)", len(nodeList))
	} else if len(agentResponses) > 0 {
		status = "partial_success"
		message = fmt.Sprintf("collection triggered on %d/%d task(s)", len(agentResponses), totalCalls)
	} else {
		status = "failed"
		message = fmt.Sprintf("collection failed on all %d task(s): %s", totalCalls, strings.Join(errors, "; "))
	}

	slog.Info("[TriggerCollection] completed",
		"cluster_name", clusterName,
		"cluster_tag", clusterTag,
		"job_id", jobID,
		"status", status,
		"success_count", len(agentResponses),
		"total_nodes", len(nodeList),
	)

	response := &model.TriggerCollectionResponse{
		Status:      status,
		ClusterName: clusterName,
		JobID:       jobID,
		ClusterTag:  clusterTag,
		NodeType:    cluster.Type,
		NodeName:    primaryNode,
		Slot:        scriptOutput.Slot,
		JobInfo:     jobInfo,
		AgentResp:   agentResponses,
		Message:     message,
	}

	return response, nil
}

// TriggerDirectCollection 直接触发采集（跳过脚本查询，用户提供节点信息）
func (s *CollectionService) TriggerDirectCollection(ctx context.Context, req *model.DirectTriggerCollectionRequest) (*model.TriggerCollectionResponse, error) {
	slog.Debug("[TriggerDirectCollection] entry",
		"cluster_name", req.ClusterName,
		"cluster_tag", req.ClusterTag,
		"job_id", req.JobID,
		"collector", req.Collector,
		"node", req.Node,
		"slot", req.Slot,
	)

	cluster, ok := s.clusterMgr.Get(req.ClusterName)
	if !ok {
		slog.Warn("[TriggerDirectCollection] cluster not found",
			"cluster_name", req.ClusterName,
		)
		return &model.TriggerCollectionResponse{
			Status:      "failed",
			ClusterName: req.ClusterName,
			JobID:       req.JobID,
			ClusterTag:  req.ClusterTag,
			Message:     fmt.Sprintf("cluster not found: %s", req.ClusterName),
		}, nil
	}

	if cluster.Type == "condor" && req.Slot == "" {
		return nil, fmt.Errorf("slot is required for condor cluster")
	}

	nodeBaseURL, err := s.registrySvc.GetNodeURLWithFallback(req.Node, cluster.DefaultNodePort)
	if err != nil {
		slog.Warn("[TriggerDirectCollection] resolve node URL failed",
			"node", req.Node,
			"error", err,
		)
		return &model.TriggerCollectionResponse{
			Status:      "failed",
			ClusterName: req.ClusterName,
			JobID:       req.JobID,
			ClusterTag:  req.ClusterTag,
			NodeType:    cluster.Type,
			NodeName:    req.Node,
			Slot:        req.Slot,
			Message:     fmt.Sprintf("resolve node URL failed: %s", err.Error()),
		}, nil
	}

	var jobInfo any
	switch cluster.Type {
	case "condor":
		jobInfo = &model.HTCondorJobInfo{
			ClusterID: req.ClusterName,
			JobID:     req.JobID,
			NodeName:  req.Node,
			Slot:      req.Slot,
		}
	case "slurm":
		jobInfo = &model.SlurmJobInfo{
			ClusterID: req.ClusterName,
			JobID:     req.JobID,
			NodeName:  req.Node,
			NodeList:  req.Node,
		}
	}

	// 解析逗号分隔的采集器列表，并添加 _collector 后缀
	rawCollectors := strings.Split(req.Collector, ",")
	collectors := make([]string, 0, len(rawCollectors))
	for _, c := range rawCollectors {
		c = strings.TrimSpace(c)
		if c != "" {
			collectors = append(collectors, c)
		}
	}

	agentResp, err := s.agentClient.TriggerAgentCollection(
		nodeBaseURL,
		cluster.Type,
		req.ClusterTag,
		req.JobID,
		req.Slot,
		collectors,
		jobInfo,
	)
	if err != nil {
		slog.Warn("[TriggerDirectCollection] agent request failed",
			"node", req.Node,
			"job_id", req.JobID,
			"error", err,
		)
		return &model.TriggerCollectionResponse{
			Status:      "failed",
			ClusterName: req.ClusterName,
			JobID:       req.JobID,
			ClusterTag:  req.ClusterTag,
			NodeType:    cluster.Type,
			NodeName:    req.Node,
			Slot:        req.Slot,
			JobInfo:     jobInfo,
			Message:     fmt.Sprintf("agent request failed: %s", err.Error()),
		}, nil
	}

	if respMap, ok := agentResp.(map[string]any); ok {
		respMap["node_name"] = req.Node
	}

	slog.Info("[TriggerDirectCollection] completed",
		"cluster_name", req.ClusterName,
		"cluster_tag", req.ClusterTag,
		"job_id", req.JobID,
		"node", req.Node,
	)

	response := &model.TriggerCollectionResponse{
		Status:      "success",
		ClusterName: req.ClusterName,
		JobID:       req.JobID,
		ClusterTag:  req.ClusterTag,
		NodeType:    cluster.Type,
		NodeName:    req.Node,
		Slot:        req.Slot,
		JobInfo:     jobInfo,
		AgentResp:   agentResp,
		Message:     "collection triggered successfully",
	}

	return response, nil
}
