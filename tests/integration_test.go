package tests

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/joblens/tap/internal/cluster"
	"github.com/joblens/tap/internal/config"
	"github.com/joblens/tap/internal/model"
	"github.com/joblens/tap/internal/repository"
)

// TestConfig 测试配置
type TestConfig struct {
	ESURL     string
	ClusterID string
	JobID     string
	TimeFrom  string
	TimeTo    string
}

// Global test configuration
var testCfg *TestConfig
var esManager *repository.ClientManager
var appCfg *config.Config
var clusterMgr *cluster.Manager

// TestMain 初始化测试环境
func TestMain(m *testing.M) {
	// 从环境变量读取测试配置
	testCfg = &TestConfig{
		ESURL:     getEnvOrDefault("TEST_ES_URL", "https://omat4htc-es.ihep.ac.cn:443"),
		ClusterID: getEnvOrDefault("TEST_CLUSTER_ID", "omat4htc"),
		JobID:     getEnvOrDefault("TEST_JOB_ID", "748564550"),
		TimeFrom:  getEnvOrDefault("TEST_TIME_FROM", "2026-03-28"),
		TimeTo:    getEnvOrDefault("TEST_TIME_TO", "2026-03-29"),
	}

	// 加载最小配置（用于 query service 初始化）
	appCfg = &config.Config{
		Port:             8080,
		MaxSize:          10000,
		DefaultSize:      100,
		MaxTimeRangeDays: 7,
		DefaultInterval:  "1m",
		Registry:         model.BuildDefaultRegistry(),
	}
	model.SetDefaultRegistry(appCfg.Registry)

	// 创建集群管理器（用于 ES 客户端管理）
	// 集成测试需要真实的 ES 环境；无 ES 时跳过
	clusterMgr = cluster.NewManager("", 5*time.Minute)
	esManager = repository.NewClientManager(clusterMgr)

	// 检测 ES 连接
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, info, err := esManager.GetClientForCluster(testCfg.ClusterID)
	if err != nil || info == nil {
		fmt.Printf("Warning: Cluster not found or ES unreachable: %v\n", err)
		fmt.Println("Integration tests will be skipped")
		os.Exit(0)
	}

	if err := client.Ping(ctx); err != nil {
		fmt.Printf("Warning: ES cluster not reachable: %v\n", err)
		fmt.Println("Integration tests will be skipped")
		os.Exit(0)
	}

	fmt.Printf("Integration tests configured: ES=%s, Cluster=%s, JobID=%s\n",
		testCfg.ESURL, testCfg.ClusterID, testCfg.JobID)

	// 运行测试
	code := m.Run()
	os.Exit(code)
}

// getEnvOrDefault 获取环境变量或返回默认值
func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// skipIfNoJobID 如果没有配置 JobID 则跳过测试
func skipIfNoJobID(t *testing.T) {
	if testCfg.JobID == "" {
		t.Skip("TEST_JOB_ID not set, skipping test that requires real job data")
	}
}

// skipIfShort 如果是短测试模式则跳过
func skipIfShort(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
}
