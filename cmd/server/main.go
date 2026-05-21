package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/joblens/tap/internal/cluster"
	"github.com/joblens/tap/internal/config"
	"github.com/joblens/tap/internal/handler"
	"github.com/joblens/tap/internal/middleware"
	"github.com/joblens/tap/internal/repository"
	"github.com/joblens/tap/internal/service"
)

// 版本信息（Version 由人为维护，GitCommit 和 BuildTime 编译时通过 -ldflags 注入）
var (
	Version   = "0.1.0"
	GitCommit = "unknown"
	BuildTime = "unknown"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	slog.Info("starting joblens tap server")

	// 加载配置
	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	slog.Info("config loaded", "port", cfg.Port)

	// 初始化集群管理器（从管理 API 拉取集群元数据）
	clusterMgr := cluster.NewManager(cfg.ManagementAPIURL, cfg.ManagementCacheTTL)
	ctx := context.Background()
	if err := clusterMgr.InitialFetch(ctx); err != nil {
		slog.Error("failed to fetch cluster scheme from management api", "error", err)
		os.Exit(1)
	}
	go clusterMgr.BackgroundRefresh()

	// 初始化 ES 客户端管理器（惰性创建，注入 clusterMgr）
	esManager := repository.NewClientManager(clusterMgr)

	// 初始化服务
	querySvc := service.NewQueryService(cfg, esManager, clusterMgr)
	collectionSvc := service.NewCollectionService(cfg, clusterMgr)

	// 初始化处理器
	healthHandler := handler.NewHealthHandler(esManager, Version, GitCommit, BuildTime)
	rawHandler := handler.NewRawHandler(esManager, querySvc)
	timeseriesHandler := handler.NewTimeSeriesHandler(esManager, querySvc)
	summaryHandler := handler.NewSummaryHandler(esManager, querySvc)
	schemaHandler := handler.NewSchemaHandler(clusterMgr, cfg)
	collectionHandler := handler.NewCollectionHandler(collectionSvc)
	checkJobHandler := handler.NewCheckJobHandler(querySvc)
	skillHandler := handler.NewSkillHandler(cfg.SkillAPIBaseURL)

	// 设置 Gin 模式
	if os.Getenv("GIN_MODE") == "" {
		gin.SetMode(gin.ReleaseMode)
	}

	// 创建路由
	r := gin.New()
	r.Use(middleware.Recovery())
	r.Use(middleware.Logger())
	r.Use(middleware.ErrorHandler())

	// 健康检查
	r.GET("/health", healthHandler.Health)
	r.GET("/ready", healthHandler.Ready)

	// API 路由组
	api := r.Group("/data")
	{
		api.GET("/raw", rawHandler.Query)
		api.GET("/timeseries", timeseriesHandler.Query)
		api.GET("/summary", summaryHandler.Query)
		api.GET("/check-job", checkJobHandler.Check)
	}

	// 采集接口
	r.POST("/collect", collectionHandler.Trigger)
	r.POST("/collect/direct", collectionHandler.TriggerDirect)

	// Schema 接口
	r.GET("/schema", schemaHandler.Get)

	// Skill 接口（返回 joblens-tap-api 使用说明）
	r.GET("/skill", skillHandler.Get)

	// 创建 HTTP 服务器
	srv := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Port),
		Handler:      r,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	}

	// 优雅关闭
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	slog.Info("server started", "addr", srv.Addr)

	// SIGHUP 热重载采集器注册文件
	if cfg.CollectorRegistryPath != "" {
		reloadCh := make(chan os.Signal, 1)
		signal.Notify(reloadCh, syscall.SIGHUP)
		go func() {
			for range reloadCh {
				slog.Info("received SIGHUP, reloading collector registry")
				if err := cfg.Registry.Reload(); err != nil {
					slog.Error("reload collector registry failed", "error", err)
				} else {
					// 同步更新 DefaultCollectors（并发安全）
					cfg.DefaultCollectors = cfg.Registry.GetCollectorNames()
					slog.Info("collector registry reloaded",
						"collectors", len(cfg.DefaultCollectors),
					)
				}
			}
		}()
	}

	// 等待中断信号
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	slog.Info("shutting down server...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("server forced to shutdown", "error", err)
	}

	slog.Info("server exited")
}
