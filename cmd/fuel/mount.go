package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"fuel/internal/cache"
	"fuel/internal/config"
	fuselayer "fuel/internal/fuse"
	"fuel/internal/metadata"
	"fuel/internal/monitor"
	"fuel/internal/objectstore"
)

func runMount(args []string) error {
	fs := flag.NewFlagSet("mount", flag.ContinueOnError)
	configPath := fs.String("config", "", "path to fuel-config.yaml")
	mountPoint := fs.String("mount-point", "", "override fuse.mountPoint")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}
	if *mountPoint != "" {
		cfg.Fuse.MountPoint = *mountPoint
	}

	// 日志体系 (PLAN §9.2)：全局 JSON logger，后续所有 zap.L() 生效。
	logger, err := monitor.NewLogger(cfg.Monitor.LogLevel)
	if err != nil {
		return fmt.Errorf("init logger: %w", err)
	}
	zap.ReplaceGlobals(logger)
	defer func() { _ = logger.Sync() }()

	logger.Info("fuel starting",
		zap.String("version", version),
		zap.String("bucket", cfg.Storage.Bucket),
		zap.String("mountPoint", cfg.Fuse.MountPoint),
		zap.String("metadataEngine", cfg.Metadata.Engine),
		zap.String("logLevel", cfg.Monitor.LogLevel),
	)

	store, err := objectstore.NewObjectStore(cfg)
	if err != nil {
		return fmt.Errorf("create object store: %w", err)
	}
	// 对象存储插桩（fuel_storage_*）：对后端透明（INV-7/8），统计所有出口流量。
	store = monitor.InstrumentStore(store)

	metaEng, err := metadata.NewMetadataEngine(cfg, store)
	if err != nil {
		return fmt.Errorf("create metadata engine: %w", err)
	}

	metaCache := cache.NewMetaCache(cfg.Metadata.Cache)

	dataCache, err := cache.NewNVMeCache(cfg.Cache.Dir, cfg.Storage.Bucket, cfg.Cache.Capacity, cfg.Cache.HighWatermark, cfg.Cache.LowWatermark, cfg.Cache.MaxFileSize)
	if err != nil {
		return fmt.Errorf("create data cache: %w", err)
	}

	// Prometheus 指标采集器 + /metrics /health 端点 (PLAN §9.1, ARCH_SPEC §9)。
	// 健康检查用元数据引擎 HealthCheck：direct 模式检查对象存储可达性，
	// redis/mysql 模式检查引擎连通性。
	if err := prometheus.DefaultRegisterer.Register(monitor.NewFuelCollector(dataCache, metaCache)); err != nil {
		return fmt.Errorf("register fuel collector: %w", err)
	}
	mon := monitor.NewServer(cfg.Monitor.MetricsAddr, metaEng.HealthCheck)
	if err := mon.Start(); err != nil {
		// 监控不可用不阻塞挂载（观测性组件降级，不影响数据面）
		zap.L().Warn("monitor endpoint unavailable, continue mounting", zap.Error(err))
		mon = nil
	}

	root := fuselayer.NewFuelRoot(store, dataCache, metaCache, metaEng, cfg)

	server, err := fuselayer.Mount(root, cfg)
	if err != nil {
		if mon != nil {
			_ = mon.Stop()
		}
		return fmt.Errorf("mount: %w", err)
	}
	logger.Info("mounted", zap.String("mountPoint", cfg.Fuse.MountPoint), zap.String("bucket", cfg.Storage.Bucket))

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		logger.Info("received signal, unmounting", zap.String("signal", sig.String()))
		_ = server.Unmount()
	}()

	server.Wait()
	logger.Info("unmounted")
	if mon != nil {
		_ = mon.Stop()
	}
	_ = metaEng.Close()
	return nil
}
