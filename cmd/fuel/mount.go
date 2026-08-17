package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"fuel/internal/cache"
	"fuel/internal/config"
	fuselayer "fuel/internal/fuse"
	"fuel/internal/metadata"
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

	store, err := objectstore.NewObjectStore(cfg)
	if err != nil {
		return fmt.Errorf("create object store: %w", err)
	}

	metaEng, err := metadata.NewMetadataEngine(cfg, store)
	if err != nil {
		return fmt.Errorf("create metadata engine: %w", err)
	}

	metaCache := cache.NewMetaCache(cfg.Metadata.Cache)

	dataCache, err := cache.NewNVMeCache(cfg.Cache.Dir, cfg.Storage.Bucket, cfg.Cache.Capacity, cfg.Cache.HighWatermark, cfg.Cache.LowWatermark, cfg.Cache.MaxFileSize)
	if err != nil {
		return fmt.Errorf("create data cache: %w", err)
	}

	root := fuselayer.NewFuelRoot(store, dataCache, metaCache, metaEng, cfg)

	server, err := fuselayer.Mount(root, cfg)
	if err != nil {
		return fmt.Errorf("mount: %w", err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		_ = server.Unmount()
	}()

	server.Wait()
	return nil
}