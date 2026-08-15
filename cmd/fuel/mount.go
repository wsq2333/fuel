package main

import (
	"flag"
	"fmt"

	"fuel/internal/config"
)

// runMount 实现 mount 子命令。
// Week 1 仅完成配置加载与校验；FUSE 挂载在 Week 3 实现。
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

	return fmt.Errorf("fuse mount not implemented yet (week 3); mount-point=%s bucket=%s backend=%s",
		cfg.Fuse.MountPoint, cfg.Storage.Bucket, cfg.Storage.Type)
}
