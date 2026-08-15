package metadata

import (
	"fmt"

	"fuel/api"
	"fuel/internal/config"
)

// MetadataEngineFactory 构造 MetadataEngine 的工厂函数 (INV-4)。
// direct 模式依赖 store 直查对象存储；redis/mysql 模式可在健康检查失败时降级为 direct。
type MetadataEngineFactory func(cfg *config.Config, store api.ObjectStore) (api.MetadataEngine, error)

// registry 引擎类型 → 工厂函数。
var registry = map[string]MetadataEngineFactory{}

// RegisterMetadataEngine 注册一个元数据引擎实现。engineName 对应 config.metadata.engine。
func RegisterMetadataEngine(engineName string, factory MetadataEngineFactory) {
	registry[engineName] = factory
}

// NewMetadataEngine 根据配置创建 MetadataEngine 实例。
// 引擎选择由 cfg.Metadata.Engine 决定 (direct | redis | mysql)。
func NewMetadataEngine(cfg *config.Config, store api.ObjectStore) (api.MetadataEngine, error) {
	factory, ok := registry[cfg.Metadata.Engine]
	if !ok {
		return nil, fmt.Errorf("unsupported metadata engine %q (registered: %v)", cfg.Metadata.Engine, registeredEngines())
	}
	return factory(cfg, store)
}

// registeredEngines 返回已注册的引擎类型列表。
func registeredEngines() []string {
	engines := make([]string, 0, len(registry))
	for e := range registry {
		engines = append(engines, e)
	}
	return engines
}
