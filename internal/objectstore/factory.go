package objectstore

import (
	"fmt"

	"fuel/api"
	"fuel/internal/config"
)

// ObjectStoreFactory 根据配置构造 ObjectStore 实例的工厂函数 (INV-8)。
type ObjectStoreFactory func(cfg *config.Config) (api.ObjectStore, error)

// registry 后端类型 → 工厂函数。新增后端通过 RegisterObjectStore 注册。
var registry = map[string]ObjectStoreFactory{}

// RegisterObjectStore 注册一个对象存储后端。typeName 对应 config.storage.type。
// 通常在实现的 init() 中调用。
func RegisterObjectStore(typeName string, factory ObjectStoreFactory) {
	registry[typeName] = factory
}

// NewObjectStore 根据配置创建 ObjectStore 实例。
// 后端选择由 cfg.Storage.Type 决定，调用方不感知具体实现。
func NewObjectStore(cfg *config.Config) (api.ObjectStore, error) {
	factory, ok := registry[cfg.Storage.Type]
	if !ok {
		return nil, fmt.Errorf("unsupported storage type %q (registered: %v)", cfg.Storage.Type, registeredTypes())
	}
	return factory(cfg)
}

// registeredTypes 返回已注册的后端类型列表。
func registeredTypes() []string {
	types := make([]string, 0, len(registry))
	for t := range registry {
		types = append(types, t)
	}
	return types
}
