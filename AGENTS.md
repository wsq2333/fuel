# Fuel — AGENTS.md

> 本文件是 AI 编码助手（如 opencode）在本项目中工作时的指导规范。
> 所有代码生成、修改、审查必须遵守本文件的约束。
> 关联文档: [ARCH_SPEC.md](./ARCH_SPEC.md) — 完整架构设计文档

---

## 1. 项目介绍

### 1.1 是什么

Fuel 是一个面向对象存储的高性能 POSIX 缓存文件系统，在数据消费集群提供本地 NVMe 缓存加速，通过 FUSE 接口对训练应用透明暴露。当前以阿里云 OSS 为主要实现后端，架构上支持扩展其他 S3 兼容对象存储。

### 1.2 业务背景

- 业务领域：面向自动驾驶的机器学习训练任务
- 数据存储：所有训练数据均存储在阿里云 OSS 上
- 数据特征：海量小文件（传感器帧 JPEG/PNG/PCD）+ 大文件（点云、视频片段）
- 访问模式：**一次写、多次读**，无追加写，对写性能不敏感
- 部署形态：先本地裸机，后 K8s（DaemonSet / CSI Driver / Sidecar）

### 1.3 技术栈

- 语言：Go 1.21+
- FUSE 库：`github.com/hanwen/go-fuse/v2`
- OSS SDK：`github.com/aliyun/aliyun-oss-go-sdk/oss`
- 元数据引擎（可选）：Redis (`github.com/redis/go-redis/v9`) / MySQL (`github.com/go-sql-driver/mysql`)
- 监控：`github.com/prometheus/client_golang`
- 日志：`go.uber.org/zap`

### 1.4 不是什么

- 不是完整的文件系统或对象存储 — 不管理数据生命周期，对象存储是数据的唯一真相来源
- 不修改对象存储中的原始数据 — 缓存是字节镜像，不做格式转换（区别于 JuiceFS）
- 不是分布式缓存系统 — 数据缓存各节点独立，不做跨节点数据共享
- 不支持随机写 / 追加写 — 仅支持"一次写多次读"语义
- 不替代编排层 — Fuel 是数据面，编排层（Fluid 等）是控制面

### 1.5 详细设计

完整的架构设计见 [ARCH_SPEC.md](./ARCH_SPEC.md)，包括：系统架构图、模块分解、接口定义、数据流、一致性模型、配置规范、部署规范、实施路线。

---

## 2. 不变量列表

> 不变量是架构设计的硬约束。任何代码生成和修改必须遵守。违反不变量意味着架构方向错误，必须停止并重新审视设计。

### INV-1: 对象存储是数据真相来源

对象存储后端中的对象元数据和内容是数据的唯一权威来源。所有缓存层（内存缓存、元数据引擎、NVMe 数据缓存）都是加速层，可以丢失、可以重建，但不可以被当作权威来源。

- 元数据引擎丢失 → 降级为直查对象存储，功能不受影响
- NVMe 缓存丢失 → 从对象存储重新拉取，无数据丢失
- 不存在"元数据引擎丢失 = 数据不可读"的场景

### INV-2: 缓存是对象存储对象的字节镜像

NVMe 上的缓存文件路径与对象存储路径一一对应，内容是完整字节副本。不做格式转换、分块、压缩、去重。

- 缓存路径: `{cache_dir}/{bucket}/{key}` ←→ 对象存储: `{scheme}://{bucket}/{key}`
- 缓存文件可被外部工具直接读取（`cat` / `md5sum`）
- 缓存可被清空，下次访问自动重建
- 缓存校验基于对象 ETag

### INV-3: 写路径不改变对象存储中的对象格式

写路径通过 PutObject 整文件上传。上传后的对象与直接通过 SDK/控制台上传的完全一致。不引入只有 Fuel 能解读的数据格式。

### INV-4: 元数据引擎是可选的加速层

元数据引擎（Redis / MySQL / 直查对象存储）是加速层，不是必需组件。三种模式通过统一接口抽象，运行时可切换。不可用时自动降级为直查对象存储。

### INV-5: FUSE 进程与编排层解耦

FUSE 进程不依赖 K8s、不依赖编排层即可运行。数据路径: 应用 → FUSE → 缓存 → 对象存储，编排层不在此路径上。

### INV-6: 单节点数据缓存，不做跨节点数据共享

每个节点的 NVMe 缓存独立。跨节点共享仅限元数据（通过元数据引擎）。不实现分布式缓存协议、不做跨节点数据传输。

### INV-7: 模块边界通过接口隔离

核心模块之间通过 Go interface 隔离，不直接依赖具体实现。

- FUSE 层依赖 `MetadataEngine` 接口，不依赖 `RedisEngine` 具体类型
- FUSE 层依赖 `DataCache` 接口，不依赖 `NVMeCache` 具体类型
- FUSE 层依赖 `ObjectStore` 接口，不依赖 `OSSClient` / `S3Client` 具体类型
- 每个接口可通过 mock 测试

### INV-8: 对象存储后端可插拔

对象存储后端通过 `ObjectStore` 接口抽象。新增后端只需实现接口并注册到工厂函数，不改 FUSE 层和缓存层代码。

- 所有后端实现统一的 `ObjectStore` 接口
- 后端选择通过配置项 `storage.type` 指定
- 不允许在 FUSE 层或缓存层出现 `if backend == "oss"` 这样的分支逻辑

---

## 3. 代码生成约束

> 以下约束适用于所有 AI 生成的代码。每条约束必须被遵守，不可跳过。

### 3.1 必须有单元测试

**约束**: 每个新增的公开函数、接口实现、关键内部逻辑必须有对应的单元测试。

**要求**:
- 测试文件与被测代码同目录，命名为 `foo_test.go`
- 使用 Go 标准 `testing` 包
- 依赖外部服务（OSS / Redis / MySQL）的测试使用 mock 或 interface 替换，不依赖真实服务
- 真实外部服务的集成测试使用 build tag `//go:build integration` 隔离
- 测试覆盖率目标: 核心逻辑（FUSE ops、cache、metadata、objectstore）> 70%
- 生成代码时，必须同时生成对应的测试代码
- 测试必须可独立运行: `go test ./internal/cache/...` 不需要外部依赖即可通过

**示例**:

```go
// internal/objectstore/oss_test.go

//go:build integration

package objectstore

import (
    "context"
    "testing"
)

func TestOSSClient_Head(t *testing.T) {
    client := newMockObjectStore()  // 使用 mock, 不依赖真实 OSS
    meta, err := client.Head(context.Background(), "test/file.txt")
    if err != nil {
        t.Fatalf("Head failed: %v", err)
    }
    if meta.Size != 1024 {
        t.Errorf("expected size 1024, got %d", meta.Size)
    }
}
```

```go
// internal/cache/data_test.go

package cache

import (
    "testing"
)

func TestDataCache_PutAndGet(t *testing.T) {
    cache := NewNVMeCache(t.TempDir(), 1<<20, nil)
    
    err := cache.Put("test/file.txt", "etag1", []byte("hello"))
    if err != nil {
        t.Fatalf("Put failed: %v", err)
    }
    
    data, hit, err := cache.Get("test/file.txt", "etag1", 0, 5)
    if err != nil || !hit {
        t.Fatalf("Get failed: err=%v, hit=%v", err, hit)
    }
    if string(data) != "hello" {
        t.Errorf("expected 'hello', got '%s'", string(data))
    }
}
```

### 3.2 必须处理硬件、网络及操作系统错误

**约束**: 所有涉及 I/O 的代码（对象存储、NVMe 磁盘、Redis/MySQL 网络、FUSE 系统调用）必须正确处理硬件、网络和操作系统层面的错误，不能 panic 或忽略。

**要求**:

1. **网络错误处理**（对象存储 / Redis / MySQL 连接）:
   - 连接超时、读取超时、连接断开必须重试（指数退避，最多 3 次）
   - 重试耗尽后返回错误给上层，不 panic
   - 区分可重试错误（5xx、429、网络超时）和不可重试错误（404、403、400）
   - 使用 context 传递超时和取消信号

2. **磁盘错误处理**（NVMe 缓存读写）:
   - 磁盘空间不足（`ENOSPC`）→ 触发 LRU 淘汰后重试，淘汰后仍不足则降级为不缓存
   - 磁盘 I/O 错误（`EIO`）→ 记录错误日志，跳过缓存，直透对象存储
   - 磁盘只读 → 记录错误日志，降级为只读不缓存模式
   - 文件不存在（`ENOENT`）→ 正常处理，不当作错误

3. **FUSE 系统调用错误处理**:
   - 挂载失败 → 返回明确错误信息，不 panic
   - `/dev/fuse` 设备不存在 → 返回错误提示需要 privileged 权限
   - mount propagation 失败 → 记录日志，尝试备选方案

4. **操作系统资源错误**:
   - 文件描述符耗尽（`EMFILE`）→ 记录日志，降级处理
   - 内存不足 → 控制 buffer 池大小，不无限制分配

5. **所有错误必须包装上下文**:
   ```go
   // 正确: 包装上下文 + 使用 %w 保留原始错误
   if err != nil {
       return fmt.Errorf("head object %s: %w", key, err)
   }
   
   // 错误: 丢失上下文
   if err != nil {
       return err
   }
   
   // 错误: 丢失原始错误类型
   if err != nil {
       return fmt.Errorf("head object %s: %v", key, err)
   }
   ```

6. **用户可见错误返回 POSIX errno**:
   ```go
   // 文件不存在
   return syscall.ENOENT
   
   // I/O 错误
   return syscall.EIO
   
   // 权限不足
   return syscall.EACCES
   
   // 不支持的操作
   return syscall.ENOTSUP
   ```

7. **禁止 panic 在 I/O 路径**:
   - FUSE 操作处理函数中禁止 panic（会导致挂载点不可用）
   - 使用 `recover()` 作为最后防线，记录日志后返回 `syscall.EIO`

### 3.3 必须保证代码实现与设计及不变量的一致性

**约束**: 生成的代码必须与 [ARCH_SPEC.md](./ARCH_SPEC.md) 中的设计和不变量保持一致。任何偏离设计的实现都是错误。

**要求**:

1. **不变量合规检查** — 生成代码前，确认代码不违反任何不变量:
   - INV-1: 任何缓存层的数据是否可从对象存储重建？如果不可重建，违反 INV-1
   - INV-2: 缓存文件路径是否与对象存储路径一一对应？是否引入了格式转换？如果引入，违反 INV-2
   - INV-3: 写路径是否通过 PutObject 整文件上传？是否引入了自定义数据格式？如果是，违反 INV-3
   - INV-4: 元数据引擎是否通过接口抽象？是否有硬编码依赖特定引擎？如果是，违反 INV-4
   - INV-5: FUSE 进程是否依赖了编排层？如果是，违反 INV-5
   - INV-6: 是否引入了跨节点数据共享逻辑？如果是，违反 INV-6
   - INV-7: 模块间是否通过 interface 隔离？是否直接引用了具体实现类型？如果是，违反 INV-7
   - INV-8: 是否在 FUSE 层或缓存层出现了后端类型判断分支？如果是，违反 INV-8

2. **接口一致性** — 实现的接口签名必须与 ARCH_SPEC.md §4.3 定义完全一致:
   - `ObjectStore` 接口: `Head` / `Get` / `Put` / `List` / `Copy` / `Delete` / `Bucket`
   - `MetadataEngine` 接口: `GetAttr` / `SetAttr` / `DeleteAttr` / `ReadDir` / `SetDir` / `DeleteDir` / `BatchGetAttr` / `Invalidate` / `HealthCheck` / `Close`
   - `DataCache` 接口: `Get` / `Put` / `Remove` / `Exists` / `Stats`

3. **配置一致性** — 配置结构体字段必须与 ARCH_SPEC.md §8 定义一致:
   - `storage.type` / `storage.bucket` / `storage.oss.endpoint`
   - `metadata.engine` / `metadata.redis.address` / `metadata.mysql.dsn`
   - `cache.dir` / `cache.size` / `cache.highWatermark` / `cache.lowWatermark`
   - `fuse.mountPoint` / `fuse.options`

4. **路径映射一致性** — 路径格式必须与 ARCH_SPEC.md §6 一致:
   - FUSE 挂载点: `/fuel/{bucket}`
   - 本地缓存: `{cache_dir}/{bucket}/{key}`

5. **如果设计需要变更** — 如果实现过程中发现设计不合理，不能擅自偏离设计:
   - 先更新 [ARCH_SPEC.md](./ARCH_SPEC.md) 对应章节
   - 再按更新后的设计生成代码
   - 在 PR / commit message 中说明变更理由

### 3.4 代码风格约束

1. **不添加注释** — 除非用户明确要求，不生成代码注释。代码应当自解释（通过清晰的命名和结构）
2. **遵循现有模式** — 生成新代码前，先阅读同目录下已有代码的风格（错误处理方式、日志方式、命名习惯），保持一致
3. **Go 惯用法**:
   - 错误作为最后一个返回值
   - 接收者命名: 短名称（如 `c *Cache` 不是 `c *CacheManager`）
   - 包名: 全小写单数（`cache` 不是 `caches`）
   - 不使用 `this` / `self` 作为接收者名
4. **日志规范**:
   - 使用 `zap` 结构化日志
   - 日志级别: INFO（正常流程）/ WARN（可恢复异常）/ ERROR（不可恢复异常）/ DEBUG（调试细节）
   - 不使用 `fmt.Println` / `log.Printf`

### 3.5 禁止事项

| 禁止 | 理由 |
|------|------|
| 在 FUSE 层引用 `OSSClient` / `S3Client` 具体类型 | 违反 INV-7 + INV-8 |
| 在缓存层引用后端 SDK 类型 | 违反 INV-7 + INV-8 |
| 引入 `jacobsa/fuse` | 使用 `hanwen/go-fuse` |
| 引入 Java/JVM 依赖 | 保持纯 Go |
| 引入 CGO 依赖 | 保持纯 Go 交叉编译 |
| 引入 etcd/ZooKeeper | 不做分布式缓存 |
| 使用 `panic` 在 I/O 路径 | 必须返回 error |
| 使用 `fmt.Println` / `log.Printf` | 使用 zap |
| 生成无测试的公开函数 | 违反 §3.1 |
| 忽略 error 返回值 | 违反 §3.2 |
| 使用 `_ = err` 吞掉错误 | 违反 §3.2 |
| 擅自偏离 ARCH_SPEC.md 设计 | 违反 §3.3 |

---

## 4. 常用命令

```bash
# 构建
go build ./cmd/fuel

# 运行单元测试（不依赖外部服务）
go test ./...

# 运行集成测试（需要真实 OSS / Redis / MySQL）
go test -tags=integration ./...

# Benchmark
go test -bench=. -benchmem ./internal/cache/...

# 代码检查
go vet ./...
golangci-lint run

# 挂载（本地模式）
./fuel mount --config /etc/fuel/config.yaml

# CSI Driver 模式
./fuel csi --endpoint=unix:///csi/csi.sock

# 查看版本
./fuel version
```

---

## 5. 项目结构速查

```
fuel/
├── AGENTS.md              ← 本文件
├── ARCH_SPEC.md            ← 架构设计文档（必读）
├── cmd/fuel/               ← 入口
├── internal/
│   ├── fuse/               ← FUSE 接口层
│   ├── cache/              ← 缓存管理层
│   ├── metadata/           ← 元数据引擎 (direct/redis/mysql)
│   ├── objectstore/        ← 对象存储后端 (oss/s3/minio)
│   ├── config/             ← 配置管理
│   ├── monitor/            ← 监控
│   └── deploy/             ← K8s 部署 (daemonset/csi/sidecar)
├── api/                    ← 公共接口 + 类型
├── deploy/                 ← K8s 部署清单 YAML
└── go.mod
```

生成代码前，先确认目标模块的正确位置。不确定时，阅读 [ARCH_SPEC.md §4.2 模块分解](./ARCH_SPEC.md#42-模块分解)。
