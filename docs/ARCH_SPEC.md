# Fuel — Architecture Specification

> 项目名称: **fuel**
> 版本: v0.1
> 日期: 2026-08-15
> 许可证: Apache 2.0
> 语言: Go
> 关联文档: [idea.md](../design/idea.md) | [fuse-cache-node-design.md](../fuse_implement/fuse-cache-node-design.md) | [estore-fuse-vs-goofys.md](../fuse_implement/estore-fuse-vs-goofys.md) | [fork-vs-build-from-scratch.md](../fuse_implement/fork-vs-build-from-scratch.md) | [IMPL_DESIGN.md](./IMPL_DESIGN.md)

---

## 1. 项目定位

### 1.1 一句话定义

**Fuel 是一个面向对象存储的高性能 POSIX 缓存文件系统，在数据消费集群提供本地 NVMe 缓存加速，通过 FUSE 接口对训练应用透明暴露。**

### 1.2 是什么

- 面向自动驾驶 ML 训练场景的 **消费侧缓存加速层**
- 通过 FUSE 挂载，对训练框架（PyTorch / TensorFlow / Ray）提供 **POSIX 兼容接口**
- 本地 NVMe 缓存为 对象存储的 **字节镜像**（不做数据格式转换）
- 支持 Redis / MySQL 元数据引擎，跨节点共享元数据索引
- 支持本地裸机部署和 K8s 部署（DaemonSet / CSI / Sidecar）

### 1.3 不是什么

- ❌ **不是完整的文件系统或对象存储** — 不管理数据生命周期，对象存储是数据的唯一真相来源
- ❌ **不修改对象存储原始数据** — 对象在对象存储中的格式、路径、内容不变（区别于 JuiceFS 的 Chunk/Slice/Block 拆分）
- ❌ **不是分布式缓存系统** — 数据缓存各节点独立，不做跨节点数据共享（元数据可跨节点共享）
- ❌ **不支持随机写 / 追加写** — 仅支持"一次写多次读"语义（对象存储语义约束 + 业务约束）
- ❌ **不替代编排层** — Fuel 是数据面，编排层（Fluid EstoreRuntime 等）是控制面，两者解耦

### 1.4 核心约束（来自业务需求）

| 约束 | 来源 | 不可变原因 |
|------|------|-----------|
| 对象存储原始数据不变 | 业务要求 + 合规 | 对象存储是长期持久化层，不能被缓存层污染 |
| 一次写多次读 | 业务访问模式 | 训练数据写入后不修改，简化写路径 |
| POSIX 兼容 | 训练框架要求 | 应用无需改造代码 |
| 本地 + K8s 双模部署 | 部署演进需求 | 先本地验证，再 K8s 规模化 |

---

## 2. 架构不变量

> 不变量是架构设计的硬约束，任何模块的设计和实现都必须遵守。违反不变量意味着架构方向错误。

### INV-1: 对象存储是数据真相来源

**表述**: 对象存储对象的元数据（size / etag / mtime）和内容是数据的唯一权威来源。Fuel 的所有缓存层（本地内存缓存、元数据引擎、NVMe 数据缓存）都是对象存储的加速层，可以丢失、可以重建，但不可以被当作数据的权威来源。

**设计含义**:
- 元数据引擎（Redis/MySQL）丢失 → 降级为直查对象存储，功能不受影响，仅性能下降
- NVMe 缓存丢失 → 从对象存储重新拉取，无数据丢失
- 任何缓存层的数据都可以通过 `清空 + 从对象存储重新填充` 完整恢复
- 不存在"元数据引擎丢失 = 数据不可读"的场景（区别于 JuiceFS）

### INV-2: 缓存是对象存储对象的字节镜像

**表述**: NVMe 上的缓存文件路径与对象存储对象路径一一对应，内容是对象存储对象的完整字节副本。不做格式转换、分块、压缩、去重。

**设计含义**:
- 缓存路径: `/nvme/cache/{bucket}/{key}` ←→ 对象存储: `oss://{bucket}/{key}`
- 缓存文件可被外部工具直接读取（`cat` / `md5sum` / `cp`）
- 缓存可被清空（`rm -rf /nvme/cache/*`），下次访问自动重建
- 缓存可被离线预热（直接 `cp` 或并行 GET 到缓存目录）
- 缓存校验基于对象存储 ETag，不做自定义哈希

### INV-3: 写路径不改变对象存储对象格式

**表述**: 写路径通过对象存储 PutObject 整文件上传，不使用 Multipart 分块拼接后产生不同 ETag 的方式（除非业务明确需要大文件 Multipart）。上传后的对象在对象存储中与直接通过对象存储 SDK/控制台上传的对象完全一致。

**设计含义**:
- 写路径: 本地临时文件 → `PutObject` → 对象存储原始对象
- 上传后的对象存储对象可被其他工具（OSS SDK / ossutil / 控制台）直接访问
- 不引入只有 Fuel 能解读的数据格式

### INV-4: 元数据引擎是可选的加速层

**表述**: 元数据引擎（Redis / MySQL / 直查对象存储）是元数据查询的加速层，不是必需组件。三种模式通过统一接口抽象，运行时可切换，切换不改变功能，仅改变性能特征。

**设计含义**:
- 模式 A（直查对象存储）: 零外部依赖，功能完整，性能取决于对象存储延迟
- 模式 B（Redis）: 跨节点元数据共享，高性能，需部署 Redis
- 模式 C（MySQL）: 元数据持久化，冷启动快，需部署 MySQL
- 模式切换通过配置修改，不改代码
- 元数据引擎不可用时，自动降级为直查对象存储

### INV-5: FUSE 进程与编排层解耦

**表述**: Fuel FUSE 进程是一个独立的文件系统进程，不依赖 K8s、不依赖 Fluid、不依赖外部编排层即可运行。编排层负责部署、配置、监控 FUSE 进程，但不参与数据路径。

**设计含义**:
- 本地裸机: `fuel mount` 直接运行，systemd 管理
- K8s: FUSE 进程在 Pod 中运行，CSI/Webhook 负责挂载和注入
- 数据路径: 应用 → FUSE → 缓存 → 对象存储，编排层不在此路径上
- 编排层升级 / 重启不影响已挂载的 FUSE 数据路径

### INV-6: 单节点数据缓存，不做跨节点数据共享

**表述**: 每个节点的 NVMe 缓存独立，不跨节点共享数据缓存。跨节点共享仅限元数据（通过元数据引擎）。

**设计含义**:
- 不实现分布式缓存协议（无 Master/Worker 架构）
- 不做跨节点数据传输（无 P2P 数据通道）
- 各节点的缓存一致性通过 ETag 校验 + 元数据引擎保证
- 远期可扩展 P2P 数据共享，但 MVP 不做

### INV-7: 模块边界通过接口隔离

**表述**: 核心模块（FUSE 层、缓存层、元数据层、对象存储客户端、部署层、监控层）之间通过 Go interface 隔离，不直接依赖具体实现。模块可独立测试、替换、演进。

**设计含义**:
- FUSE 层依赖 `MetadataEngine` 接口，不依赖 `RedisEngine` 具体类型
- FUSE 层依赖 `DataCache` 接口，不依赖 `NVMeCache` 具体类型
- FUSE 层依赖 `ObjectStore` 接口，不依赖 `OSSClient` 具体类型
- 每个接口有明确的输入输出契约，可通过 mock 测试

---

## 3. 架构目标

> 目标是 Fuel 要达成的工程和运维能力，每个目标有明确的验收标准。

### GOAL-1: POSIX 兼容性

**目标**: 训练框架通过标准 POSIX 接口透明访问对象存储数据，无需修改代码。

**验收标准**:
- 实现 `stat` / `lookup` / `open` / `read` / `readdir` / `mkdir` / `rmdir` / `create` / `write` / `flush` / `unlink` / `rename` / `fsync`
- PyTorch DataLoader、TensorFlow tf.io、Ray Dataset 可正常读取数据
- 不支持: 随机写、追加写、hardlink、symlink、flock（明确文档告知）

### GOAL-2: 缓存命中性能

**目标**: 缓存命中时读延迟接近本地 NVMe 直读。

**验收标准**:
- 缓存命中读延迟 P50 < 1ms
- 缓存命中读延迟 P99 < 5ms
- 顺序读吞吐 > 2 GB/s（NVMe 带宽）
- 元数据操作（stat）P50 < 5ms（L1 命中）/ < 20ms（L2 命中）

### GOAL-3: 缓存未命中回源性能

**目标**: 缓存未命中时充分利用对象存储内网带宽，回源延迟可控。

**验收标准**:
- 缓存未命中读延迟 P50 < 50ms（对象存储内网 GET Range）
- 大文件并发拉取吞吐 ≥ 对象存储内网带宽的 80%
- 元数据回源（HEAD）延迟 P50 < 30ms

### GOAL-4: 缓存命中率

**目标**: 热数据集在 NVMe 容量范围内时，缓存命中率 > 80%。

**验收标准**:
- 热数据集 < NVMe 容量 70% 时，命中率 > 90%
- 热数据集 = NVMe 容量 100% 时，命中率 > 80%（LRU 淘汰）
- 数据预热后首次训练 epoch 命中率 > 95%

### GOAL-5: 双模部署

**目标**: 本地裸机和 K8s 两种部署模式下，FUSE 进程行为一致。

**验收标准**:
- 本地模式: systemd 管理进程，YAML 配置，Prometheus 指标
- K8s 模式: DaemonSet / CSI Driver / Sidecar 三种部署形态
- 两种模式下 FUSE 进程使用相同的配置文件格式和命令行参数
- 两种模式下监控指标一致

### GOAL-6: 可观测性

**目标**: 缓存命中率、对象存储请求量、FUSE 延迟、元数据引擎状态等关键指标可监控可告警。

**验收标准**:
- Prometheus `/metrics` 端点暴露全部指标
- `/health` 端点支持 K8s livenessProbe
- 关键告警: 缓存命中率低 / 对象存储错误率高 / 元数据引擎不可达 / FUSE 延迟高

### GOAL-7: 故障降级

**目标**: 任何缓存层故障不影响数据可读性（仅影响性能）。

**验收标准**:
- NVMe 磁盘故障 → 从对象存储重新拉取
- 元数据引擎不可达 → 降级为直查对象存储
- FUSE 进程崩溃 → systemd/K8s 自动重启，缓存索引重建
- 对象存储不可达 → 已缓存数据正常读，未缓存返回 EIO

### GOAL-8: 可维护性

**目标**: 代码架构清晰，新开发者可在 1 天内理解整体架构，1 周内开始贡献代码。

**验收标准**:
- 每个模块有单一职责，通过 interface 隔离
- 核心数据结构在 `types.go` 集中定义
- 每个模块有单元测试（mock 依赖）
- 端到端集成测试覆盖核心场景

---

## 4. 系统架构

### 4.1 架构总览

```
┌──────────────────────────────────────────────────────────────┐
│                    消费集群计算节点                          │
│                                                              │
│   训练应用 (PyTorch / TensorFlow / Ray)                       │
│     │                                                        │
│     │ POSIX (stat / open / read / readdir / write)           │
│     ▼                                                        │
│   ┌────────────────────────────────────────────────────────┐ │
│   │  fuel FUSE 进程 (Go)                                  │ │
│   │                                                        │ │
│   │  ┌──────────────┐  ┌──────────────────┐  ┌──────────┐ │ │
│   │  │ FUSE 层      │  │ 缓存管理层       │  │ 对象存储  │ │ │
│   │  │ (go-fuse)    │  │                  │  │ 客户端    │ │ │
│   │  │              │  │ ┌──────────────┐ │  │ (OSS SDK)│ │ │
│   │  │ POSIX ops    │  │ │ L1: 内存缓存  │ │  │          │ │ │
│   │  │ → 内部接口   │──│ │   stat/dir    │ │  │ Head     │ │ │
│   │  │              │  │ │   TTL        │ │  │ Get(Range)│ │ │
│   │  │              │  │ └──────┬───────┘ │  │ Put      │ │ │
│   │  │              │  │        │         │  │ List     │ │ │
│   │  │              │  │ ┌──────▼───────┐ │  │ Copy     │ │ │
│   │  │              │  │ │ L2: 元数据    │ │  │ Delete   │ │ │
│   │  │              │  │ │   引擎接口    │ │  │          │ │ │
│   │  │              │  │ │ (OSS/Redis/  │ │  │          │ │ │
│   │  │              │  │ │  MySQL)      │ │  │          │ │ │
│   │  │              │  │ └──────────────┘ │  │          │ │ │
│   │  │              │  │ ┌──────────────┐ │  │          │ │ │
│   │  │              │  │ │ 数据缓存     │ │  │          │ │ │
│   │  │              │  │ │ (NVMe LRU)   │ │  │          │ │ │
│   │  │              │  │ │ 对象镜像     │ │  │          │ │ │
│   │  │              │  │ │ 预读/并发    │ │  │          │ │ │
│   │  │              │  │ └──────────────┘ │  │          │ │ │
│   │  └──────────────┘  └────────┬─────────┘  └────┬─────┘ │ │
│   └──────────────────────────────┼─────────────────┼───────┘ │
│                                  │                 │         │
│                    ┌─────────────▼──────────┐      │         │
│                    │  /nvme/cache/          │      │         │
│                    │  (OSS 对象字节镜像)      │      │         │
│                    └──────────────────────┘      │         │
└──────────────────────────────────────────────────┼─────────┘
              │                                      │
              │ (元数据引擎, 可选)                    │
              ▼                                      ▼
  ┌─────────────────────────┐    ┌──────────────────────────────┐
  │   元数据引擎 (可选)      │    │         阿里云 OSS           │
  │                         │    │  (oss-cn-wulanchabu-internal) │
  │  模式 A: 不部署          │    │  (真相来源)                  │
  │         (直查 OSS)       │    │                              │
  │  模式 B: Redis           │    │  原始对象, 不被修改           │
  │  模式 C: MySQL           │    │                              │
  └─────────────────────────┘    └──────────────────────────────┘
```

### 4.2 模块分解

```
fuel/
├── cmd/
│   └── fuel/              # 入口: fuel mount / fuel version
├── internal/
│   ├── fuse/              # FUSE 接口层 (go-fuse fs.InodeEmbedder)
│   │   ├── server.go      # FUSE Server 生命周期
│   │   ├── node.go        # FuelRoot + FuelNode (POSIX 操作实现)
│   │   ├── handle.go      # 文件句柄管理
│   │   └── mount.go       # 挂载/卸载
│   ├── cache/             # 缓存管理层
│   │   ├── data.go        # 数据缓存 (NVMe LRU + ETag 校验)
│   │   ├── meta.go        # 元数据 L1 内存缓存 (TTL)
│   │   ├── neg.go         # 负缓存
│   │   ├── prefetch.go    # 预读 + 乱序检测
│   │   ├── eviction.go    # LRU 淘汰 (高低水位)
│   │   └── index.go       # 缓存索引持久化 (BoltDB)
│   ├── metadata/          # 元数据引擎层 (L2)
│   │   ├── engine.go      # MetadataEngine 接口
│   │   ├── oss.go         # 模式 A: 直查对象存储
│   │   ├── redis.go       # 模式 B: Redis
│   │   ├── mysql.go       # 模式 C: MySQL
│   │   └── types.go       # MetaEntry 数据结构
│   ├── objectstore/        # 对象存储后端 (INV-8: 可插拔)
│   │   ├── oss.go         # OSS 后端实现
│   │   ├── mock.go        # Mock 实现 (测试用)
│   │   ├── retry.go       # 重试/退避
│   │   └── factory.go     # 工厂函数 + 注册表
│   ├── config/            # 配置管理
│   │   └── config.go      # YAML + 环境变量 + 命令行
│   ├── monitor/           # 监控
│   │   ├── metrics.go     # Prometheus 指标
│   │   └── health.go      # 健康检查端点
│   └── deploy/            # K8s 部署支持
│       ├── daemonset.go   # DaemonSet 模式
│       ├── csi.go         # CSI Driver
│       └── sidecar.go     # Sidecar 模式
├── api/                   # 公共接口定义
│   ├── interfaces.go      # MetadataEngine / DataCache / ObjectStore 接口
│   └── types.go           # 核心数据结构 (MetaEntry / FileHandle / Config)
├── go.mod
├── go.sum
└── README.md
```

### 4.3 核心接口定义

```go
// api/interfaces.go

// ObjectStore 对象存储客户端接口 (INV-7: 模块边界通过接口隔离, INV-8: 后端可插拔)
type ObjectStore interface {
    Head(ctx context.Context, key string) (*ObjectMeta, error)
    Get(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error)
    Put(ctx context.Context, key string, r io.Reader, size int64) (*ObjectMeta, error)
    List(ctx context.Context, prefix, delimiter string, maxKeys int) ([]ObjectEntry, []string, error)
    Copy(ctx context.Context, srcKey, dstKey string) error
    Delete(ctx context.Context, key string) error
    Bucket() string
}

// MetadataEngine 元数据引擎接口 (INV-4: 元数据引擎是可选的加速层)
type MetadataEngine interface {
    GetAttr(ctx context.Context, path string) (*MetaEntry, error)
    SetAttr(ctx context.Context, path string, entry *MetaEntry) error
    DeleteAttr(ctx context.Context, path string) error
    ListDir(ctx context.Context, dirPath string) ([]DirEntry, error)
    SetDir(ctx context.Context, dirPath string, entries []DirEntry) error
    DeleteDir(ctx context.Context, dirPath string) error
    BatchGetAttr(ctx context.Context, paths []string) (map[string]*MetaEntry, error)
    Invalidate(ctx context.Context, path string) error
    HealthCheck(ctx context.Context) error
    Close() error
}

// DataCache 数据缓存接口 (INV-2: 缓存是对象存储对象的字节镜像)
// 缓存单位是整文件，返回本地文件路径，FUSE 层通过 pread 直读
type DataCache interface {
    Get(key, etag string) (localPath string, hit bool, err error)
    Put(key, etag string, size int64, r io.Reader) (localPath string, err error)
    Remove(key string) error
    Contains(key, etag string) bool
    Stats() CacheStats
}
```

> 详细接口变更理由见 [IMPL_DESIGN.md §4](./IMPL_DESIGN.md#4-接口定义)

### 4.4 数据流

#### 4.4.1 读路径

```
read(path, offset, size)
  │
  ├── 1. 元数据获取
  │     L1 内存缓存 (TTL 30s) → hit: 用缓存 etag/size
  │     L1 miss → L2 元数据引擎
  │       模式 A: 对象存储 HEAD → 返回 + 写回 L1
  │       模式 B: Redis GET → hit: 返回 + 写回 L1
  │       模式 B: Redis miss → 对象存储 HEAD → 写回 Redis + L1
  │       模式 C: MySQL SELECT → 同 B
  │
  ├── 2. 数据缓存查找
  │     DataCache.Get(key, etag) → hit: 返回 localPath
  │       → pread(localFile, offset, size) → 返回数据
  │       （零拷贝，内核 page cache 加速）
  │
  └── 3. 缓存未命中拉取（整文件缓存）
        singleflight 去重（多 worker 同时读同一文件只触发一次 GET）
        ObjectStore.Get(key, 0, 0) → 完整对象流
        DataCache.Put(key, etag, size, reader) → 流式写入 NVMe
        （临时文件 → atomic rename）
        打开缓存文件 → pread(localFile, offset, size) → 返回数据
```

#### 4.4.2 写路径

```
write(path, data)
  │
  ├── 1. 写本地临时文件
  ├── 2. PutObject → 对象存储 (INV-3: 不改变对象存储对象格式)
  ├── 3. 失效缓存
  │     L1: statCache.Delete(path) + dirCache.Delete(parentDir)
  │     L2: MetadataEngine.Invalidate(path)
  │     数据缓存: dataCache.Remove(path)
  └── 4. (可选) 写入新缓存
        dataCache.Put(path, newEtag, data)
```

#### 4.4.3 元数据查询

```
stat(path)
  │
  ├── L1 内存缓存 (TTL 30s) → hit → 返回
  ├── L1 miss → L2 元数据引擎
  │     模式 A: 对象存储 HEAD
  │     模式 B: Redis GET
  │     模式 C: MySQL SELECT
  ├── L2 hit → 写回 L1 → 返回
  └── L2 miss → 对象存储 HEAD (真相来源)
        存在 → 构造 MetaEntry → 写回 L2 + L1 → 返回
        不存在 → 写入负缓存 (60s) → 返回 ENOENT
```

---

## 5. 技术栈约束

### 5.1 必选技术栈

| 组件 | 选型 | 约束理由 |
|------|------|---------|
| 语言 | Go 1.21+ | 开发效率 + FUSE 库生态 + goroutine 并发模型 |
| FUSE 库 | `github.com/hanwen/go-fuse/v2` | JuiceFS 生产验证，Go 生态最成熟的 FUSE 库 |
| OSS SDK | `github.com/aliyun/aliyun-oss-go-sdk/oss` | 阿里云官方 SDK，原生对象存储 API |
| Redis 客户端 | `github.com/redis/go-redis/v9` | 元数据引擎模式 B |
| MySQL 驱动 | `github.com/go-sql-driver/mysql` | 元数据引擎模式 C |
| 监控 | `github.com/prometheus/client_golang` | 与现有监控体系一致 |
| 配置 | `gopkg.in/yaml.v3` | YAML + 环境变量 + 命令行（未引入 viper，符合 §11.3 不过度设计） |
| 日志 | `go.uber.org/zap` | 高性能结构化日志 |
| 缓存索引 | `go.etcd.io/bbolt` (可选) | LRU 索引持久化，嵌入式 KV |

### 5.2 禁止引入的技术

| 禁止项 | 理由 |
|--------|------|
| AWS SDK (v1 或 v2) | 使用阿里云 OSS 原生 SDK，不通过 S3 兼容 API |
| jacobsa/fuse | 已停止维护，使用 hanwen/go-fuse |
| Java / JVM 依赖 | FUSE 进程保持纯 Go，无 JVM 开销 |
| 外部 C 依赖 (CGO) | 保持纯 Go 编译，交叉编译友好（bbolt 是纯 Go） |
| 分布式协调服务 (etcd/ZooKeeper) | 不做分布式缓存，不需要集中式协调 |

### 5.3 参考项目（只读源码，不引入依赖）

| 项目 | 参考内容 |
|------|---------|
| goofys | S3→FUSE 路径映射、stat/readdir 实现、预读 + 乱序检测、Slurp 目录列表、BufferPool |
| JuiceFS | go-fuse 用法、元数据引擎 Key 设计（Redis/MySQL）、FUSE 挂载参数 |
| Mountpoint for S3 | 并发拉取策略、小文件批量处理、性能优化 |
| Fluid | CSI Driver 实现、Mutating Webhook、Runtime Interface |
| Alluxio CSI | CSI NodePublish/NodeStage 实现参考 |

---

## 6. 路径映射规范

### 6.1 路径对应关系

```
FUSE 挂载点:   /fuel/{bucket}
对象存储对象路径:  oss://{bucket}/{key}
FUSE 文件路径: /fuel/{bucket}/{key}
本地缓存路径:  {cache_dir}/{bucket}/{key}
```

### 6.2 示例

```
OSS:          oss://eabot-train-prod/training/2024-q1/frame_000001.jpg
FUSE:         /fuel/eabot-train-prod/training/2024-q1/frame_000001.jpg
本地缓存:      /nvme/cache/eabot-train-prod/training/2024-q1/frame_000001.jpg
```

### 6.3 目录语义

对象存储没有真正的目录，目录由对象 key 的前缀隐式构成。Fuel 的处理：

| 操作 | 对象存储行为 | 说明 |
|------|---------|------|
| `readdir(dir)` | `ListObjects(prefix=dir/)` | 列出前缀下的直接子项 |
| `mkdir(dir)` | 创建 0 字节对象 `dir/` | 显式目录标记（兼容 OSS-HDFS） |
| `rmdir(dir)` | 删除 `dir/` 对象 | 仅当目录为空时 |
| `stat(dir)` | HEAD `dir/` 对象 | 存在则返回目录属性；不存在则尝试 List 推断 |

---

## 7. 一致性模型

### 7.1 一致性保证

| 场景 | 保证级别 | 机制 |
|------|---------|------|
| 单节点读已写对象 | 强一致 | 写完 PutObject 后失效 L1 + L2 + 数据缓存 |
| 单节点读未变更对象 | 缓存命中即正确 | ETag 不变，缓存有效 |
| 跨节点读已写对象 | 最终一致 | L2 元数据引擎共享 + L1 TTL 过期后刷新 |
| 跨节点读未变更对象 | 各节点独立缓存 | ETag 校验保证正确性 |

### 7.2 写后读一致性

```
写完成 (PutObject 成功)
  → 主动失效 L1 (内存)
  → 主动失效 L2 (Redis/MySQL)
  → 主动删除数据缓存 (NVMe)
  → 后续读必然从对象存储获取最新 ETag → 重新拉取
```

### 7.3 缓存校验

| 校验时机 | 机制 | 对象存储请求 |
|---------|------|---------|
| `open()` | HEAD 获取 ETag，与本地缓存比对 | 1 次 HEAD（L1/L2 miss 时） |
| `read()` | 不重复校验（open 时已校验） | 0 |
| 文件不变期间 | L1 TTL 内不重复 HEAD | 0 |
| 元数据引擎缓存 | L2 miss → 对象存储 HEAD 回填 | 1 次 HEAD（首次访问） |

### 7.4 负缓存

文件不存在的场景写入负缓存（L1 内存 60s + L2 引擎 60s），避免重复 HEAD 不存在路径。

---

## 8. 配置规范

### 8.1 配置文件格式

```yaml
# fuel-config.yaml
storage:
  type: oss                            # oss | s3 | minio (INV-8: 后端可插拔)
  bucket: eabot-train-prod
  oss:
    endpoint: oss-cn-wulanchabu-internal.aliyuncs.com
  # AK/SK 通过环境变量 OSS_ACCESS_KEY_ID / OSS_ACCESS_KEY_SECRET 注入

metadata:
  engine: oss                    # oss | redis | mysql
  redis:
    address: ""                  # redis://host:6379/0
  mysql:
    dsn: ""                      # user:pass@tcp(host:3306)/fuel_metadata
  cache:
    statTTL: 30s
    dirTTL: 10s
    negTTL: 60s

cache:
  dir: /mnt/nvme/cache
  size: 1800000000000            # 1.8TB
  highWatermark: 0.85
  lowWatermark: 0.70
  blockSize: 4194304             # 4MB
  maxFileSize: 1073741824        # 1GB (超过不缓存, MVP)

prefetch:
  enabled: true
  concurrency: 4
  readahead:
    initial: 1048576             # 1MB
    max: 16777216                # 16MB

fuse:
  mountPoint: /fuel/eabot-train-prod
  maxRead: 1048576               # 1MB
  options:
    - allow_other
    - kernel_cache
    - auto_cache

monitor:
  metricsAddr: ":49999"
  logLevel: info
```

### 8.2 配置优先级

```
命令行参数 > 环境变量 > 配置文件 > 默认值
```

### 8.3 敏感信息

对象存储 AK/SK 和 Redis/MySQL 密码**不写入配置文件**，通过环境变量注入：

```
OSS_ACCESS_KEY_ID
OSS_ACCESS_KEY_SECRET
FUEL_REDIS_PASSWORD
FUEL_MYSQL_PASSWORD
```

---

## 9. 监控规范

### 9.1 指标命名

所有指标以 `fuel_` 为前缀：

```
fuel_cache_hit_total{type="data"}              # 数据缓存命中
fuel_cache_miss_total{type="data"}             # 数据缓存未命中
fuel_cache_size_bytes                           # 缓存已用空间
fuel_cache_capacity_bytes                       # 缓存配额
fuel_cache_eviction_total                       # LRU 淘汰次数
fuel_meta_hit_total{layer="l1"}                # 元数据 L1 命中
fuel_meta_hit_total{layer="l2"}                # 元数据 L2 命中
fuel_meta_miss_total                            # 元数据全部 miss
fuel_neg_cache_hit_total                        # 负缓存命中
fuel_oss_requests_total{operation="head"}       # 对象存储 HEAD 次数
fuel_oss_request_duration_seconds{operation="get"}  # 对象存储 GET 延迟
fuel_fuse_read_duration_seconds                 # 读延迟分布
fuel_fuse_operations_total{op="stat"}           # FUSE 操作计数
fuel_prefetch_total                             # 预读次数
fuel_prefetch_bytes_total                       # 预读字节数
fuel_process_memory_bytes                       # 进程内存 RSS
fuel_process_goroutine_count                    # goroutine 数
```

### 9.2 健康检查

```
GET /health → 200 OK (进程健康) / 503 (元数据引擎不可达等)
GET /metrics → Prometheus 格式指标
```

---

## 10. 部署规范

### 10.1 本地裸机

```bash
fuel mount \
  --config /etc/fuel/config.yaml \
  --oss-bucket eabot-train-prod \
  --mount-point /fuel/eabot-train-prod
```

systemd 管理进程生命周期，`Restart=always`。

### 10.2 K8s 三种模式

| 模式 | 适用场景 | 核心组件 |
|------|---------|---------|
| DaemonSet | GPU 训练节点（低 Pod 密度），与 Alluxio 对齐 | FUSE Pod (DaemonSet) + 应用 Pod (hostPath) |
| CSI Driver | 标准 K8s PVC 语义 | CSI NodePlugin + FUSE DaemonSet + StorageClass/PVC |
| Sidecar | 多租户 / Pod 级隔离 | 应用 Pod + FUSE Sidecar 容器 |

### 10.3 部署不变量

- FUSE 进程需要 `/dev/fuse` 设备访问权限
- FUSE 进程需要 `privileged` 或 `CAP_SYS_ADMIN` 安全上下文
- NVMe 缓存目录需要 hostPath 挂载
- 监控端口（默认 49999）需要暴露给 Prometheus scrape

---

## 11. 实施路线约束

### 11.1 分阶段原则

每个阶段必须**独立可验证**，前一阶段是后一阶段的前置条件：

```
Phase 1: 只读 MVP (模式 A: 直查对象存储)
  → 先证明"能读" + "缓存命中"

Phase 2: 性能优化
  → 再证明"读得快" (对比 Alluxio FUSE)

Phase 3: 元数据引擎 (模式 B/C) + 写路径
  → 然后扩展到"跨节点共享" + "能写"

Phase 4: 生产化 (K8s 部署 + 监控 + 故障恢复)
  → 最后实现"可运维"

Phase 5: K8s 深度集成 (CSI + Webhook + 编排层)
  → 远期目标
```

### 11.2 Phase 1 最小验证标准

Phase 1 完成后必须满足：
1. `fuel mount` 成功挂载
2. `ls /fuel/{bucket}/` 能列目录
3. `cat /fuel/{bucket}/path/to/file` 能读文件
4. 二次读同一文件命中本地缓存（通过日志或指标确认）
5. ETag 变化后缓存自动失效

### 11.3 不做过度设计

| 不做 | 理由 |
|------|------|
| 不做多后端支持 | OSS 是唯一后端，不引入多后端抽象 |
| 不做分布式数据缓存 | INV-6 约束，MVP 不做跨节点数据共享 |
| 不做多级缓存 (MEM + NVMe) | MVP 只做 NVMe 单层，验证后再扩展 |
| 不做 P2P 加速 | 远期目标，不提前实现 |
| 不做 CSI Driver (Phase 1-4) | 延迟到 Phase 5，先做 FUSE 本身 |
| 不做 Webhook (Phase 1-4) | 延迟到 Phase 5 |
| 不做 xattr / flock / symlink | 训练场景不需要 |

---

## 12. 文档与代码规范

### 12.1 代码组织

- 每个模块在 `internal/` 下独立目录
- 公共接口和类型在 `api/` 下
- 入口在 `cmd/fuel/`
- 测试与被测代码同目录（`foo_test.go`）

### 12.2 命名规范

| 类型 | 规范 | 示例 |
|------|------|------|
| 包名 | 全小写，单数 | `cache`, `metadata`, `oss` |
| 接口 | 名词或 `er` 后缀 | `ObjectStore`, `MetadataEngine`, `DataCache` |
| 结构体 | 名词 | `FileHandle`, `CacheItem`, `MetaEntry` |
| 配置字段 | YAML 小驼峰, Go 结构体大驼峰 | `statTTL` / `StatTTL` |
| 指标 | `fuel_` 前缀 + 下划线 | `fuel_cache_hit_total` |
| 日志 | 结构化 JSON (zap) | `{"level":"info","msg":"cache hit"}` |

### 12.3 错误处理

- 外部错误（对象存储 / Redis / MySQL）必须包装上下文: `fmt.Errorf("head object %s: %w", key, err)`
- 用户可见错误返回 POSIX errno: `syscall.ENOENT`, `syscall.EIO`
- 内部错误带结构化日志

### 12.4 测试

| 测试类型 | 覆盖范围 | 工具 |
|---------|---------|------|
| 单元测试 | 每个模块的接口实现 | Go testing + mock |
| 集成测试 | FUSE 挂载 + 真实对象存储读取 | Go testing + build tag `integration` |
| Benchmark | 读延迟 / 吞吐 / 缓存命中 | Go benchmark + pprof |

---

## 13. 附录

### 13.1 核心数据结构

```go
// api/types.go

// MetaEntry 文件/目录元数据 (包含 POSIX stat 所需全部字段)
type MetaEntry struct {
    Path        string    `json:"path"`
    Inode       uint64    `json:"inode"`                   // FNV-1a(path)，go-fuse inode 管理
    Size        int64     `json:"size"`
    ETag        string    `json:"etag"`
    Mode        uint32    `json:"mode"`                    // 文件 0644，目录 0755
    Uid         uint32    `json:"uid"`                     // 挂载进程 UID
    Gid         uint32    `json:"gid"`                     // 挂载进程 GID
    MTime       time.Time `json:"mtime"`
    ATime       time.Time `json:"atime"`                   // 本地维护，不回写对象存储
    Nlink       uint32    `json:"nlink"`                   // 文件 1，目录 2
    IsDir       bool      `json:"isDir"`
    ContentType string    `json:"contentType,omitempty"`
}

// DirEntry 目录列表项（内联元数据，避免 N+1 HEAD 请求）
type DirEntry struct {
    Name  string     `json:"name"`     // 子项名称（不含路径前缀）
    IsDir bool       `json:"isDir"`
    Meta  *MetaEntry `json:"meta"`     // 来自 ListObjects 的内联元数据
}

// ObjectMeta 对象存储对象元数据 (来自 HEAD)
type ObjectMeta struct {
    Key          string
    Size         int64
    ETag         string
    LastModified time.Time
    ContentType  string
}

// ObjectEntry 对象存储对象列表项 (来自 List)
type ObjectEntry struct {
    Key  string
    Size int64
}

// FileHandle 打开的文件句柄
type FileHandle struct {
    Path      string
    Flags     uint32
    Meta      *MetaEntry
    CacheFile *os.File
    Offset    int64
}

// CacheStats 缓存统计
type CacheStats struct {
    HitCount    int64
    MissCount   int64
    UsedSize    int64
    Capacity    int64
    EntryCount  int64
    EvictionCount int64
}

// Config 全局配置
type Config struct {
    Storage   StorageConfig   `yaml:"storage"`
    Metadata  MetadataConfig  `yaml:"metadata"`
    Cache     CacheConfig     `yaml:"cache"`
    Prefetch  PrefetchConfig  `yaml:"prefetch"`
    Fuse      FuseConfig      `yaml:"fuse"`
    Monitor   MonitorConfig   `yaml:"monitor"`
}
```

### 13.2 关联文档索引

| 文档 | 位置 | 说明 |
|------|------|------|
| idea.md | `design/idea.md` | 项目背景与目标 |
| fuse-cache-node-design.md | `fuse_implement/fuse-cache-node-design.md` | 详细设计文档（v2） |
| estore-fuse-vs-goofys.md | `fuse_implement/estore-fuse-vs-goofys.md` | goofys 对比分析 |
| fork-vs-build-from-scratch.md | `fuse_implement/fork-vs-build-from-scratch.md` | Fork vs 自建评估 |
| self-build-route-evaluation.md | `design/self-build-route-evaluation.md` | 自建路线评估 |
| go-alluxio-fuse-feasibility.md | `design/go-alluxio-fuse-feasibility.md` | Go Alluxio FUSE 可行性 |
| alluxio_arch.md | `design/alluxio_arch.md` | 现有 Alluxio 架构 |
