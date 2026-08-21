# Fuel 实施计划

> 版本: v1.0
> 日期: 2026-08-15
> 关联文档: [ARCH_SPEC.md](./ARCH_SPEC.md) | [IMPL_DESIGN.md](./IMPL_DESIGN.md) | [AGENTS.md](../AGENTS.md)

---

## 1. 计划总览

```
Phase 1: 只读 MVP (模式 A: 直查对象存储)       3 周
Phase 2: 性能优化 + Benchmark                   2 周
Phase 3: 元数据引擎 (模式 B/C) + 写路径         3 周
Phase 4: 生产化 (第一期部署: K8s DaemonSet + 监控)   2 周
Phase 5: 第二期部署 (CSI Driver + Sidecar + PVC)     3 周
Phase 6: 多后端扩展 (按需)                       按需
```

总工期：**13 周（约 3 个月）**，其中 Phase 1-4 为 MVP 到生产可用的核心路径（10 周，第一期部署用 DaemonSet），Phase 5 为第二期部署（K8s 标准化：CSI + Sidecar，3 周）。

### 部署分期说明

- **第一期部署（Phase 4）**：DaemonSet + hostPath。面向 GPU 训练节点，应用 Pod 通过 hostPath + `mountPropagation: HostToContainer` 访问节点级 FUSE 挂载点 `/fuel/{bucket}`。
- **第二期部署（Phase 5）**：CSI Driver（标准 PVC 语义，应用 Pod 不感知 FUSE）+ Sidecar（多租户 Pod 级隔离）。三种模式下应用 Pod 均不运行 FUSE、不需要 privileged，详见 ARCH_SPEC §10.3。

### 关键里程碑

| 里程碑 | Phase | 交付物 | 验收标准 |
|--------|-------|--------|---------|
| **M1: 能读** | Phase 1 | 可挂载的只读 FUSE 文件系统 | `ls` / `cat` 正常工作，缓存命中可验证 |
| **M2: 读得快** | Phase 2 | 性能优化 + Benchmark 报告 | 缓存命中 P50 < 1ms，吞吐 > 2 GB/s |
| **M3: 能写 + 跨节点** | Phase 3 | 写路径 + Redis/MySQL 元数据引擎 | 写后读一致，跨节点元数据共享 |
| **M4: 可运维** | Phase 4 | systemd + K8s DaemonSet + 监控 | 7 天无故障运行，监控指标齐全 |
| **M5: 标准 K8s** | Phase 5 | CSI Driver + PVC + 应用 Pod 透明挂载 | 标准 K8s 语义，PVC 挂载正常 |

---

## 2. Phase 1: 只读 MVP（3 周）

> 目标：`fuel mount` 挂载后能 `ls` + `cat`，二次读命中本地缓存。模式 A（直查对象存储），无外部依赖。

### Week 1: 项目骨架 + 核心类型 + 对象存储后端

> ✅ **已完成**（2026-08-15）。单元测试全部通过：`go test ./...`（api 100% / config 95.8% / objectstore 64.2%，objectstore 未覆盖部分为真实 OSS I/O，需集成测试覆盖）。
> 实现差异已同步至 IMPL_DESIGN/ARCH_SPEC：配置用 yaml.v3 而非 viper，命令框架用标准库 flag 而非 cobra，环境变量 OSS_* 与 FUEL_STORAGE_* 双兼容。

#### 1.1 项目初始化 ✅

```
任务: 创建 Go 项目骨架
文件:
  go.mod / go.sum
  cmd/fuel/main.go          ← 入口, 子命令: mount / version
  api/interfaces.go          ← ObjectStore / MetadataEngine / DataCache 接口 (见 IMPL_DESIGN §4)
  api/types.go               ← MetaEntry / DirEntry / ObjectMeta / ObjectEntry / CacheStats / Config
验证:
  go mod init fuel
  go mod tidy
  go build ./cmd/fuel        ← 编译通过
```

**实际交付**: `go.mod`/`go.sum`、`cmd/fuel/main.go`（flag 子命令：mount/version）、`cmd/fuel/mount.go`（mount 骨架，FUSE 挂载留待 Week 3）、`api/interfaces.go`、`api/types.go`（含 `InodeFromPath`/`MetaEntryFromObjectMeta`/`DirMetaEntry` 及测试）。`fuel version`/`fuel mount` 可运行。

#### 1.2 配置模块 ✅

```
任务: 实现配置加载
文件:
  internal/config/config.go  ← YAML 加载 + 环境变量覆盖 + 命令行参数
  internal/config/config_test.go
验证:
  能加载 fuel-config.yaml
  环境变量 OSS_ACCESS_KEY_ID / OSS_ACCESS_KEY_SECRET (或 FUEL_STORAGE_ACCESS_KEY / FUEL_STORAGE_ACCESS_SECRET) 正确注入
  命令行参数覆盖配置文件
  go test ./internal/config/... 通过
```

**实际交付**: `internal/config/config.go`（yaml.v3 加载 + 默认值 + 环境变量注入 + 必填校验），测试覆盖 95.8%。AK/SK 双变量兼容，`OSS_*` 优先。

#### 1.3 对象存储后端 (OSS) ✅

```
任务: 实现对象存储 ObjectStore 接口
文件:
  internal/objectstore/factory.go  ← ObjectStoreFactory + RegisterObjectStore + NewObjectStore
  internal/objectstore/oss.go      ← OSS 实现 (Head/Get/Put/List/Copy/Delete/Bucket)
  internal/objectstore/mock.go     ← Mock 实现 (测试用)
  internal/objectstore/retry.go    ← 重试 + 指数退避
  internal/objectstore/oss_test.go ← 单元测试 (使用 mock)
  internal/objectstore/oss_integration_test.go ← 集成测试 (build tag: integration)
  internal/objectstore/retry_test.go ← 重试单元测试
注意:
  List 接口返回 ([]ObjectEntry, []string, error)，第二个返回值是 common prefixes（子目录）
  Get 接口: length=0 表示读到末尾
验证:
  go test ./internal/objectstore/... 通过 (mock 测试)
  go test -tags=integration ./internal/objectstore/... 通过 (真实对象存储)
  Head / Get(Range) / List / Copy / Delete 行为正确
  错误处理: 404 → ENOENT, 网络超时 → 重试, 403 → EACCES
```

**实际交付**: 全部文件就绪。额外修复一个 bug：`isRetryable` 曾把 `syscall.Errno`（如 ENOENT/EACCES，实现了 `net.Error.Timeout()`）误判为可重试网络错误，已在 net.Error 判定前排除 errno。单元测试通过（含 mock 契约、错误映射、重试逻辑）。

### Week 2: 缓存层 + 元数据引擎 (模式 A)

#### 2.1 元数据引擎接口 + 直查模式 ✅

```
任务: 实现 MetadataEngine 接口 + DirectEngine (模式 A)
文件:
  internal/metadata/factory.go     ← MetadataEngine 工厂
  internal/metadata/direct.go      ← 模式 A: 直查对象存储 (调用 ObjectStore.Head / List)
  internal/metadata/direct_test.go ← 单元测试
注意:
  ListDir 返回 []DirEntry（内联元数据），不是 []string
  Invalidate 语义: 删除 path 及其所有子路径的缓存
验证:
  GetAttr / ListDir / SetAttr / Invalidate 行为正确
  通过 ObjectStore 获取元数据
  go test ./internal/metadata/... 通过
```

**实际交付**（2026-08-15）: `factory.go`（`MetadataEngineFactory` + `RegisterMetadataEngine` + `NewMetadataEngine`，签名带 `ObjectStore` 依赖以支持直查/降级）、`direct.go`、`direct_test.go`。测试覆盖 94.4%。

**实现要点**:
- `GetAttr` 目录推断顺序（ARCH_SPEC §6.3）: 根 → HEAD key（文件）→ HEAD `key/`（显式目录标记）→ List `key/` 有子项（隐式目录）→ 否则 ENOENT。路径经 `normalizeKey` 去前导/尾部 `/`。
- `ListDir` 用 `List(prefix, delimiter="/")` 一次取文件 + 子目录前缀，内联 `MetaEntry`（预填充 L1，避免 N+1 HEAD）；过滤掉 `key/` 形式的目录标记对象。
- `SetAttr/SetDir/DeleteAttr/DeleteDir/Invalidate/Close` 为 no-op（direct 无本地存储，直查即最新）。
- `HealthCheck` 通过 bucket 根 `List(maxKeys=1)` 探测可达性。
- 错误传播：Head/List 的非 ENOENT 错误（EACCES/EIO）原样包装上传，仅 ENOENT 触发目录推断链。

#### 2.2 数据缓存 (NVMe LRU) ✅

```
任务: 实现 NVMe 数据缓存
文件:
  internal/cache/data.go         ← DataCache 实现 (Get/Put/Remove/Exists/Stats)
  internal/cache/eviction.go     ← LRU 淘汰 (高低水位)
  internal/cache/index.go        ← 缓存索引 (内存 map, MVP 不持久化)
  internal/cache/data_test.go    ← 单元测试
  internal/cache/eviction_test.go
验证:
  缓存文件路径 = {cache_dir}/{bucket}/{key} (INV-2)
  缓存内容为对象字节镜像
  ETag 校验: 匹配 → hit, 不匹配 → miss + 删除
  LRU 淘汰: 超过高水位 → 删除最久未访问 → 低于低水位
  go test ./internal/cache/... 通过
```

**实际交付**（2026-08-15）: `index.go`（`cacheIndex` = map + `container/list` LRU 双向链表，Front 最近访问）、`eviction.go`（高低水位淘汰）、`data.go`（`nvmeCache` 实现 `api.DataCache`）、`data_test.go` + `eviction_test.go`。测试覆盖 88.9%，`go test -race` 干净。

**实现要点**:
- 构造函数 `NewNVMeCache(dir, bucket, capacity, highWatermark, lowWatermark, maxFileSize)`，bucket 注入以构成路径 `{dir}/{bucket}/{key}`（INV-2）。
- **写入**：`os.MkdirAll` 父目录 → 同目录 `CreateTemp` → `io.Copy` 流式 → `fsync` → `os.Rename`（原子，同文件系统）。临时文件任何失败都清理。
- **ETag 校验**：`Get` 时索引 etag 不匹配 → 删条目+文件 → miss；文件被外部删除 → 清索引 → miss。
- **LRU 淘汰**：写入前 `used+incoming > 高水位` → `evictFor` 淘汰到 `used+incoming <= 低水位`；写遇 `ENOSPC` 再淘汰重试一次，仍失败返回错误（上层降级不缓存，IMPL_DESIGN §9.2）。
- **并发**：hits/misses 用 `atomic.Int64`；索引用 `sync.Mutex`（淘汰需全局一致视图，IMPL_DESIGN §7.2）。
- **防御**：`sanitizeKey` 拒绝空/`..`/绝对路径 key；`maxFileSize` 超限不缓存。
- 同 key 重复 Put 覆盖，used 不重复累计（先减旧 size）。

#### 2.3 元数据 L1 内存缓存

```
任务: 实现 L1 内存缓存 (TTL)
文件:
  internal/cache/meta.go         ← statCache + dirCache + negCache (TTL)
  internal/cache/neg.go          ← 负缓存
  internal/cache/meta_test.go    ← 单元测试
验证:
  stat 缓存 TTL 30s
  dir 缓存 TTL 10s
  负缓存 TTL 60s
  过期自动失效
  go test ./internal/cache/... 通过
```

### Week 3: FUSE 接口层 + 挂载 + 端到端验证

#### 3.1 FUSE 接口层

```
任务: 实现 FUSE 接口层 (只读)
文件:
  internal/fuse/server.go        ← FUSE Server 生命周期 (New/Mount/Unmount/Wait)
  internal/fuse/node.go          ← FuelRoot + FuelNode (fs.InodeEmbedder 实现)
  internal/fuse/handle.go        ← fileHandle 管理 (读写状态)
  internal/fuse/mount.go         ← 挂载参数 + 内核选项
  internal/fuse/node_test.go     ← 单元测试 (mock 依赖)
实现操作 (只读, 基于 go-fuse fs.InodeEmbedder API):
  Getattr(path)         → MetaCache → MetadataEngine.GetAttr → 返回 POSIX stat
  Lookup(parent, name)  → MetaCache → MetadataEngine.GetAttr → 返回 FuelNode
  Open(path, READ)      → DataCache.Get → 命中返回 fd; 未命中暂不拉取
  Read(fd, offset, size) → 缓存命中: pread(cacheFile); 未命中: singleflight 拉取整文件
  Readdir(dir)          → MetaCache → MetadataEngine.ListDir → 返回 []DirEntry
注意:
  使用 go-fuse v2 fs.InodeEmbedder API（非 pathfs），参考 JuiceFS 实现
  DataCache.Get/Put 返回本地文件路径，FUSE 层通过 pread 零拷贝读取
  singleflight 去重并发缓存未命中拉取
验证:
  go test ./internal/fuse/... 通过 (mock ObjectStore + mock MetadataEngine)
```

#### 3.2 入口命令

```
任务: 实现 fuel mount 命令
文件:
  cmd/fuel/main.go               ← 入口
  cmd/fuel/mount.go              ← mount 子命令
  cmd/fuel/version.go            ← version 子命令
验证:
  fuel mount --config fuel-config.yaml 成功挂载
  fuel version 输出版本信息
```

#### 3.3 端到端验证

```
任务: 端到端验证 (本地, 使用真实对象存储)
验证标准 (ARCH_SPEC.md §11.2):
  1. fuel mount 成功挂载
  2. ls /fuel/{bucket}/ 能列目录
  3. cat /fuel/{bucket}/path/to/file 能读文件
  4. 二次读同一文件命中本地缓存 (通过日志确认)
  5. ETag 变化后缓存自动失效
测试数据:
  在对象存储 bucket 中准备 100 个测试文件 (1KB-10MB)
  在对象存储 bucket 中准备 1 个 1GB 大文件
```

### Phase 1 交付物清单

```
文件:
  go.mod / go.sum
  cmd/fuel/main.go / mount.go / version.go
  api/interfaces.go / types.go
  internal/config/config.go / config_test.go
  internal/objectstore/factory.go / oss.go / mock.go / retry.go / oss_test.go
  internal/metadata/factory.go / direct.go / direct_test.go
  internal/cache/data.go / meta.go / eviction.go / index.go / *_test.go
  internal/fuse/server.go / node.go / handle.go / mount.go / node_test.go

验证:
  全部单元测试通过: go test ./...
  集成测试通过: go test -tags=integration ./internal/objectstore/...
  端到端: ls + cat + 缓存命中 + ETag 失效

预估代码量: ~3500 行
```

---

## 3. Phase 2: 性能优化 + Benchmark（2 周）

> 目标：缓存命中读延迟 P50 < 1ms，吞吐 > 2 GB/s。对比 Alluxio FUSE benchmark。

### Week 4: 预读 + 并发拉取 + 小文件优化

#### 4.1 预读策略

```
任务: 实现顺序读检测 + 倍增预读
文件:
  internal/cache/prefetch.go     ← 预读器
  internal/cache/prefetch_test.go
逻辑:
  顺序读检测: 连续 read 的 offset 递增
  倍增预读: 1MB → 2MB → 4MB → 8MB → 16MB 封顶
  乱序读检测: numOOORead > 3 时禁用预读 (借鉴 goofys)
  异步预读: 后台 goroutine GET Range + 写缓存
验证:
  go test ./internal/cache/... 通过
  顺序读文件时预读触发
  乱序读时预读自动禁用
```

#### 4.2 并发拉取

```
任务: 大文件多 block 并发 GET Range
文件:
  internal/cache/prefetch.go     ← 并发拉取逻辑
逻辑:
  缓存未命中大文件 (> blockSize) 时, 按 4MB block 并发拉取
  并发度 = config.prefetch.concurrency (默认 4)
  每个 goroutine GET Range [blockStart, blockEnd)
  全部完成后 rename 临时文件为正式缓存
验证:
  100MB 文件拉取时间 ≤ 单线程 1/4 (理想情况)
  go test ./internal/cache/... 通过
```

#### 4.3 小文件批量预取 + 元数据批量预取

```
任务: 小文件批量预取 + readdir 元数据并行预取
文件:
  internal/cache/prefetch.go     ← 批量预取
  internal/fuse/ops.go           ← readdir 时并行预取 stat
逻辑:
  检测连续小文件读 (同目录) → 批量预取后续文件
  readdir 时, 对目录下所有文件并行 HEAD 预取元数据
验证:
  连续读 10 个小文件时, 后续 3-10 个自动预取
  readdir 后目录下文件 stat 延迟 < 1ms (L1 命中)
```

#### 4.4 FUSE 内核参数调优

```
任务: 挂载参数优化
文件:
  internal/fuse/mount.go         ← 挂载参数
参数:
  MaxRead:      1 << 20          # 1MB 单次读
  MaxBackground: 128              # 后台请求队列
  Options:      large_read, kernel_cache, auto_cache
验证:
  benchmark 前后对比读吞吐
```

### Week 5: BufferPool + Benchmark

#### 5.1 BufferPool 内存管理

```
任务: 实现 BufferPool (借鉴 goofys, cgroup 感知)
文件:
  internal/cache/buffer.go       ← BufferPool (sync.Pool, 5MB)
  internal/cache/buffer_test.go
逻辑:
  sync.Pool 复用 5MB buffer
  cgroup 感知: 读取容器内存限制, 动态调整 buffer 池大小
验证:
  go test ./internal/cache/... 通过
  内存使用不超过配置上限
```

#### 5.2 Benchmark

```
任务: 性能 benchmark, 对比 Alluxio FUSE
文件:
  internal/benchmark/read_test.go      ← 读 benchmark
  internal/benchmark/meta_test.go      ← 元数据 benchmark
测试场景 (ARCH_SPEC.md §GOAL-2/3/4):
  场景 1: 海量小文件顺序读 (10K files, 100KB each)
    指标: 吞吐 / 延迟 P50 / P99
  场景 2: 大文件顺序读 (1GB file)
    指标: 吞吐
  场景 3: 多并发读 (8 并发, 同数据集)
    指标: 吞吐 / 延迟 P99
  场景 4: 缓存命中二次读
    指标: 延迟 P50
  场景 5: 首次冷启动读 (cache miss)
    指标: 吞吐
  场景 6: 元数据操作 (stat 10K files)
    指标: 耗时
  场景 7: 目录列表 (readdir 1K files)
    指标: 耗时
验证标准:
  缓存命中读延迟 P50 < 1ms (GOAL-2)
  缓存命中读延迟 P99 < 5ms
  顺序读吞吐 > 2 GB/s (GOAL-2)
  缓存命中率 > 90% (热数据 < NVMe 70%) (GOAL-4)
  缓存未命中读延迟 P50 < 50ms (GOAL-3)
  元数据 stat P50 < 5ms (L1 命中) (GOAL-2)
产出:
  benchmark 报告 (含与 Alluxio FUSE 的对比数据)
```

### Phase 2 交付物清单

```
文件:
  internal/cache/prefetch.go / prefetch_test.go
  internal/cache/buffer.go / buffer_test.go
  internal/benchmark/read_test.go / meta_test.go
  benchmark 报告

预估新增代码量: ~1500 行
```

---

## 4. Phase 3: 元数据引擎 + 写路径（3 周）

> 目标：Redis/MySQL 元数据引擎跨节点共享，写路径完成（PutObject + 缓存失效），写后读一致。

### Week 6: Redis 元数据引擎

#### 6.1 Redis Engine ✅

```
任务: 实现 Redis MetadataEngine
文件:
  internal/metadata/redis.go       ← RedisEngine 实现
  internal/metadata/redis_test.go  ← 单元测试 (使用 miniredis 或 mock)
逻辑:
  Key 设计:
    meta:{bucket}/{path}  → JSON MetaEntry (不过期, 写路径主动删)
    dir:{bucket}/{dir}/   → Hash 子项列表 (不过期, 写路径主动删)
    neg:{bucket}/{path}   → "1" (TTL 60s)
  读: Redis GET → hit: 返回; miss: ObjectStore.Head → 写回 Redis + L1
  写: PutObject 后 → SET meta + DEL neg + 失效 dir 缓存
  跨节点共享: 所有节点读同一 Redis
验证:
  go test ./internal/metadata/... 通过
  Redis 元数据回填: 首次 HEAD 对象存储 → 写入 Redis → 二次读 Redis 命中
  跨节点: 节点 A 写入后, 节点 B 可读到
```

### Week 7: MySQL 元数据引擎

#### 7.1 MySQL Engine ✅

```
任务: 实现 MySQL MetadataEngine
文件:
  internal/metadata/mysql.go       ← MysqlEngine 实现
  internal/metadata/mysql_test.go  ← 单元测试 (使用 sqlmock 或测试 MySQL)
  internal/metadata/schema.sql     ← 建表语句
表结构:
  fuel_inodes (id, path, bucket, size, etag, mtime, is_dir, content_type, created_at, updated_at)
  fuel_dentries (id, parent_path, name, inode_id)
逻辑:
  读: SELECT → hit: 返回; miss: ObjectStore.Head → INSERT/UPSERT → 返回
  写: PutObject 后 → UPSERT + DELETE dentry + 失效 dentry 缓存
  持久化: 进程重启后元数据不丢失
验证:
  go test ./internal/metadata/... 通过
  MySQL 元数据回填和持久化
  跨节点: 节点 A 写入后, 节点 B 可读到
  FUSE 进程重启后元数据不丢失
```

### Week 8: 写路径 + 一致性验证

#### 8.1 写路径实现 ✅

```
任务: 实现 FUSE 写路径
文件:
  internal/fuse/ops.go             ← 写操作实现
  internal/fuse/handle.go          ← fileHandle 写状态 + flush
新增操作:
  Create(path)     → 创建 FileHandle (tmp 写句柄)
  Write(fd, data)  → 写本地临时文件 (tmp.WriteAt)
  Flush(fd)        → 临时文件 PutObject → 失效 L1 + L2 + 数据缓存 → SetStat 新元数据 → 尽力回写数据缓存
  Fsync(fd)        → 同 Flush
  Open(O_WRONLY|O_TRUNC) → 整文件覆盖写
  Unlink(path)     → ObjectStore.Delete → 失效缓存
  Rename(old, new) → ObjectStore.Copy + Delete → 失效两端缓存 (仅文件; 目录 ENOTSUP)
  Mkdir(dir)       → ObjectStore.Put 零字节占位对象 → SetStat 目录元数据
  Rmdir(dir)       → 非空检查 → ObjectStore.Delete 占位对象
验证:
  写文件 → 立即读 → 读到最新数据 ✅
  写文件 → 跨节点读 → 读到最新数据 (L2 引擎共享) ✅
  删除文件 → 读 → ENOENT ✅
  go test ./internal/fuse/... 通过 ✅
实现说明 (2026-08):
  写语义: 一次写多次读。O_RDWR / O_APPEND / 无 O_TRUNC 的原地写 → ENOTSUP；
  Flush 后再 Write → ENOTSUP。写临时文件落 {cache.dir}/{bucket}/.fuel-write-*，
  崩溃残留由 NewNVMeCache 启动时 cleanOrphanTemps 清理。
  失效顺序 (ARCH_SPEC §7.2): L1(DeleteStat/DeleteNeg/InvalidatePrefix/DeleteDir父目录)
  → L2(metaEng.Invalidate，失败降级靠 TTL) → 数据缓存(Remove，失败靠 ETag 不匹配自愈)。
  L2/数据缓存失效失败不掩盖写成功（INV-1 真相来源在对象存储）。
```

#### 8.2 写后读一致性验证 ✅

```
任务: 端到端写后读一致性测试
文件:
  internal/fuse/write_test.go    ← 写后读一致性测试
测试场景:
  场景 1: 写文件 → 同节点立即读 → 读到新数据 ✅ (+ INV-3 字节镜像断言)
  场景 2: 写文件 → 同节点 L1 TTL 过期后读 → 读到新数据 ✅ (与场景 4 合并)
  场景 3: 写文件 → 跨节点读 (模式 B Redis, miniredis) → 读到新数据 ✅
  场景 4: 覆盖写 → 缓存失效 → 读到新数据 ✅
  场景 5: 删除 → 读到 ENOENT ✅
  场景 6: rename → 旧路径 ENOENT, 新路径可读 ✅
  场景 7: mkdir → readdir 可见 ✅
  场景 8: rmdir → readdir 不可见 ✅
  边界: 零字节文件 / Flush 后写 ENOTSUP / 无 O_TRUNC 覆盖 ENOTSUP /
       O_TRUNC Open 覆盖 / Unlink 目录 EISDIR / Rmdir 非空 ENOTEMPTY /
       Rename 源缺失 ENOENT / Fsync 等价 Flush ✅
验证标准 (ARCH_SPEC.md §7):
  单节点写后读: 强一致 ✅
  跨节点写后读: 最终一致 (L1 TTL 过期后) ✅
```

#### 8.3 元数据引擎模式切换验证 ✅

```
任务: 验证三种模式可切换
文件:
  internal/fuse/mode_test.go     ← 模式切换测试
测试:
  模式 A (direct) → 模式 B (redis) 切换后 FUSE 写读删行为一致 ✅
    (TestModeSwitch_DirectRedis_IdenticalBehavior)
  元数据引擎不可用时自动降级直查 ✅
    (TestModeSwitch_RedisDown_DegradesToDirect)
  模式 C (mysql): 引擎契约由 internal/metadata 的 sqlmock 测试覆盖
    (工厂不支持注入 *sql.DB，FUSE 层行为经统一接口归纳成立)
```

### Phase 3 交付物清单

```
文件:
  internal/metadata/redis.go / redis_test.go
  internal/metadata/mysql.go / mysql_test.go / schema.sql
  internal/fuse/ops.go (扩展写操作)
  internal/fuse/write_test.go

验证:
  Redis 元数据引擎跨节点共享
  MySQL 元数据持久化 + 重启恢复
  写路径完整 (create/write/flush/unlink/rename/mkdir/rmdir)
  写后读一致性 (单节点强一致, 跨节点最终一致)
  模式切换验证

预估新增代码量: ~2500 行
```

---

## 5. Phase 4: 生产化 — 第一期部署 (K8s DaemonSet + 监控)（2 周）

> 目标：systemd + K8s DaemonSet 部署 + Prometheus 监控 + 故障恢复。7 天无故障运行。

### Week 9: 监控 + 日志 + 健康检查

#### 9.1 Prometheus 指标 ✅

```
任务: 实现全部监控指标
文件:
  internal/monitor/metrics.go      ← 指标定义 + FuelCollector 采集 + InstrumentStore 装饰器
  internal/monitor/http.go         ← /metrics + /health HTTP 端点
  internal/monitor/metrics_test.go / http_test.go
指标 (ARCH_SPEC.md §9.1):
  fuel_cache_hit_total{type="data"} / fuel_cache_miss_total{type="data"}  ✅ (FuelCollector 读 DataCache.Stats)
  fuel_cache_size_bytes / fuel_cache_capacity_bytes / fuel_cache_eviction_total / fuel_cache_entries  ✅
  fuel_meta_hit_total{layer="l1"} / fuel_meta_miss_total / fuel_neg_cache_hit_total  ✅ (MetaCache.Stats; L2 命中无计数 → §11 D11)
  fuel_storage_requests_total{operation} / fuel_storage_request_duration_seconds{operation}  ✅
    (InstrumentStore 装饰器，INV-8 后端中立; ARCH_SPEC 的 oss_ 前缀改为 storage_，多后端时再加 backend label)
  fuel_fuse_read_duration_seconds / fuel_fuse_operations_total{op}  ✅ (FUSE 层 IncFuseOp/ObserveFuseRead)
  fuel_prefetch_total / fuel_prefetch_bytes_total  ✅ (批量预取粒度，fuse 层上报)
  fuel_process_memory_bytes / fuel_process_goroutine_count  ✅ (runtime.MemStats/NumGoroutine)
健康检查:
  GET /health → 200 OK / 503 (checker 失败，3s 超时兜底)  ✅
  GET /metrics → promhttp 默认注册表  ✅
挂载接线 (cmd/fuel/mount.go):
  store 经 monitor.InstrumentStore 包装；FuelCollector 注册默认注册表；
  monitor.Server 以 metaEng.HealthCheck 为 checker 启动；
  监控端口绑定失败仅 WARN 不阻塞挂载（观测性组件降级不影响数据面）；
  卸载后 mon.Stop + metaEng.Close。
验证:
  curl localhost:49999/metrics 返回全部指标 ✅ (TestServer_Metrics)
  curl localhost:49999/health 返回 200 / 503 ✅ (TestServer_Health*)
```

#### 9.2 日志体系 ✅

```
任务: zap 结构化日志
文件:
  internal/monitor/log.go          ← zap JSON logger 构造（NewLogger，zap.ReplaceGlobals 全局生效）
  internal/monitor/log_test.go     ← JSON 格式 + 级别过滤测试
  接入点（zap.L() 全局 logger，mount 启动时 ReplaceGlobals）:
    cmd/fuel/mount.go              ← 启动参数 / mounted / unmount / 信号处理 (INFO)
    internal/objectstore/retry.go  ← 重试 WARN（op/key/attempt/delay）、耗尽 ERROR
    internal/cache/eviction.go     ← LRU 淘汰 INFO（entries/freedBytes）、删除失败 WARN
    internal/cache/verify.go       ← 巡检损坏剔除 WARN、外部丢失 DEBUG
    internal/metadata/redis.go     ← redis 失败降级 direct WARN (getattr/listdir/mget)
    internal/metadata/mysql.go     ← mysql 失败降级 direct WARN (getattr/listdir)
    internal/fuse/handle.go        ← read 缓存命中/未命中 DEBUG、回源失败 ERROR
    internal/fuse/node.go          ← getAttr 引擎故障 ERROR
    internal/fuse/ops.go           ← 写路径 store 失败 ERROR（Week 8 已建）
规范 (AGENTS.md §3.4):
  INFO: 挂载/卸载、缓存淘汰 ✅
  WARN: 对象存储请求失败重试、缓存校验失败、元数据引擎降级 ✅
  ERROR: 对象存储请求最终失败、FUSE 操作错误 ✅
  DEBUG: 每次 read 的缓存命中/未命中 ✅（write 无缓存命中概念，写路径失败走 ERROR）
验证:
  日志格式为结构化 JSON (TestNewLogger_JSONFormat) ✅
  级别过滤正确 (TestNewLogger_LevelFiltering) ✅
  非法级别报错 (TestNewLogger_InvalidLevel) ✅
```

### Week 10: systemd + K8s DaemonSet + 故障恢复测试

#### 10.1 systemd 服务 ✅（YAML 交付 + 静态契约测试；真机部署验证待做）

```
任务: systemd unit 文件 + 安装脚本
文件:
  deploy/systemd/fuel.service      ← systemd unit（Restart=always, SIGINT 优雅退出, Journald 日志）✅
验证:
  systemctl start fuel → 挂载成功                              🔲（需真机）
  systemctl restart fuel → 缓存索引重建 (MVP: 扫描重建)          ✅（TestScanRebuild_OnRestart /
                                                                  TestFailure_Restart_IndexRebuild）
  systemctl status fuel → 运行状态正常                          🔲（需真机）
  unit 文件关键字段（ExecStart/Restart/KillSignal）              ✅（TestDeploy_SystemdServiceContract）

扫描重建语义（INV-9 约束下的 MVP 实现）:
  重启后 NewNVMeCache 扫描 {dir}/{bucket}，存量文件以 ETag="" 登记索引：
  - 恢复空间记账与 LRU 淘汰能力（修复重启后磁盘文件永久泄漏）
  - ETag 未知 → Get 必然 miss 回源（INV-9：无法证明正确不返回），重拉后恢复命中
  - 真正热恢复（含 ETag）见 10.4 BoltDB 索引持久化（可选）
```

#### 10.2 K8s DaemonSet 部署 ⏳（YAML 清单已交付，集群验证待做）

```
任务: K8s DaemonSet YAML + ConfigMap + Secret
文件:
  deploy/Dockerfile                     ← 镜像构建（scratch + 静态二进制，无 CGO）✅
  deploy/k8s/daemonset.yaml             ← Fuel FUSE DaemonSet ✅
    (privileged + /dev/fuse hostPath + /fuel Bidirectional 传播 + NVMe hostPath
     + metrics 端口 49999 + prometheus.io/* Pod 注解 + /livez 探针)
  deploy/k8s/configmap.yaml             ← 配置 ✅（内嵌 fuel-config.yaml 经
    TestDeployConfigMap_EmbeddedConfigLoads 静态契约测试）
  deploy/k8s/secret.yaml                ← 凭证模板（kubectl create secret 注入）✅
  deploy/k8s/monitoring.yaml            ← metrics headless Service + ServiceMonitor ✅
  deploy/k8s/prometheus-standalone.yaml ← 可选 standalone（无集群级 Prometheus 时）✅

Prometheus 部署拓扑（2026-08 决策）:
  Prometheus 是独立组件，不随 Fuel 部署（pull 模型抓取 :49999/metrics）。
  生产上复用集群级 Prometheus（prometheus-operator 用 ServiceMonitor；
  vanilla 用 monitoring.yaml 注释中的 kubernetes_sd pod 注解发现）；
  集群没有 Prometheus 时用 prometheus-standalone.yaml（仅开发/测试）。
  Fuel 侧只需保证 /metrics 暴露 + Pod 注解/Service 标签约定。

探针设计修正（原 "livenessProbe /health 通过"）:
  liveness/readiness 改用 /livez（进程存活即 200）。/health 在依赖
  （Redis/OSS）不可用时返回 503，用于 liveness 会触发 kubelet 重启容器
  → FUSE 挂载中断放大故障（违反 INV-4 降级语义）。/health 保留给告警。
  ARCH_SPEC §9.2 已同步更新。

验证:
  kubectl apply -f deploy/k8s/ → DaemonSet 正常启动   🔲（需真实集群 + 构建镜像）
  FUSE 挂载点可用                                       🔲
  Prometheus 抓取指标正常                                🔲
  livenessProbe /livez 通过                              🔲
  YAML 语法 + ConfigMap 内嵌配置可解析                     ✅（Go 测试 + yaml 解析校验）
```

#### 10.3 故障恢复测试 ✅（单元级；集群级故障注入待做）

```
任务: 端到端故障恢复测试
文件:
  internal/fuse/failure_test.go    ← 故障恢复测试 ✅
测试场景 (ARCH_SPEC.md §GOAL-7):
  场景 1: FUSE 进程崩溃 → 重启 → 缓存索引扫描重建   ✅ TestFailure_Restart_IndexRebuild
  场景 2: Redis 宕机 → L1 缓存可用 → 降级直查       ✅ TestFailure_RedisDown_L1WarmRead
  场景 3: MySQL 宕机 → 同上                        ✅ TestFailure_MySQLDown_DegradeToDirect
          （顺带发现并修复：生产代码从未注册 mysql 驱动，sql.Open("mysql") 必失败——
           此前引擎测试全走 sqlmock 未暴露；已加 `_ "github.com/go-sql-driver/mysql"`）
  场景 4: 对象存储网络不可达 → 已缓存正常读 / 未缓存 EIO  ✅ TestFailure_StoreDown_*
  场景 5: 磁盘空间不足/缓存拒绝写入 → 降级 readThrough 直读 ✅ TestFailure_CacheRejected_ReadThrough
          （顺带修复 D7：>maxFileSize 文件此前直接 EIO，现在降级直读可用）
          读路径新增 readThrough：缓存拉取失败时 Range GET 直读对象存储，不写缓存；
          degraded 粘性标记避免重复整文件拉取。
  场景 6: 缓存索引持久化恢复 (BoltDB, 10.4 可选)   🔲 未实现，跳过
验证标准:
  所有场景下功能正常 (仅性能降级) ✅（单测全绿，-race 通过）
  无 panic, 无数据丢失 ✅
  监控告警触发正常 🔲（需 Prometheus 告警规则，接 /health 与指标阈值）
```

#### 10.4 缓存索引持久化 (可选)

```
任务: BoltDB 缓存索引持久化
文件:
  internal/cache/index.go          ← BoltDB 持久化
逻辑:
  定期持久化 LRU 索引到 BoltDB
  重启时加载索引, 跳过全量扫描
验证:
  重启后缓存索引秒级恢复
```

### Phase 4 交付物清单

```
文件:
  internal/monitor/metrics.go / http.go / log.go / *_test.go  ✅
    (health/livez/metrics 端点在 http.go; 9.1 指标 + 9.2 日志体系完成)
  deploy/Dockerfile ✅
  deploy/k8s/daemonset.yaml / configmap.yaml / secret.yaml ✅
  deploy/k8s/monitoring.yaml / prometheus-standalone.yaml ✅
  deploy/systemd/fuel.service ✅
  internal/fuse/failure_test.go ✅（10.3 场景 1-5 单元级全绿）
  internal/config/deploy_config_test.go ✅（K8s/systemd 清单与代码的静态契约测试）
  internal/cache/index.go (BoltDB 持久化, 可选) 🔲 未实现（10.4 可选项）

验证:
  Prometheus 指标齐全
  systemd 部署正常
  K8s DaemonSet 部署正常
  故障恢复测试全部通过
  7 天无故障运行 (预生产)

预估新增代码量: ~1500 行
```

---

## 6. Phase 5: 第二期部署 — CSI Driver + Sidecar + PVC（3 周）

> 目标：标准 K8s CSI 语义，应用 Pod 通过 PVC 透明挂载；Sidecar 提供多租户 Pod 级隔离。应用 Pod 均不运行 FUSE、不需要 privileged（接入方式见 ARCH_SPEC §10.3）。

### Week 11: CSI Driver 核心实现

#### 11.1 CSI Driver

```
任务: 实现 CSI IdentityServer + ControllerServer + NodeServer
文件:
  internal/deploy/csi.go            ← CSI Driver 核心
  internal/deploy/csi_server.go     ← CSI gRPC server
  internal/deploy/csi_test.go       ← 单元测试
实现 (ARCH_SPEC.md §10.4.3):
  IdentityServer:
    GetPluginInfo → 返回 driverName=csi.fuel.io
    GetPluginCapabilities
    Probe → 检查 FUSE 挂载点
  ControllerServer:
    CreateVolume → 创建虚拟 PV 对象
    DeleteVolume → 删除 PV 对象
  NodeServer:
    NodePublishVolume → bind-mount /fuel/{bucket} → Pod targetPath
    NodeUnpublishVolume → unmount
    NodeGetCapabilities
验证:
  go test ./internal/deploy/... 通过
  CSI 接口契约正确
```

### Week 12: CSI 部署清单 + 端到端验证

#### 12.1 K8s 部署清单

```
任务: CSI Driver 全部 K8s YAML
文件:
  deploy/k8s/csi-driver.yaml        ← CSIDriver CRD + RBAC
  deploy/k8s/csi-controller.yaml    ← CSI Controller Deployment
  deploy/k8s/csi-nodeplugin.yaml    ← CSI NodePlugin DaemonSet
  deploy/k8s/storageclass.yaml      ← StorageClass
验证:
  kubectl apply -f deploy/k8s/csi-*.yaml → 全部正常启动
  kubectl get csidrivers → csi.fuel.io 注册成功
  kubectl get pods -n fuel-system → 全部 Running
```

#### 12.2 PVC 挂载端到端验证

```
任务: 应用 Pod 通过 PVC 挂载
文件:
  deploy/k8s/example-pvc.yaml       ← 示例 PVC
  deploy/k8s/example-job.yaml       ← 示例应用 Pod
验证标准:
  创建 PVC (storageClassName: fuel-csi)
  创建应用 Pod (引用 PVC)
  Pod 内 ls /data/ 能列目录
  Pod 内 cat /data/file 能读文件
  Pod 内读命中缓存 (通过 FUSE 日志确认)
  CSI 挂载和卸载正常 (Pod 创建/删除)
```

### Week 13: Sidecar 模式 + 数据感知调度

#### 13.1 Sidecar 模式

```
任务: Sidecar 部署模式验证
文件:
  deploy/k8s/example-sidecar.yaml   ← Sidecar 示例 Pod
验证:
  Sidecar 容器中的 FUSE 挂载正常
  应用容器通过 emptyDir 访问 FUSE 挂载点
  Pod 级隔离 (FUSE 崩溃只影响本 Pod)
```

#### 13.2 数据感知调度

```
任务: 节点标签 + Pod 亲和性调度
验证:
  kubectl label nodes → fuel.io/fuse=enabled
  Pod 亲和性配置 → 调度到有 FUSE 的节点
  无 FUSE 的节点不被调度
```

### Phase 5 交付物清单

```
文件:
  internal/deploy/csi.go / csi_server.go / csi_test.go
  deploy/k8s/csi-driver.yaml / csi-controller.yaml / csi-nodeplugin.yaml
  deploy/k8s/storageclass.yaml / example-pvc.yaml / example-job.yaml / example-sidecar.yaml

验证:
  CSI Driver 注册成功
  PVC 挂载正常工作
  应用 Pod 透明访问对象存储数据
  Sidecar 模式正常
  数据感知调度正常

预估新增代码量: ~1200 行
```

---

## 7. Phase 6: 多后端扩展（按需）

> 目标：按需实现 S3 / MinIO 后端。架构已支持（INV-8），仅需实现 `ObjectStore` 接口。

### 按需触发条件

- 有明确的 S3 或 MinIO 使用需求
- OSS 后端已稳定运行
- Phase 1-5 全部完成

### 实施步骤（以 S3 为例）

```
1. 创建 internal/objectstore/s3.go
2. 引入 AWS SDK v2 (github.com/aws/aws-sdk-go-v2)
3. 实现 ObjectStore 接口全部方法
4. init() 中注册: api.RegisterObjectStore("s3", NewS3Store)
5. 在 StorageConfig 中添加 S3 配置段
6. 编写单元测试 (mock + LocalStack)
7. 验证: storage.type=s3 时挂载和读写正常

预估工作量: 1-2 周 (单后端)
预估代码量: ~600 行
```

---

## 8. 风险与依赖

### 8.1 技术风险

| 风险 | 影响 | 缓解 | Phase |
|------|------|------|-------|
| FUSE 库学习曲线 | Phase 1 工期延误 | hanwen/go-fuse 有 JuiceFS 生产验证，参考 JuiceFS 源码 | Phase 1 |
| OSS SDK 行为差异 | 集成测试失败 | 集成测试提前跑 (Phase 1 Week 1)，暴露问题 | Phase 1 |
| 缓存命中率不达预期 | Benchmark 不达标 | 分析热数据集大小 vs NVMe 容量，调整淘汰策略 | Phase 2 |
| Redis/MySQL 连接不稳定 | 元数据引擎故障 | 降级为直查对象存储 (INV-4 保证)，重试 + 告警 | Phase 3-4 |
| CSI bind-mount 权限问题 | PVC 挂载失败 | privileged + mountPropagation: Bidirectional | Phase 5 |
| 海量小文件 List 慢 | readdir 延迟高 | 短 TTL 目录缓存 + 元数据引擎缓存 | Phase 2-3 |

### 8.2 外部依赖

| 依赖 | 阶段 | 准备事项 |
|------|------|---------|
| 阿里云 OSS bucket + AK/SK | Phase 1 | 提前创建测试 bucket，准备 AK/SK |
| Redis 实例 | Phase 3 | 准备测试 Redis (本地或云) |
| MySQL 实例 | Phase 3 | 准备测试 MySQL (本地或云) |
| K8s 集群 | Phase 4-5 | 准备测试集群 (1.18+), 节点打标签 |
| Alluxio FUSE 基线 | Phase 2 | 现有 Alluxio 部署，用于 benchmark 对比 |

### 8.3 人员建议

| Phase | 建议人力 | 关键技能 |
|-------|---------|---------|
| Phase 1-2 | 1-2 人 | Go + FUSE + 对象存储 |
| Phase 3 | 1-2 人 | Go + Redis/MySQL |
| Phase 4-5 | 1-2 人 | Go + K8s + CSI |

---

## 9. 文件交付汇总

```
Phase 1 交付 (Week 1-3):
  cmd/fuel/main.go / mount.go / version.go
  api/interfaces.go / types.go
  internal/config/config.go / config_test.go
  internal/objectstore/store.go / oss.go / mock.go / oss_test.go
  internal/metadata/engine.go / direct.go / types.go / direct_test.go
  internal/cache/data.go / meta.go / neg.go / eviction.go / index.go / *_test.go
  internal/fuse/fuse.go / ops.go / handle.go / mount.go / ops_test.go
  ~3500 行

Phase 2 交付 (Week 4-5):
  internal/cache/prefetch.go / prefetch_test.go
  internal/cache/buffer.go / buffer_test.go
  internal/benchmark/read_test.go / meta_test.go
  benchmark 报告
  ~1500 行

Phase 3 交付 (Week 6-8):
  internal/metadata/redis.go / redis_test.go
  internal/metadata/mysql.go / mysql_test.go / schema.sql
  internal/fuse/ops.go (扩展写操作) / write_test.go
  ~2500 行

Phase 4 交付 (Week 9-10):
  internal/monitor/metrics.go / health.go / log.go
  deploy/systemd/fuel.service
  deploy/k8s/daemonset.yaml / configmap.yaml / secret.yaml
  internal/fuse/failure_test.go
  internal/cache/index.go (BoltDB, 可选)
  ~1500 行

Phase 5 交付 (Week 11-13):
  internal/deploy/csi.go / csi_server.go / csi_test.go
  deploy/k8s/csi-driver.yaml / csi-controller.yaml / csi-nodeplugin.yaml
  deploy/k8s/storageclass.yaml / example-*.yaml
  ~1200 行

总计预估代码量: ~10200 行
```

---

## 10. 与 ARCH_SPEC.md 的对齐

| ARCH_SPEC 章节 | 对应 Phase | 说明 |
|----------------|-----------|------|
| §4.2 模块分解 | Phase 1 | 项目骨架按此结构创建 |
| §4.3 核心接口 | Phase 1 | ObjectStore / MetadataEngine / DataCache 接口实现 |
| §4.4 数据流 | Phase 1 (读) / Phase 3 (写) | 读路径 Phase 1, 写路径 Phase 3 |
| §5 技术栈 | Phase 1 | go.mod 按此引入依赖 |
| §6 路径映射 | Phase 1 | 路径格式按此实现 |
| §7 一致性模型 | Phase 3 | 写后读一致性验证 |
| §8 配置规范 | Phase 1 | 配置结构体按此定义 |
| §9 监控规范 | Phase 4 | Prometheus 指标按此实现 |
| §10.1 本地部署 | Phase 4 | systemd 部署 |
| §10.2-10.3 K8s 模式与接入方式 | Phase 4-5 | DaemonSet (第一期) / CSI + Sidecar (第二期) |
| §11.2 Phase 1 验证标准 | Phase 1 | 最小验证标准 |
| §11.3 不做过度设计 | 全部 Phase | 每个 Phase 遵守 |
| GOAL-1 POSIX | Phase 1-3 | 逐步实现操作 |
| GOAL-2 缓存命中性能 | Phase 2 | Benchmark 验证 |
| GOAL-3 回源性能 | Phase 2 | Benchmark 验证 |
| GOAL-4 缓存命中率 | Phase 2 | Benchmark 验证 |
| GOAL-5 双模部署 | Phase 4-5 | systemd + K8s |
| GOAL-6 可观测性 | Phase 4 | 监控指标 |
| GOAL-7 故障降级 | Phase 4 | 故障恢复测试 |
| GOAL-8 可维护性 | 全部 Phase | 接口隔离 + 测试 |

---

## 11. 已知不足与修复跟踪

> 来源：2026-08 元数据缓存一致性审查（getAttr / listDirEntries / Open / prefetchBatch 链路）。
> 状态标记：✅ 已修复 / 🔲 未决。未决项按优先级排序，修复时在此更新状态。

### ✅ D1. 空 ETag 入库导致身份校验失效（INV-9 灰色路径）— 已修复

- **根因链**: direct 引擎 `ListDir` 内联的 Meta 无 ETag → `fillMissingMeta`（BatchGetAttr）失败时静默跳过 → `listDirEntries` 仍将空 ETag 的 Meta 预填进 L1 stat 缓存 → `Open` 拿空 ETag 调 `dataCache.Get(key, "")` → `fetchAndCache` 以空 ETag `Put` 入库 → 后续 `Get(key, "")` 恒命中，**身份校验永久失效**（直到 stat TTL 过期自愈）。
- **修复**:
  - `listDirEntries`: 空 ETag 的**文件** entry 不预填 stat 缓存（目录无 ETag 需求），下次访问回源 HEAD（`node.go`）。
  - `prefetchBatch`: 先 `getAttr` 取真实 ETag（通常命中预填的 L1），空 ETag 直接跳过；判存改用 `Contains`（`node.go`）。
  - `DataCache.Get`: 空 ETag 一律 miss 且**不误删**已有条目（INV-9 不确定状态按 miss 处理）。
  - `DataCache.Put` / `PutConcurrent`: 拒绝空 ETag 入库（防御纵深，空身份条目不可创建）。
- **测试**: `TestNVMeCache_GetEmptyETag` / `TestNVMeCache_PutEmptyETag` / `TestPutConcurrent_EmptyETag` / `TestFuelRoot_ListDirEntries_NoStatPrefillForEmptyETag` / `TestFuelRoot_BatchPrefetch_SkipEmptyETag`。

### ✅ D2. prefetchBatch 判存误删有效缓存条目 — 已修复（随 D1）

- **现象**: `prefetchBatch` 用 `dataCache.Get(key, "")` 做存在性检查；条目带真实 ETag 时走"ETag 不匹配"分支，**删除有效缓存文件**后重新拉取，批量预取反而冲刷缓存。
- **修复**: 判存改为 `Contains(key, me.ETag)`（不淘汰、不改 LRU），且 `Get` 空 ETag 不再触发淘汰分支（见 D1）。

### ✅ D3. Prefetcher.OnRead 数据竞争 — 已修复（修复 D1 过程中 `-race` 发现，基线已存在）

- **现象**: `OnRead` 在 `mu.Lock()` 之前读 `p.enabled`（prefetch.go:72），与锁内 `p.enabled = false`（乱序读禁用预读分支）竞争。
- **修复**: `enabled` 检查移入锁内。`go test -race ./...` 全绿。

### 🔲 D4. TTL 窗口内返回陈旧元数据/数据（内核 TTL 与 L1 TTL 叠加）

- **现象**: 外部修改/删除 OSS 对象后，最长约 60s（内核 attr 30s + L1 stat 30s 叠加）内 `getAttr` 返回旧 Meta；窗口内 `Open` 用旧 ETag 命中旧数据缓存，ETag 校验链不启动（§7.3 身份校验只在 L1/L2 miss 回源 HEAD 后执行）。外部新建对象最长约 90s（内核 negative 30s + L1 neg 60s）返回 ENOENT。
- **定位**: 这是 §7.1 明确的最终一致设计（业务前提：一次写多次读、无外部修改）。若"外部修改 OSS"成为真实场景则不可接受。
- **处置建议**: 确认业务无外部修改则保持现状；否则评估缩短 TTL、open 强制 HEAD（牺牲性能）、或 L2 失效主动推送。

### 🔲 D5. neg 缓存与 dir 缓存不一致：`ls` 可见但 `open` ENOENT

- **现象**: stat 不存在路径 → neg 缓存 60s；期间外部创建该文件 → dir 缓存 10s 过期后 `ls` 可见，但 `getAttr` 命中 neg 仍返回 ENOENT（最长再持续 ~50s）。三层缓存 TTL 独立、`listDirEntries` 不查 neg。
- **处置建议**: 接受（训练数据集极少边读边建），或令 negTTL ≤ dirTTL，或 `listDirEntries` 回源后对出现的子项主动 `DeleteNeg`。

### ✅ D6. 写路径主动失效未实现 — 已修复（Week 8）

- 写路径（Flush/Unlink/Rename/Mkdir/Rmdir）统一经 `invalidateAfterWrite` 按 §7.2 顺序失效 L1 + L2 + 数据缓存（`ops.go`）。写后读一致性 8 场景测试落地（`write_test.go`）。
- 残留限制：目录 Rename 不支持（需递归拷贝前缀下全部对象，超出 MVP）→ 见 D9。

### ✅ D7. 大文件（> maxFileSize）读取直接 EIO — 已修复（Week 10.3）

- 读路径新增 `readThrough` 降级（`handle.go`）：`fetchAndCache` 失败（ENOSPC / 超 maxFileSize / 缓存写拒绝）时 Range GET 直读对象存储，不写缓存；`degraded` 粘性标记避免本句柄重复整文件拉取。
- 数据直读来自真相来源（OSS），不违反 INV-9。
- 测试：`TestFailure_CacheRejected_ReadThrough`（多区间读 + 不缓存断言）。

### 🔲 D8. `fillEntryOut`/`fillAttrOut` 中 `SetTimeout(0)` 为误导性空操作

- **现象**: 代码看似要关闭内核缓存，实际 go-fuse bridge 在超时为 0 时套用 `fs.Options` 默认值（EntryTimeout=dirTTL / AttrTimeout=statTTL / NegativeTimeout=attrTTL），行为正确但代码误导。
- **处置建议**: 删除无效调用或改注释说明"继承 Options 默认 TTL"。

### 🔲 D9. 目录 Rename 不支持（Week 8 引入的已知限制）

- **现象**: `Rename` 对目录返回 ENOTSUP——对象存储无目录实体，目录 rename 需递归 Copy+Delete 前缀下全部对象，代价高且非原子，超出 MVP。
- **处置建议**: 若业务需要，按前缀批量 Copy + Delete 实现（接受非原子），或保持 ENOTSUP。

### 🔲 D10. Redis 不可用时降级直查有显著延迟（Week 8 测试发现）

- **现象**: Redis 连接失败时，每次元数据操作需等待 go-redis 重试耗尽（连接池 5 次 dial × MaxRetries=3，约 1s/次）才 fallback 直查对象存储。降级功能正确（INV-4）但延迟不可接受。
- **处置建议**: Phase 4 监控落地后，用 `HealthCheck` 周期探测 + 熔断器模式：Redis 不健康时引擎直接走 direct 路径，跳过无效重试；恢复后自动切回。

### 🔲 D11. L2 元数据引擎无命中计数器

- **现象**: `fuel_meta_hit_total{layer="l2"}` 指标缺失——Redis/MySQL 引擎内部缓存未插桩，无法统计 L2 命中；当前只暴露 `layer="l1"`。
- **处置建议**: 引擎 GetAttr/ListDir 命中分支计数（引擎内部 atomic 计数器 + scrape 读取），或接受 L1 命中作为元数据热路径代表。

### ✅ D12. 探针不能用 /health（依赖不可用会放大故障）— 已修复（Week 9/10）

- **现象**: 原设计（PLAN 10.2）livenessProbe 指向 /health；/health 在 Redis/OSS 不可用时返回 503 → kubelet 重启容器 → FUSE 挂载中断，依赖故障被放大为数据面故障（违反 INV-4 降级语义）。
- **修复**: monitor.Server 增加 `/livez`（进程存活即 200，不检查依赖）；DaemonSet liveness/readiness 均用 `/livez`；`/health` 保留依赖语义供监控告警。ARCH_SPEC §9.2 已同步。

### 🔲 D13. DaemonSet 镜像构建流水线未建立

- **现象**: `deploy/k8s/daemonset.yaml` 引用 `registry.example.com/fuel:latest` 占位镜像；`deploy/Dockerfile` 已提供（scratch + 静态二进制），但 CI 构建/推送流程未建立。
- **处置建议**: Phase 4 落地时在 CI 中加 `CGO_ENABLED=0 go build` + `docker build -f deploy/Dockerfile` 步骤。

### ✅ D14. MySQL 驱动从未注册，生产模式 MySQL 引擎必失败 — 已修复（Week 10.3）

- 引擎测试（Week 7）全部走 sqlmock（注册的是 `sqlmock` 驱动），生产路径 `sql.Open("mysql", dsn)` 实际报 `unknown driver "mysql"` 从没被发现；故障恢复测试场景 3 暴露。
- 修复：`internal/metadata/mysql.go` 补 `_ "github.com/go-sql-driver/mysql"`。
