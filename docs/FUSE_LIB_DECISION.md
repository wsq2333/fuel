# Fuel FUSE 库选型决策 — 为什么选择 hanwen/go-fuse/v2

> 版本: v0.1
> 日期: 2026-08-17
> 关联文档: [ARCH_SPEC.md](./ARCH_SPEC.md) | [IMPL_DESIGN.md](./IMPL_DESIGN.md) | [AGENTS.md](../AGENTS.md) | [PAGE_STORE_DECISION.md](./PAGE_STORE_DECISION.md)

---

## 1. 问题提出

Fuel 需要一个 Go 语言 FUSE 库，把「对象存储 + NVMe 缓存」暴露为 POSIX 文件系统。候选方案分两大类：

1. **纯 Go 实现 FUSE 内核协议** — `hanwen/go-fuse/v2`、`jacobsa/fuse`、`bazil.org/fuse`
2. **CGO 绑定 libfuse（官方 C 参考实现）** — 直接链接 libfuse.so

本文档记录选型结论与理由，作为后续 FUSE 层开发的约束依据。

---

## 2. 核心结论

**选择 `hanwen/go-fuse/v2`，使用其 `fs.InodeEmbedder` API（非 `pathfs`）。**

核心理由：它是 Go 生态中**唯一经过 JuiceFS 这种同场景（对象存储 POSIX 缓存文件系统）生产验证、纯 Go 无 CGO、持续维护**的 FUSE 库。`fs.InodeEmbedder` API 基于 inode 的抽象与内核 VFS 语义对齐，提供 Fuel 需要的内核缓存 TTL 精细控制和 inode 自主管理能力。

与 CGO libfuse 方案相比：**go-fuse 的优势全部是 Fuel 的硬需求**（纯 Go、可单测、单二进制部署、崩溃可控），**不足全部不命中 Fuel 的关键路径**（不依赖 FUSE 3.x 新特性、性能瓶颈在对象存储而非协议层、只跑 Linux/macOS）。

---

## 3. 为什么是 hanwen/go-fuse（库层选型）

### 3.1 同场景生产验证（最核心理由）

**JuiceFS 使用 hanwen/go-fuse**。JuiceFS 是「对象存储 + POSIX 缓存文件系统」领域与 Fuel 场景几乎完全相同的标杆项目（ARCH_SPEC §5.1：「JuiceFS 生产验证，Go 生态最成熟的 FUSE 库」）。同一场景被生产级验证，意味着：

- FUSE 协议的边缘 case（内核缓存失效、并发 Lookup/Forget、readahead 语义）已被踩平
- 性能调优参数（MaxBackground、MaxReadAhead、FOPEN_KEEP_CACHE）有现成参考
- 出问题时有 JuiceFS 源码可对照（PLAN.md 风险表：「FUSE 库学习曲线 → 参考 JuiceFS 源码」）

### 3.2 纯 Go 实现 FUSE 协议，无 CGO

hanwen/go-fuse **直接实现内核 FUSE 协议**（读写 `/dev/fuse`），不绑定 libfuse C 库，满足 AGENTS.md §3.5 硬约束「不引入 CGO 依赖，保持纯 Go 交叉编译」：

| 维度 | go-fuse（纯 Go） | CGO libfuse |
|------|-----------------|-------------|
| 交叉编译 | `GOOS=linux go build` 一条命令出静态二进制 | 需要目标平台 C 工具链 + libfuse 头文件，几乎无法交叉编译 |
| 部署依赖 | 只需 `/dev/fuse` 设备节点 | 目标机必须安装匹配版本的 libfuse.so（2.x/3.x 不兼容是大坑） |
| 链接方式 | 纯静态，单文件分发 | 动态链接或打包 libfuse，容器镜像变大 |
| K8s/DaemonSet | FUSE Pod 镜像可以做到 scratch/distroless | 必须带 C 运行时 + libfuse |

Fuel 的部署形态（裸机 → DaemonSet → CSI → Sidecar，AGENTS.md §1.2）决定了「单静态二进制、无外部依赖」是硬需求。

### 3.3 候选库对比

| 库 | 状态 | 结论 |
|----|------|------|
| `hanwen/go-fuse/v2` | 持续维护，Go 生态事实标准，JuiceFS 生产验证 | ✅ 选中 |
| `jacobsa/fuse` | Google 出品，goofys 使用，维护近乎停滞，性能弱于 hanwen | ❌ AGENTS.md §3.5 明确禁止 |
| `bazil.org/fuse` | API 过于底层，需自己处理大量协议细节 | ❌ 开发成本高，无同场景参考实现 |
| CGO libfuse 绑定 | 破坏纯 Go 约束 | ❌ 违反 AGENTS.md §3.5 |

---

## 4. 为什么是 fs.InodeEmbedder API（API 层选型）

go-fuse v2 提供两套 API（IMPL_DESIGN §5.1）：

| API | 抽象层 | 性能 | 说明 |
|-----|--------|------|------|
| `fuse/pathfs` | 基于**路径** | 差 | 每次操作内核只给 inode，库内部要做 inode→path 反查，路径拼接开销大 |
| `fuse/fs`（`fs.InodeEmbedder`） | 基于 **inode** | 高 | 直接操作 inode，与内核 VFS 语义对齐，JuiceFS 使用此 API |

选择 `fs.InodeEmbedder` 的具体收益：

1. **避免路径反查开销**：内核 FUSE 请求天然是 inode 导向的（Lookup 返回 inode，后续 Open/Read 都带 inode）。pathfs 要维护 inode→path 映射并在每次操作做查找；InodeEmbedder 让 `FuelNode` 直接挂在 inode 上，`path` 只是节点的一个字段。

2. **内核缓存控制精细**：`fs` 包暴露 `EntryTimeout` / `AttrTimeout` / `NegativeTimeout`，对应 Fuel 的 L1 元数据 TTL（stat/dir/neg 三层）。`mount.go` 把 `cfg.Metadata.Cache.*TTL` 直接映射到这三个超时，pathfs 给不了这种控制粒度。

3. **inode 号自主管理**：`fs.StableAttr{Ino: api.InodeFromPath(path)}` 让 inode 号由 FNV-1a(path) 稳定生成，与 `MetaEntry.Inode` 一致（IMPL_DESIGN §5.4），pathfs 无法指定。

4. **与 JuiceFS 同构**：`FuelRoot`/`FuelNode` 的结构（root 持有 store/cache/engine 依赖，node 持有 path）直接参照 JuiceFS 的节点设计，后续接入写路径（Create/Flush）时模式已验证。

---

## 5. fs 与 fuse 两个包的分工

```
"github.com/hanwen/go-fuse/v2/fs"    → NodeFS 框架层：InodeEmbedder 接口、
                                       NewNodeFS（把 Node 树桥接成 RawFileSystem）、
                                       StableAttr、DirStream、ServerCallbacks（测试用）

"github.com/hanwen/go-fuse/v2/fuse"  → 协议层：fuse.Server（挂载点生命周期）、
                                       MountOptions、EntryOut/AttrOut/DirEntry、
                                       ReadResult、FOPEN_KEEP_CACHE 等协议常量
```

调用链：

```
fs.NewNodeFS(root, opts)                    // Node 树 → RawFileSystem
  → fuse.NewServer(rawFS, mountPoint, opts) // 挂载到 {mountPoint}/{bucket}，启动服务 goroutine
  → server.Wait()                           // 阻塞直到 Unmount
```

---

## 6. 与 CGO libfuse 方案的详细对比

### 6.1 go-fuse 的优势（对应 Fuel 的硬需求）

**（1）Go 原生并发与工具链**

- go-fuse 用 goroutine 处理内核请求队列，与 store/cache 层的 `singleflight`、`sync.RWMutex` 天然协同；libfuse 是 C 线程模型，跨语言边界调度成本高
- `fs.ServerCallbacks` 可 stub 掉内核通知，**单元测试不需要真实挂载**（`internal/fuse/node_test.go` 13 个用例全靠这个）；CGO libfuse 的单测基本要真实挂载或写 C 层 mock
- `go test -race`、`pprof`、逃逸分析全套工具链可用；CGO 边界处这些工具全部失效

**（2）崩溃域隔离**

- 纯 Go panic 可 recover（AGENTS.md §3.2-7 要求 FUSE 操作 recover 后返回 EIO），不拖垮挂载点
- CGO 中 C 段 segfault 直接拖死整个进程，挂载点变 `transport endpoint not connected`，恢复成本高

**（3）与内核语义对齐的现代 API**

- `fs.InodeEmbedder` 按内核 VFS 语义设计（inode 导向、Entry/Attr/Negative 三级 TTL、kernel notify）
- libfuse 传统 `fuse_operations` 是 path 导向（`getattr(path)`），需要库内部做 inode→path 反查

### 6.2 go-fuse 的不足（均不命中 Fuel 关键路径）

**（1）协议新特性滞后于 libfuse**

libfuse 是内核 FUSE 模块的官方同源参考实现，新特性永远先到：

| 特性 | libfuse | go-fuse |
|------|---------|---------|
| FUSE 3.x passthrough（fd 透传内核态，零用户态往返） | ✅ 3.10+ | ❌ 不支持 |
| 多队列 session（max_threads、NUMA 亲和） | ✅ 3.12+ | ⚠️ MaxBackground 语义不同 |
| io_uring 集成（Linux 5.15+） | ✅ 跟进中 | ❌ |
| 新 capability 位 | 及时 | 逐个追赶 |

对 Fuel 的影响：**较小**。只读场景（Lookup/Getattr/Open/Read/Readdir）是 FUSE 1.0 时代就定型的经典语义，不依赖新特性。passthrough 模式对缓存命中读路径（pread 本地 NVMe 文件）有理论收益——读数据可完全不经过用户态，是未来可关注的点（见 §8）。

**（2）极限性能略低**

libfuse 3.x 的请求分发、内存池、splice 优化经过 20 年打磨，单 mount 点极限 IOPS/吞吐高于 go-fuse。go-fuse 的 `ReadResultFd`（sendfile 语义）可追平大部分差距，但小 I/O 高频场景仍有 gap。

对 Fuel 的影响：**几乎无感**。读路径瓶颈是对象存储网络延迟（几十~几百 ms）和 NVMe 带宽（GB/s 级），FUSE 协议层的微秒级开销不在关键路径上（ARCH_SPEC GOAL-2 缓存命中读 P50 < 1ms，go-fuse 完全够得着）。

**（3）平台覆盖面窄**

| 平台 | libfuse | go-fuse |
|------|---------|---------|
| Linux | ✅ | ✅ 主力 |
| macOS (macFUSE) | ✅ | ✅ 支持 |
| FreeBSD/OpenBSD/NetBSD | ✅ | ⚠️ 有限/无 |
| Windows (WinFsp) | ✅ | ❌ 需用其他库 |

对 Fuel 的影响：**无**。目标环境是 Linux 训练集群 + 开发机 macOS，都在覆盖范围内。

**（4）边缘 case 与兼容性打磨**

libfuse 与内核的 init 握手、capability 协商、异常 unmount 清理（fusermount）经过所有主流内核版本考验。go-fuse 在旧内核或特殊配置（FUSE 模块未加载、`user_allow_other` 未开）下的报错不如 libfuse 友好——这恰好是 AGENTS.md §3.2-3 要求显式处理的场景（挂载失败返回明确错误、不 panic）。

**（5）生态与排障资源**

libfuse 有 xfstests/fstests 完整测试套件与深厚的社区积累。go-fuse 生态相对小，复杂问题常需读源码或对照 JuiceFS 实现（PLAN.md 把「FUSE 库学习曲线」列为风险，缓解措施即「参考 JuiceFS 源码」）。

### 6.3 对比总结表

| 维度 | go-fuse（选中） | CGO libfuse |
|------|----------------|-------------|
| 部署/交叉编译 | ✅ 静态单二进制 | ❌ 依赖目标机 libfuse |
| 并发模型 | ✅ goroutine 原生 | ⚠️ C 线程跨边界 |
| 可测性 | ✅ 免挂载单测 | ❌ 需真实挂载 |
| 崩溃隔离 | ✅ recover 可控 | ❌ C 崩溃拖死进程 |
| 协议新特性 | ⚠️ 滞后（无 passthrough/io_uring） | ✅ 官方同步 |
| 极限性能 | ⚠️ 略低 | ✅ 最高 |
| 平台覆盖 | ⚠️ Linux/macOS | ✅ 全平台 |
| 边缘兼容性 | ⚠️ 需自己打磨 | ✅ 20 年积累 |
| 排障资源 | ⚠️ 较少（靠 JuiceFS 参照） | ✅ 丰富 |

---

## 7. 决策记录

| 决策项 | 选择 | 理由 |
|--------|------|------|
| FUSE 库 | `hanwen/go-fuse/v2` | JuiceFS 同场景生产验证 + 纯 Go 无 CGO + 持续维护 |
| API 风格 | `fs.InodeEmbedder`（inode 导向） | 避免 path 反查 + 内核缓存 TTL 精细控制 + inode 自主管理 |
| 协议实现方式 | 纯 Go 读写 `/dev/fuse` | 满足交叉编译与单二进制部署约束 |
| 崩溃处理 | 操作级 recover → 返回 EIO | AGENTS.md §3.2-7，挂载点不被单请求拖垮 |

**明确拒绝**：

- ❌ `jacobsa/fuse`（AGENTS.md §3.5 禁止，维护停滞）
- ❌ `bazil.org/fuse`（API 过底层，无同场景参考）
- ❌ CGO 绑定 libfuse（违反纯 Go 约束）
- ❌ `fuse/pathfs` API（inode→path 反查性能差）

---

## 8. 后续跟踪点

| 事项 | 触发条件 | 应对 |
|------|---------|------|
| passthrough 模式 | 缓存命中读的微秒级延迟成为瓶颈（NVMe 直读场景） | 评估 go-fuse 是否跟进；或用 `FOPEN_KEEP_CACHE` + 内核 page cache 逼近同样效果 |
| io_uring FUSE | Linux 内核普及 + go-fuse 支持 | 届时评估性能收益 |
| 写路径新语义 | Phase 2 接入 Create/Write/Flush | 沿用 InodeEmbedder 模式，对照 JuiceFS 写路径实现 |

---

## 9. 不变量合规检查

| 不变量 | 合规性 | 说明 |
|--------|--------|------|
| INV-5 | ✅ | go-fuse 不依赖 K8s/编排层，`fuel mount` 裸机可运行 |
| INV-7 | ✅ | FUSE 层依赖 `ObjectStore`/`DataCache`/`MetadataEngine` 接口，go-fuse 只在 fuse 包内出现 |
| §3.5 禁止 CGO | ✅ | 纯 Go 协议实现，无 CGO |
| §3.2-3 挂载错误处理 | ✅ | 挂载失败返回明确错误，不 panic |
| §3.1 单元测试 | ✅ | `ServerCallbacks` 支持免挂载单测，`go test ./internal/fuse/...` 独立通过 |

---

## 10. 参考资料

| 文档 | 位置 | 说明 |
|------|------|------|
| IMPL_DESIGN §5 | `./IMPL_DESIGN.md#5-go-fuse-集成方案` | go-fuse API 选型与 Node 类型设计 |
| ARCH_SPEC §5.1 | `./ARCH_SPEC.md` | 技术栈约束（FUSE 库选型理由） |
| AGENTS.md §3.5 | `../AGENTS.md` | 禁止事项（jacobsa/fuse、CGO） |
| PLAN.md 风险表 | `./PLAN.md` | FUSE 库学习曲线风险与缓解 |
| go-fuse 源码 | `github.com/hanwen/go-fuse/v2` | fs/fuse 包 API 文档 |
| JuiceFS 源码 | `github.com/juicedata/juicefs` | InodeEmbedder 同场景参考实现 |
