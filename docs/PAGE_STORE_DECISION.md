# Fuel 缓存粒度设计决策 — 为什么不采用 PageStore

> 版本: v0.1
> 日期: 2026-08-17
> 关联文档: [ARCH_SPEC.md](./ARCH_SPEC.md) | [IMPL_DESIGN.md](./IMPL_DESIGN.md) | [AGENTS.md](../AGENTS.md)

---

## 1. 问题提出

> "如果采用 PageStore 的抽象，以 page 为单位 free 或者分配，对 NVMe 的性能会有优化吗？"

这是缓存系统设计的经典问题。PageStore（以 page 为单位分配/释放缓存空间）是 Alluxio、Ceph BlueStore、RocksDB BlockBasedTable 等系统的核心抽象。本文档系统分析 PageStore 在 Fuel 场景下的性能收益与正确性代价，并给出明确的架构决策。

---

## 2. 核心结论

**对 Fuel 的负载特征和现有架构，PageStore 抽象不会带来性能优化，反而会破坏核心不变量 INV-2 与 INV-9。强烈不建议引入。**

PageStore 的正确性校验是架构性放弃，而非实现缺陷。Alluxio 通过"事后巡检 + 业务可重试"补偿，JuiceFS 通过"Chunk/Slice/Block 三层 + 元数据强一致"规避，两者都不满足 Fuel 的 INV-9 严格性要求。

---

## 3. PageStore 与 INV-9 的根本冲突

### 3.1 INV-9 的要求

> 任何读路径（stat / readdir / read）返回的数据只有两种合法状态：
> 1. **绝对正确** — 数据与对象存储（真相来源）一致，且经得起来源校验（ETag 比对、内容 MD5 等）
> 2. **回退到真相来源** — 缓存层无法证明自身正确时，必须穿透到对象存储重新获取

INV-9 的关键：**缓存命中后必须能用某种机制证明数据正确；证明不了就当 miss。**

### 3.2 PageStore 的校验困境

PageStore 把对象拆成 N 个 page（默认 1MB），每个 page 独立分配/释放。要满足 INV-9，必须能校验每个 page 的正确性。但对象存储的 ETag 是**整对象粒度**：

| ETag 类型 | 生成方式 | page 级可用性 |
|-----------|---------|--------------|
| 单次 PUT | 整文件内容 MD5 | ❌ 无法拆分到 page |
| Multipart | `N-MD5(part-MD5s)` | ❌ 无法拆分到 page，part 边界与 page 边界不一致 |

**后果**：
- 无法为单个 page 计算"正确性证明" → page 命中后无法校验 → 违反 INV-9
- 要校验必须读完整对象重算 MD5 → 校验成本远高于 page 命中收益 → 本末倒置

### 3.3 与 INV-2 的冲突

```
INV-2: 缓存路径: {cache_dir}/{bucket}/{key} ←→ 对象存储: {scheme}://{bucket}/{key}
       缓存文件可被外部工具直接读取（cat / md5sum / cp）
```

PageStore 把对象拆成 N 个 page 散布在自定义布局中：
- 缓存文件不再是可直接 `cat` 的字节镜像
- 需要 Fuel 自己的索引才能拼回完整对象
- `rm -rf {cache_dir}` 无法直接重建，需要额外索引清理

直接违反 INV-2，也违反 ARCH_SPEC §7.1"整文件缓存 vs 分块缓存"的明确决策（分块缓存被否决）。

---

## 4. 性能层面分析

### 4.1 PageStore 的优化目标与 Fuel 的不匹配

PageStore 优化的是：
- **频繁随机覆盖写**（数据库、虚拟机块设备）
- **小粒度随机 I/O 聚合**（把多个小写合并成大 page）

但 Fuel 的负载是 **INV-3** 规定的"一次写、多次读、整文件字节镜像"：

| 维度 | PageStore 目标负载 | Fuel 实际负载 | 匹配度 |
|------|------------------|--------------|--------|
| 写模式 | 随机覆盖写、小粒度更新 | 整文件一次写、无覆盖 | ❌ 完全不匹配 |
| 读模式 | 小粒度随机读 | 大文件顺序读 + 小文件整读 | ⚠️ 部分匹配 |
| 空间回收 | page 级 free + 碎片整理 | 整文件 LRU 淘汰 | ❌ PageStore 更复杂 |

### 4.2 写路径：Fuel 已是顺序写最优

```
Fuel 写路径: 临时文件 → io.Copy(流式) → fsync → atomic rename
```

- 已经是**纯顺序写**，NVMe 顺序写带宽已接近硬件极限
- Page 分配不会更快，反而多一层用户态 page 索引查找
- `atomic rename` 保证原子性，PageStore 需要额外的 WAL/事务机制

### 4.3 读路径：内核 page cache 已免费聚合

```
Fuel 读路径: pread(cacheFile, offset, size)
```

- 内核 page cache 自动做 page 级聚合与预读
- `pread` 命中 page cache 时零拷贝返回
- PageStore 多一次用户态索引查找 + 多 page 拼接，延迟反而更高

### 4.4 空间回收：整文件 LRU 优于 page 级 free

| 方案 | 回收操作 | 复杂度 | 碎片问题 |
|------|---------|--------|---------|
| Fuel 整文件 | `eviction.go`: 删索引 + `os.Remove(path)` | O(1) | 无（文件系统负责） |
| PageStore | page 级 free + 后台 compaction | O(N) + 碎片整理 | 有，需额外 I/O 整理 |

### 4.5 真正的瓶颈与正确优化方向

Fuel 读路径的 NVMe 性能瓶颈（如有）在于：
- 大文件 pread 带宽 → 受 NVMe 顺序读带宽限制，**与缓存格式无关**
- 小文件 open/stat 延迟 → 受 inode/dentry 查找限制，**整文件方案反而最快**（一次 open）

正确优化方向（不破坏 INV-2/INV-9）：
- 预读（`prefetch.go`，Phase 2 已规划）
- 大文件 `mmap` 替代 `pread`
- `O_DIRECT` + 应用层 page pool（仅对超大文件且内存紧张时有用）

---

## 5. 与 Alluxio PageStore 的对比

### 5.1 Alluxio 的架构现实

Alluxio 的 `LocalPageStore` / `RocksPageStore` 确实是按 page 组织缓存：

```
meta_dir/  pages_dir/
  {fileId}/PAGE_{idx}      ← 单页文件，大小默认 1MB
  索引存储在 rocksdb 或内存 meta store
```

### 5.2 Alluxio 的完整性"盲区"

| 盲区 | 具体表现 | Fuel 的对比 |
|------|---------|------------|
| 单页损坏/部分写入 | 进程 kill -9 时 buffer 丢失，元数据 length > 实际 page 数；磁盘 bit 翻转读路径不校验 | Fuel 整文件 + ETag 校验 + 临时文件 rename，每个环节可证明正确 |
| 跨 page 一致性 | 前 5 页旧 + 后 5 页新的拼接数据可能返回给应用 | Fuel open 时 ETag 校验失败 → 整文件作废 → 全部回源 |
| 校验机制分散 | 客户端 sync interval + Worker 启动时 verify + 后台巡检只查存在性不查内容 | Fuel 读路径强制 ETag 校验 + `Verify()` 巡检算内容 MD5 |

### 5.3 Alluxio 为什么仍然"能用"

Alluxio 选择了**用一致性窗口换性能，接受偶发不正确**：

1. **写路径同步落 UFS**：应用 `write` 的 page 在 close 时强制同步 PUT UFS，回源读总能拿到正确数据
2. **mtime 启发式失效**：`FileSystemMaster` 周期性 sync UFS mtime，外部修改最终能被发现
3. **应用层兜底**：训练框架读取失败/数据错乱时，重新跑 epoch 即可，不致命

这套机制在"数据不可变 + 训练任务可重试"的自动驾驶 ML 场景下，**恰好是业务上可接受的**。

### 5.4 与 INV-9 的哲学差异

| | Alluxio | Fuel |
|--|---------|------|
| 正确性哲学 | 尽力而为（best-effort） | 要么对，要么回源（INV-9） |
| 校验时机 | 事后巡检 + 启发式 | 命中即校验 |
| 不正确后果 | 应用重试 epoch | 违反架构不变量，不可接受 |
| 业务前提 | 训练任务可重试 | 任何场景都必须正确 |

---

## 6. 与 JuiceFS 的对比

### 6.1 JuiceFS 的 Chunk/Slice/Block 三层架构

JuiceFS 是 PageStore 的极端版本，做了**对象存储上的完整文件系统**：

```
对象存储: bucket/{chunkHash}/{sliceId}_{index}
         ↑
Block (4MB) ← 实际存储单元，带独立 CRC32
         ↑
Slice (64MB) ← 写缓冲，聚合多个 Block
         ↑
Chunk (64MB) ← 文件逻辑分块
```

关键差异：**JuiceFS 在 OSS 上创建了自定义数据格式，不是字节镜像**。

### 6.2 JuiceFS 的正确性机制

JuiceFS 用**元数据强一致 + 内容自校验**解决完整性问题：

| 机制 | 实现 | 效果 |
|------|------|------|
| 元数据强一致 | Redis/MySQL 事务更新，Chunk→Slice→Block 映射 | 元数据不会指向不存在的 Block |
| Block CRC32 | 每个 Block 写入时计算 CRC32，读取时校验 | 单 Block 损坏可发现 |
| Slice 事务 | 写 Slice 成功后才更新元数据 | 避免半写入 |
| 后台 GC | 定期检查孤儿 Block、修复元数据 | 最终一致性 |

### 6.3 JuiceFS vs Fuel 的核心差异

| 维度 | JuiceFS | Fuel |
|------|---------|------|
| 数据格式 | 自定义 Chunk/Slice/Block，非字节镜像 | 字节镜像（INV-2） |
| 元数据引擎 | **必需组件**，丢失 = 数据不可读 | **可选加速层**，丢失可降级（INV-4） |
| 对象存储角色 | 存储后端，格式由 JuiceFS 定义 | 数据真相来源，格式不变（INV-1/INV-3） |
| 外部工具可读性 | ❌ 需要 JuiceFS 客户端 | ✅ `cat` / `md5sum` / `ossutil` 直接读 |
| 随机写支持 | ✅ 支持（Slice 覆盖写） | ❌ 不支持（INV-3） |
| 校验粒度 | Block 级 CRC32 | 整文件 ETag + 内容 MD5 |

### 6.4 为什么 JuiceFS 的模式不适合 Fuel

1. **违反 INV-1**：JuiceFS 的元数据引擎是真相来源的一部分，丢失 = 数据不可读。Fuel 要求元数据引擎可丢失、可重建。
2. **违反 INV-2**：JuiceFS 的对象存储数据是自定义格式，外部工具无法直接读取。Fuel 要求字节镜像。
3. **违反 INV-3**：JuiceFS 写路径产生只有 JuiceFS 能解读的数据格式。Fuel 要求与直接 OSS SDK 上传完全一致。
4. **业务不需要**：Fuel 明确"不支持随机写/追加写"（AGENTS.md §1.4），JuiceFS 的 Slice 覆盖写能力对 Fuel 是过度设计。

### 6.5 从 JuiceFS 借鉴的正确做法

Fuel 从 JuiceFS 借鉴的是**元数据引擎接口设计**和**FUSE 层 go-fuse 用法**，而非数据格式：

- `MetadataEngine` 接口（Redis/MySQL/direct 三种模式）参考 JuiceFS 元数据引擎抽象
- go-fuse `fs.InodeEmbedder` API 用法参考 JuiceFS
- **不借鉴** Chunk/Slice/Block 数据格式

---

## 7. 决策记录

| 决策项 | 选择 | 理由 |
|--------|------|------|
| 缓存粒度 | 整文件（非 PageStore） | INV-2 字节镜像 + INV-9 可校验性 |
| 空间管理 | LRU 整文件淘汰 | O(1) 简单，无碎片 |
| 校验机制 | ETag 身份校验 + 内容 MD5 巡检 | 整对象 ETag 天然支持，无需拆分 |
| 写路径 | 临时文件 + fsync + atomic rename | 顺序写最优，原子性保证 |
| 读路径 | pread + 内核 page cache | 零拷贝，内核自动聚合 |

**明确拒绝**：
- ❌ PageStore 抽象（page 级分配/释放）
- ❌ 对象拆分为固定大小 page
- ❌ 自定义 page 索引格式
- ❌ JuiceFS 式 Chunk/Slice/Block 三层

---

## 8. 不变量合规检查

本决策满足所有相关不变量：

| 不变量 | 合规性 | 说明 |
|--------|--------|------|
| INV-1 | ✅ | 对象存储是真相来源，整文件缓存可直接从 OSS 重建 |
| INV-2 | ✅ | 字节镜像，路径一一对应，外部工具可直接读 |
| INV-3 | ✅ | PutObject 整文件上传，不改变 OSS 对象格式 |
| INV-9 | ✅ | ETag 校验 + 内容 MD5 巡检，命中即可证明正确 |
| INV-7 | ✅ | DataCache 接口不变，PageStore 不引入 |
| INV-8 | ✅ | 后端可插拔，无 page 格式后端差异 |

---

## 9. 参考资料

| 文档 | 路径 | 说明 |
|------|------|------|
| Alluxio 架构分析 | `../design/alluxio_arch.md` | Alluxio PageStore 部署与配置 |
| Alluxio FUSE 可行性 | `../design/go-alluxio-fuse-feasibility.md` | Go 实现 Alluxio FUSE 的评估 |
| ARCH_SPEC §7.1 | `./ARCH_SPEC.md#71-数据缓存策略` | 整文件缓存 vs 分块缓存决策 |
| IMPL_DESIGN §4.4 | `./IMPL_DESIGN.md#44-l1-内存缓存--元数据加速层` | L1 缓存设计 |
| AGENTS.md INV-9 | `../AGENTS.md#inv-9-读路径要么返回绝对正确的数据要么回退到真相来源` | 正确性不变量 |
