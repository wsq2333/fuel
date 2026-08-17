# Fuel 性能测试计划

> 本文档定义 Fuel 的性能测试方案，包括测试场景、验收标准、Benchmark 实现和对比基线。
> 关联文档: [ARCH_SPEC.md](./ARCH_SPEC.md) §3 性能目标 (GOAL-2/3/4)

---

## 1. 性能目标

基于 ARCH_SPEC.md §3 定义的性能目标：

| 编号 | 指标 | 目标值 | 场景 | 优先级 |
|------|------|--------|------|--------|
| GOAL-2 | **缓存命中读延迟** | P50 < 1ms, P99 < 5ms | 热数据二次读 | P0 |
| GOAL-3 | **缓存未命中读延迟** | P50 < 50ms | 首次冷启动读 | P0 |
| GOAL-4 | **顺序读吞吐** | > 2 GB/s | 大文件顺序读（缓存命中）| P0 |
| GOAL-5 | **元数据操作延迟** | stat P50 < 5ms | 元数据热读（L1 TTL 命中）| P1 |
| GOAL-6 | **目录列表延迟** | readdir 1K 文件 < 100ms | 大目录列表（MetadataEngine 模式）| P1 |
| GOAL-7 | **缓存命中率** | > 90% | 热数据集 < NVMe 容量 70% | P1 |

---

## 2. 测试场景

### 2.1 场景 1: 海量小文件顺序读

**业务背景**: 自动驾驶训练任务读取连续帧的传感器数据（JPEG/PNG），单文件 100KB-1MB。

**数据集**:
- 文件数: 10,000 个
- 单文件大小: 100 KB（模拟 640x480 JPEG）
- 总数据量: ~1 GB
- 目录结构: `sensor/camera/frame_{000000..009999}.jpg`

**测试步骤**:
1. 挂载 Fuel，缓存为空（冷启动）
2. 顺序读全部 10,000 个文件（`cat frame_*.jpg > /dev/null`）
3. 重复步骤 2（缓存命中）
4. 记录每次 open + read 的延迟（P50/P99）和总吞吐

**验收标准**:
- 缓存命中读延迟 P50 < 1ms ✅ GOAL-2
- 缓存命中读延迟 P99 < 5ms ✅ GOAL-2
- 缓存命中率 > 90%（假设 NVMe 容量 > 1.1 GB）✅ GOAL-7
- 首次冷读总耗时 < 20s（假设 OSS 吞吐 50 MB/s）

---

### 2.2 场景 2: 大文件顺序读

**业务背景**: 读取点云数据（PCD）或视频片段（MP4），单文件 1-10 GB。

**数据集**:
- 文件: 1 个 1 GB 文件
- 路径: `lidar/scene_001.pcd`

**测试步骤**:
1. 挂载 Fuel，缓存为空
2. 顺序读 1 GB 文件（`cat scene_001.pcd > /dev/null`）
3. 重复步骤 2（缓存命中）
4. 记录吞吐（MB/s）

**验收标准**:
- 缓存命中吞吐 > 2 GB/s ✅ GOAL-4
- 预读命中率 > 80%（顺序读应触发预读）
- 首次冷读吞吐 > 100 MB/s（假设 OSS + 预读优化后）

---

### 2.3 场景 3: 多并发读

**业务背景**: 多 GPU 训练任务并发读取不同数据分片。

**数据集**:
- 文件数: 1,000 个
- 单文件大小: 1 MB
- 总数据量: ~1 GB
- 并发度: 8（模拟 8 GPU）

**测试步骤**:
1. 挂载 Fuel，缓存为空
2. 8 个并发进程各自顺序读 125 个文件（分片）
3. 重复步骤 2（缓存命中）
4. 记录总吞吐和 P99 延迟

**验收标准**:
- 缓存命中并发吞吐 > 4 GB/s（8 并发 × 500 MB/s）
- 缓存命中 P99 延迟 < 10ms ✅ GOAL-2（放宽到 10ms，因为并发竞争）
- 首次冷读总耗时 < 30s

---

### 2.4 场景 4: 缓存命中二次读

**业务背景**: 训练任务 epoch 重复读相同数据集（缓存热数据）。

**数据集**:
- 文件数: 100 个
- 单文件大小: 1 MB
- 总数据量: 100 MB（< NVMe 容量）

**测试步骤**:
1. 挂载 Fuel，预热缓存（读一遍）
2. 重复读相同数据集 10 次
3. 记录每次读的平均延迟（P50）

**验收标准**:
- 缓存命中率 = 100%
- 缓存命中读延迟 P50 < 1ms ✅ GOAL-2
- 无对象存储请求（监控 OSS SDK 调用次数 = 0）

---

### 2.5 场景 5: 首次冷启动读

**业务背景**: 训练任务首次启动，缓存为空，需要从对象存储拉取数据。

**数据集**:
- 文件数: 1,000 个
- 单文件大小: 500 KB
- 总数据量: ~500 MB

**测试步骤**:
1. 挂载 Fuel，清空缓存（`rm -rf {cache_dir}/*`）
2. 顺序读全部文件
3. 记录 P50/P99 延迟和吞吐

**验收标准**:
- 缓存未命中读延迟 P50 < 50ms ✅ GOAL-3
- 缓存未命中读延迟 P99 < 200ms
- 总吞吐 > 50 MB/s（受限于 OSS 带宽）

---

### 2.6 场景 6: 元数据操作

**业务背景**: 训练框架预扫描数据集（`os.stat` 全部文件获取大小）。

**数据集**:
- 文件数: 10,000 个
- 操作: `stat` 获取文件大小（不读内容）

**测试步骤**:
1. 挂载 Fuel（MetadataEngine = direct 模式，不用 Redis）
2. 连续 `stat` 全部 10,000 个文件
3. 重复步骤 2（L1 TTL 缓存命中）
4. 记录总耗时和 P50 延迟

**验收标准**:
- 首次 stat 总耗时 < 60s（直查 OSS HEAD，10,000 次请求）
- L1 缓存命中 stat 延迟 P50 < 1ms ✅ GOAL-5（放宽到 1ms，因为内存查找）
- L1 缓存命中总耗时 < 1s（10,000 次内存查找）

---

### 2.7 场景 7: 目录列表

**业务背景**: 训练框架扫描数据目录（`ls` / `readdir`）。

**数据集**:
- 单目录文件数: 1,000 个
- 操作: `readdir` 列出全部文件名 + 类型

**测试步骤**:
1. 挂载 Fuel（MetadataEngine = direct 模式）
2. `ls -1` 目录（触发 `readdir`）
3. 重复步骤 2（L1 目录缓存命中）
4. 记录总耗时

**验收标准**:
- 首次 readdir 耗时 < 2s（直查 OSS List，假设单次 List 返回 1000 条）✅ GOAL-6（放宽到 2s）
- L1 缓存命中 readdir 耗时 < 100ms ✅ GOAL-6

---

## 3. Benchmark 实现

### 3.1 工具选型

- **Go Benchmark**: 使用 `go test -bench` 实现微基准（单个 FUSE 操作延迟）
- **fio**: 测试吞吐和 IOPS（大文件顺序读、小文件随机读）
- **自定义脚本**: 模拟训练任务读模式（顺序读全部文件，多 epoch）

### 3.2 代码结构

```
internal/benchmark/
├── read_bench_test.go          ← 读吞吐 Benchmark (场景 1/2/3)
├── meta_bench_test.go          ← 元数据操作 Benchmark (场景 6/7)
├── cache_hit_bench_test.go     ← 缓存命中延迟 Benchmark (场景 4)
└── helpers.go                  ← 测试辅助函数 (生成数据集、清空缓存)
```

**示例代码框架** (read_bench_test.go):

```go
// BenchmarkSequentialRead_SmallFiles 对应场景 1
func BenchmarkSequentialRead_SmallFiles(b *testing.B) {
    // 1. 挂载 Fuel
    // 2. 生成 10,000 个 100KB 文件
    // 3. b.ResetTimer()
    // 4. for i := 0; i < b.N; i++ { 顺序读全部文件 }
    // 5. 报告 MB/s 和 P50/P99 延迟
}

// BenchmarkSequentialRead_LargeFile 对应场景 2
func BenchmarkSequentialRead_LargeFile(b *testing.B) {
    // 1. 生成 1GB 文件
    // 2. b.ResetTimer()
    // 3. for i := 0; i < b.N; i++ { cat 1GB > /dev/null }
    // 4. 报告吞吐 (GB/s)
}
```

### 3.3 执行命令

```bash
# 运行全部 Benchmark
go test -bench=. ./internal/benchmark/... -benchmem -benchtime=10s

# 单独运行场景 1
go test -bench=SmallFiles ./internal/benchmark/read_bench_test.go

# 生成 CPU profile
go test -bench=. -cpuprofile=cpu.prof ./internal/benchmark/...
go tool pprof cpu.prof
```

---

## 4. 对比基线

### 4.1 对比对象

**Alluxio FUSE** (v2.9.x):
- 业界主流分布式缓存文件系统
- 支持 OSS/S3 后端
- FUSE 接口，与 Fuel 功能相似

### 4.2 对比场景

选择场景 1/2/4 进行对比（核心读性能）：
- 场景 1: 海量小文件顺序读
- 场景 2: 大文件顺序读
- 场景 4: 缓存命中二次读

### 4.3 对比指标

| 指标 | Fuel 目标 | Alluxio FUSE (参考) | 对比维度 |
|------|----------|---------------------|---------|
| 缓存命中读延迟 P50 | < 1ms | ~2-5ms | 延迟 |
| 缓存命中读吞吐 | > 2 GB/s | ~1-1.5 GB/s | 吞吐 |
| 缓存未命中读延迟 P50 | < 50ms | ~100-200ms | 延迟 |
| 内存占用 | < 500 MB | ~2-4 GB (JVM) | 资源 |

**注**: Alluxio 数据为估算值，需实测验证。

### 4.4 环境配置

**硬件**:
- CPU: 8 核 Intel/AMD
- 内存: 16 GB
- NVMe: 500 GB，读写 > 3 GB/s（Samsung PM9A3）
- 网络: 10 Gbps（阿里云 VPC）

**软件**:
- OS: Ubuntu 22.04 / CentOS 7
- Kernel: 5.10+
- FUSE: libfuse 3.10+
- OSS: 阿里云华东区（同 region）

---

## 5. 验收流程

### 5.1 里程碑

| 阶段 | 验收条件 | 时间节点 |
|------|---------|---------|
| Phase 2.1 | 场景 1/2/4 通过验收标准（P0 指标） | Week 5 |
| Phase 2.2 | 场景 3/5 通过验收标准 | Week 6 |
| Phase 2.3 | 场景 6/7 通过验收标准 + Alluxio 对比报告 | Week 7 |

### 5.2 自动化回归

在 CI 流程中集成性能 Benchmark：
```yaml
# .github/workflows/benchmark.yml
- name: Run Benchmarks
  run: |
    go test -bench=. ./internal/benchmark/... -benchmem > bench.txt
    # 解析 bench.txt，验证关键指标未退化（±10% 误差）
```

---

## 6. 已知限制和未来优化

### 6.1 当前限制

1. **INV-2 约束**: 当前缓存是整文件镜像，不支持部分缓存
   - 影响: 大文件（> 10 GB）首次读需下载完整文件，延迟高
   - 缓解: 预读仅优化对象存储侧 page cache，不改变 INV-2

2. **元数据 direct 模式**: 无 Redis 时每次 stat 需 HEAD 对象存储
   - 影响: 场景 6 首次 stat 延迟高（10,000 次 HEAD）
   - 缓解: L1 TTL 缓存（30s）+ 建议生产环境用 Redis

3. **单节点缓存**: 不支持跨节点缓存共享
   - 影响: 多节点训练任务重复拉取相同数据
   - 缓解: 通过编排层（Fluid）实现数据亲和性调度

### 6.2 未来优化方向

1. **Block-level cache** (Phase 3):
   - 支持 4MB block 粒度缓存（放宽 INV-2）
   - 优化大文件读延迟（只缓存热 block）

2. **Metadata batch prefetch** (Phase 2 Week 4):
   - readdir 时并行预取目录下全部文件元数据
   - 优化场景 6（减少 HEAD 请求数）

3. **P2P cache warming** (Phase 4):
   - 训练开始前批量预热缓存（通过控制面触发）
   - 优化场景 5（减少冷启动延迟）

---

## 7. 附录

### 7.1 性能测试数据记录模板

```
| 场景 | 日期 | Fuel 版本 | 指标 | 实测值 | 目标值 | 通过 |
|------|------|-----------|------|--------|--------|------|
| 场景1 | 2024-01-15 | v0.2.0 | 缓存命中 P50 | 0.8ms | < 1ms | ✅ |
| 场景1 | 2024-01-15 | v0.2.0 | 缓存命中 P99 | 4.2ms | < 5ms | ✅ |
| 场景2 | 2024-01-15 | v0.2.0 | 缓存命中吞吐 | 2.1 GB/s | > 2 GB/s | ✅ |
```

### 7.2 参考文档

- ARCH_SPEC.md §3: 性能目标定义
- PLAN.md Phase 2 Week 5: Benchmark 实现计划
- [Alluxio Performance Tuning](https://docs.alluxio.io/os/user/stable/en/operation/Performance-Tuning.html)
- [goofys Benchmark](https://github.com/kahing/goofys#benchmark)
