# Spec: `benchmark` Warmup 请求（`--num-warmup-requests`）

> 生成日期：2026-07-25。配套：实施计划 `docs/superpowers/plans/2026-07-25-benchmark-warmup-requests.md`；架构索引见根 `CLAUDE.md`、`src/llmperf/benchmark/CLAUDE.md`；代码级导读见 `docs/code-summary.md`。

## 1. 动机与目标

压测的头几条请求会被**冷启动效应**污染：服务端首次加载/换入模型权重、KV-cache 与编译缓存尚未预热、连接池与 TLS 会话首次建立。这些一次性开销落在最早的几条请求上，会把 TTFT/E2E 的高分位（p95/p99）和最大值整体抬高，让分位数报告失真——尤其在 `max-num-completed-requests` 较小时污染占比更大。

**Warmup 请求**是压测领域的标配解法：正式测量前先发若干条"热身"请求，其指标**完全不计入统计**，只用于把被测端预热到稳态。

### 目标

1. 新增 CLI 旋钮 `--num-warmup-requests N`（默认 `0`）：正式测量前先发 `N` 条请求，采集但**丢弃**其指标。
2. **测量窗口的 wall-clock 计时在 warmup 之后才起点**——warmup 的耗时绝不进入吞吐量/达成速率的分母。
3. `N=0`（默认）时，行为与落盘 summary **逐字节等同现状**（向后兼容第一原则，延续本模块历史约定）。
4. 三种测量模式（闭环 / 开环 `--request-rate` / HTTP 直连 `--api-base`）都受益。

### 非目标（本轮明确不做）

1. **不**单独报告 warmup 阶段的指标（warmup 结果整条丢弃，不进 `results`、不写任何 summary 块）。若未来要"看看热身有多慢"，另开迭代。
2. **不**为 warmup 引入独立的 `--warmup-timeout`：warmup 复用 `--timeout` 作为自己那一段的独立预算上限（见 §4）。
3. **不**让 warmup 走开环泊松到达：warmup 一律闭环发射（见 §3.2），因为热身只关心"把服务器打热"，到达节奏无意义。
4. **不**改动 `metrics.py`（`summarize`/`derive_request_metrics` 零改动，见 §5）。

## 2. 开发方式

延续本模块 TDD 铁律：Red（先写复现/守护测试）→ Green（最小实现）→ `black .` → commit。零新增第三方依赖。类型注解遵守 `>=3.8,<3.11` 硬约束（`typing.List/Dict/Optional`，禁 `list[str]`/`X|None`）。

## 3. 设计

### 3.1 阶段化：warmup 相 → 测量相（计时分离）

`run_benchmark` 变为**两相**结构。关键是测量相的 `start = time.monotonic()` 挪到 warmup 相**之后**，从而 warmup 的墙钟耗时天然被排除在 `wall_clock_s` 之外（它是吞吐/达成速率的分母）。

```
                          修改前                         修改后
                    ┌──────────────────┐          ┌──────────────────┐
                    │ start = now()    │          │ [warmup 相] N>0:  │
                    │                  │          │   建 N 条 prompt   │
                    │ [测量相]          │          │   闭环发射→丢弃     │  ← 计时不含此段
   run_benchmark →  │  闭环/开环发射     │    →     ├──────────────────┤
                    │  results 收集     │          │ start = now()     │  ← 计时起点后移
                    │                  │          │ [测量相] 不变      │
                    │ wall = now()-start│          │  闭环/开环发射     │
                    └──────────────────┘          │  results 收集     │
                                                  │ wall = now()-start│
                                                  └──────────────────┘
   变化说明：
   • 新增 warmup 相（仅 N>0 触发）；其 results 用一次性列表承接后即丢弃，绝不进主 results。
   • 测量相代码逐字不动；唯一改动是 start 起点后移到 warmup 之后 → 吞吐分母天然干净。
   • N=0 时 warmup 相整段跳过，等价现状。
```

### 3.2 warmup 一律闭环，复用 worker-pool（DRY）

测量相的闭环路径本就是"`num_concurrent_requests` 个 `worker()` 常驻消费一个 `asyncio.Queue`，到 `--timeout` 或队列空即停"。warmup 相**就是同一套机制**跑 `N` 条、结果丢弃。因此把现有闭环循环抽成可复用协程 `_closed_loop(config, queue, start, results, executor)`：

- 测量闭环：`await _closed_loop(config, queue, start, results, http_executor)`。
- warmup：`await _closed_loop(config, warmup_queue, warmup_start, throwaway, http_executor)`，`throwaway` 收完即弃。

warmup 的并发度取 `num_concurrent_requests`（闭环下即 worker 数；开环下该值语义为"最大在途上限"，用作 warmup 并发同样合理）。**开环测量模式下，warmup 仍闭环**——即先用 worker-pool 打热，再进入泊松到达的测量相。这样 `_arrival_offsets` 的 count 仍是 `max_num_completed_requests`（不含 N），泊松调度器零改动。

> 顺带（KISS）：现有闭环 `worker()` 里的 `started` 计数器是**死变量**（只 `+= 1`、从不被读取，`summarize` 用的是 `len(results)`）。抽取时一并删除，收敛噪声。

### 3.3 warmup 的超时预算：自带一段，测量相仍拿满

warmup 相以自己的 `warmup_start` 为基准、`--timeout` 为上限（防止某条 warmup 卡死拖垮整轮）。测量相在 warmup 结束后**重新 `start` 计时**，因此测量窗口拿到**完整的 `--timeout` 预算**，不被 warmup 蚕食。语义：`--timeout` 是"每一相的墙钟上限"，而非全程共享。此约定写入 `docs/benchmark.md`。

### 3.4 HTTP 直连模式下的 warmup

`http_executor` 在两相之前创建（尺寸对齐 `num_concurrent_requests`），warmup 相与测量相**共用同一线程池**。warmup 走 `_single_request` → 因 `api_base` 非空而转发 `http_client.single_request_sync`，与测量相同一后端、同一路径，确保"热身打的就是被测端点"。`finally` 中统一 `shutdown(wait=False)`。

## 4. 数据模型与指标

- `BenchmarkConfig` 新增字段 `num_warmup_requests: int = 0`（放在 `api_key_env` 之后，保持默认值不破坏既有构造）。
- `common_metrics.py`：**零新增常量**（warmup 不产出任何指标 key）。
- `metrics.py`：**零改动**。warmup 结果从不进入传给 `summarize` 的 `results`，故聚合逻辑无感知，`N=0` 时 summary 键集合与数值与现状逐字节一致。

## 5. CLI 扩展

- 新增 `p.add_argument("--num-warmup-requests", type=int, default=0)`。
- 校验：`< 0` → `parser.error("--num-warmup-requests must be >= 0")`（退出码 2），与既有数值参数拦截风格一致。
- summary 回显：**仅当 `N > 0`** 才写 `summary["num_warmup_requests"] = N`（对齐 `request_rate`/`api_base` 的条件回显，保证 `N=0` summary 字节不变）。
- 接线进 `BenchmarkConfig(num_warmup_requests=args.num_warmup_requests)`。

## 6. 测试矩阵

沿用"假 `_single_request` / 假分词器、`asyncio.run()` 驱动、不起网络"的既有手法。

| 用例 | 断言 |
|---|---|
| T1 `num_warmup_requests=0` | `_single_request` 恰好被调 `max` 次；`results` 长度 == `max`（等价现状回归） |
| T2 `warmup=3, max=5` | `_single_request` 共被调 8 次；返回 `results` 恰 5 条（warmup 丢弃） |
| T3 计时分离 | warmup 的假请求 `sleep(δ)`、测量相极快 → `wall_clock_s` 明显 < 含 warmup 的总耗时（证明 warmup 不进分母） |
| T4 顺序性 | 记录每次假请求的序号/时刻，断言前 3 次（warmup）全部早于测量相首个请求 |
| T5 开环仍闭环 warmup | `request_rate` 设置时：warmup 照发 N 条；`_arrival_offsets` 的 count == `max`（不含 N），泊松路径未被 warmup 干扰 |
| T6 HTTP 直连 warmup | `api_base` 设置时：warmup 与测量都走 `http_client.single_request_sync`，`acompletion` 全程未被触及；共用同一线程池 |
| T7 CLI 接线 | `--num-warmup-requests` 进 config；负值 → 退出码 2；summary 含 `num_warmup_requests` 当且仅当 `N>0` |
| T8 metrics 回归 | 现有 `test_benchmark_metrics.py` 全绿（证明 `metrics.py` 零改动、无回归） |

## 7. 影响面与兼容性

- **改动文件**：`runner.py`（抽 `_closed_loop` + 两相化 + 新字段）、`__main__.py`（CLI 旗标 + 校验 + 条件回显）、`docs/benchmark.md`（用户文档）、三级 `CLAUDE.md` 与 `docs/code-summary.md`（changelog）。
- **`metrics.py` / `common_metrics.py` / `prompts.py` / `http_client.py` 零改动**。
- **向后兼容**：`N=0`（默认）下，代码路径、`results`、summary 全部等价现状。
- **前置阻塞（Task 0，必须先做）**：当前 `.venv` 为 `pydantic 2.4.2 + litellm 1.73.6.post1`，`import litellm` 因缺 `pydantic.Discriminator` 而失败，benchmark 全部测试收集期 ImportError——**TDD 的 Red 根本跑不起来**。根因在 `pyproject.toml`：声明了 `pydantic<2.5` 却给 `litellm>=0.1.738` 留了无上界，解析出的 litellm 1.73.x 又强依赖 `pydantic>=2.5`，自相矛盾。持久修复：给 litellm 补一个与 `pydantic<2.5` 兼容的上界（钉到仓库既有可用组合 `litellm==1.61.13`），并同步 `.venv`。此为一切后续任务的前提。
