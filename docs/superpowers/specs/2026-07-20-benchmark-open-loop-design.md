# Spec: `benchmark` 开环请求速率模式（`--request-rate`）

- 日期：2026-07-20
- 范围：`src/llmperf/benchmark/`（runner / metrics / \_\_main\_\_）+ `src/llmperf/common_metrics.py`（仅追加常量）
- 类型：增量新功能。**不传 `--request-rate` 时闭环行为与 summary/individual JSON 输出结构完全不变**
- 前置：`benchmark` 模块已完工并完成生产硬化（43 测试函数 / pytest 48 例全绿）
- **状态：已实现（2026-07-20）。** R1-R7 / M1-M2 / C1-C3 全部落地，benchmark 路线测试增至 57 个函数（pytest 70 例）。实施计划与提交映射见 `docs/superpowers/plans/2026-07-20-benchmark-open-loop.md`。

## 1. 动机与目标

现有 `runner.py` 是纯闭环（closed-loop）模型：N 个 worker 循环取任务，上一个请求返回才发下一个。闭环只能回答"固定并发下延迟多少"，回答不了"生产流量以 X QPS 到达时服务会不会排队恶化"——后者是 vLLM `benchmark_serving`、genai-perf 等主流压测工具都内置的开环（open-loop）测量，也是用户无法在 CLI 外用脚本包出来的能力。

### 目标

- 新增 `--request-rate <float>`（每秒到达请求数）。到达间隔按指数分布采样（泊松过程），请求在**计划到达时刻**发出，不等上一个返回。
- `--num-concurrent-requests` 在开环下语义变为**最大在途请求数上限**（信号量），防止服务恶化时请求无限堆积把客户端打爆。
- 新增两个观测值：每请求的**调度延迟**（实际发出时刻 − 计划到达时刻，反映客户端因在途上限排队）与 summary 的**实际达成速率**。

### 非目标（本轮明确不做）

- `--burstiness` 突发度调节（gamma 到达过程）——先钉死指数到达，留待需要时叠加。
- 速率/并发扫描（sweep）与对比报告——独立功能，另开迭代。
- 闭环路径的任何行为、输出改动。

## 2. 开发方式

全程 TDD（对齐仓库既有约定）：Red → Green → `black` → commit。异步测试用 `asyncio.run()` 在同步 `def test_...()` 内驱动（无 `pytest-asyncio`）。到达时刻表生成抽成纯函数直接单测（对齐 `extract_content` / `parse_data_line` 范例）。零新增依赖（`random` 为标准库）。类型注解遵守 `<3.11` 约束（`typing.Optional/List/...`）。

## 3. 并发模型设计

### 3.1 到达时刻表：纯函数 `_arrival_offsets`

```python
def _arrival_offsets(rate: float, count: int, horizon: float) -> List[float]:
    """泊松过程到达时刻表（相对压测起点的秒偏移）。

    首个到达在 0 时刻；间隔 ~ Exp(rate)；生成 count 个或越过 horizon 即止。
    """
```

- 用**绝对偏移**（相对 `start`）而非逐次相对 sleep，累计不漂移。
- `horizon = config.timeout`：计划到达时刻已越过全局预算的请求直接不生成（对齐闭环 worker "到点不取新任务"语义）。
- 纯函数、可 monkeypatch `random.expovariate` 直接单测，不碰 asyncio。

### 3.2 发射协程 `_dispatch_at`

每个到达对应一个 task：

1. `asyncio.sleep` 到计划到达时刻（`start + offset`）；
2. `async with sem`（`Semaphore(num_concurrent_requests)`）获取在途名额；
3. 拿到名额后重查全局预算：`elapsed >= timeout` → 直接返回不发射（与闭环 worker 到点停发一致）；
4. 计算调度延迟 `sched_delay = max(0, now − (start + offset))`，按剩余预算派生单请求 timeout（沿用 B2 语义），调用 `_single_request`；
5. 结果 dict 追加 `cm.SCHED_DELAY` 字段后 append 进 results。

`_single_request` 零改动。开环分支与闭环分支在 `run_benchmark` 内按 `config.request_rate is None` 分派，闭环代码原样保留。

### 3.3 `BenchmarkConfig` 扩展

```python
request_rate: Optional[float] = None  # None=闭环（默认）；>0 = 开环每秒到达数
```

库直调路径传非法值（`<=0`）时 `run_benchmark` 抛 `ValueError`（CLI 路径在 argparse 层已拦截，见 §5）。

### 3.4 调度延迟的语义决策

- **对所有已发射请求记录**（含最终失败的）：调度延迟是客户端排队现象，与请求成败无关；只统计成功请求会在服务恶化（错误率升高）时恰好把最能说明问题的样本剔掉。
- 被 timeout 截断、从未发射的到达**不产生任何记录**（既不算成功也不算错误），与闭环"到点不取新任务"的口径一致。

## 4. 指标扩展

### 4.1 `common_metrics.py`（仅追加，Ray 路线零感知）

```python
SCHED_DELAY = "scheduling_delay_s"
ACHIEVED_REQUEST_RATE = "achieved_requests_per_s"
```

### 4.2 `summarize` 条件扩展

**触发条件**：`results` 中任一记录含 `SCHED_DELAY` 键（即开环运行）。闭环 results 不带该键，summary 输出与现状逐字节一致。

开环时追加两个顶层键：

- `ret[SCHED_DELAY]`：与其它指标同形的统计块（`quantiles`/`mean`/`min`/`max`/`stddev`），数据取**所有携带该键的记录**（含错误请求，见 §3.4）。
- `ret[ACHIEVED_REQUEST_RATE] = _safe_div(num_started, wall_clock_s)`：实际达成的每秒发射数，供与 `--request-rate` 目标值对照（差距大 = 在途上限或客户端瓶颈在限流）。

`derive_request_metrics` 零改动（`dict(raw)` 拷贝自动透传 `SCHED_DELAY` 到 individual JSON）。

## 5. CLI 扩展

- `--request-rate`，`type=float`，`default=None`。不传 = 闭环，一切照旧。
- 校验沿用 B4 约定：`request_rate is not None and request_rate <= 0` → `parser.error`（退出码 2）。
- summary 回显：仅当传了 `--request-rate` 时写入 `summary["request_rate"]`（对齐 `num_concurrent_requests` 的回显方式；闭环 summary 键集合不变）。
- help 文本注明开环下 `--num-concurrent-requests` 的语义变化（最大在途上限）。

## 6. 测试矩阵

| 编号 | 测试 | 守护 | 文件 |
|---|---|---|---|
| R1 | `_arrival_offsets`：首到达为 0、间隔来自 `expovariate(rate)`、`horizon` 截断、`count` 截断 | 到达时刻表 | `test_benchmark_runner.py` |
| R2 | 开环端到端（expovariate→0）：全部完成、每条记录含 `SCHED_DELAY` | 发射链路 | `test_benchmark_runner.py` |
| R3 | 慢请求 + 高速率：峰值在途 == `num_concurrent_requests`（信号量封顶），被排队请求 `SCHED_DELAY > 0` | 在途上限 | `test_benchmark_runner.py` |
| R4 | `_dispatch_at` 预算耗尽（`start` 置于远过去）：`acompletion` 不被触及、results 不追加 | 到点停发 | `test_benchmark_runner.py` |
| R5 | 开环透传单请求 timeout ≤ 剩余预算 | B2 语义保持 | `test_benchmark_runner.py` |
| R6 | 闭环（rate=None）结果不含 `SCHED_DELAY` 键 | 零侵入 | `test_benchmark_runner.py` |
| R7 | 库直调 `request_rate <= 0` → `ValueError` | 库路径校验 | `test_benchmark_runner.py` |
| M1 | 开环 results → summary 含 `SCHED_DELAY` 块与 `ACHIEVED_REQUEST_RATE`；错误请求的调度延迟计入 | 条件扩展 | `test_benchmark_metrics.py` |
| M2 | 闭环 results → summary 不含上述两键 | 零侵入 | `test_benchmark_metrics.py` |
| C1 | `--request-rate 0` / `-0.5` → `SystemExit(2)` | B4 约定 | `test_benchmark_cli.py` |
| C2 | `--request-rate 2.5` → config 收到 2.5 且 summary 回显 `request_rate` | 接线 | `test_benchmark_cli.py` |
| C3 | 不传 → config 为 None 且 summary 无 `request_rate` 键 | 零侵入 | `test_benchmark_cli.py` |

## 7. 影响面与兼容性

- **闭环（默认）**：runner 代码路径、summary / individual JSON 键集合与形状**完全不变**，既有 48 例测试不得回退。
- **开环（新）**：individual 记录多 `scheduling_delay_s` 键；summary 多 `scheduling_delay_s` 统计块、`achieved_requests_per_s`、`request_rate` 三键。
- **`common_metrics.py`**：仅追加两个常量，Ray 路线不引用、零感知。
- **依赖**：零新增（`random` 标准库）。
- **语义提示（文档需写明）**：开环下错误率天然可能高于闭环——请求不等前序返回，服务过载时会真实暴露排队与超时，这正是该模式的测量目的。
