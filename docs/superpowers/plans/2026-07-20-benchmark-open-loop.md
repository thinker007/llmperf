# benchmark 开环请求速率模式 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 `src/llmperf/benchmark/` 新增开环（open-loop）请求速率模式 `--request-rate`：泊松到达 + 在途上限信号量 + 调度延迟/达成速率观测。不传该参数时闭环行为与 JSON 输出零变化。

**Architecture:** 增量修改三个源文件（`runner.py` / `metrics.py` / `__main__.py`）+ `common_metrics.py` 追加两常量，配套扩充三个测试文件。4 个原子任务 1→2→3→4，各自 Red→Green→`black`→commit；最后文档同步收尾。

**Tech Stack:** Python 3.8–3.10、`litellm`（`acompletion`）、`asyncio`（`Semaphore`/`create_task`）、`random.expovariate`、`pytest`（`monkeypatch`，无 `pytest-asyncio`）、`black`。

**参考 spec:** `docs/superpowers/specs/2026-07-20-benchmark-open-loop-design.md`（§3 并发模型、§4 指标、§5 CLI、§6 测试矩阵）。

> **✅ 状态：全部完成（2026-07-20）。** 提交映射：Task 1 `b699802`、Task 2 `842a8b2`、Task 3 `c25841a`、Task 4 见本提交。前置收口：上一迭代遗留的 M2 半成品（CLI 层 mean/stddev 校验缺失）以 `7e31075` 补齐。benchmark 路线测试 43 → 57 个函数（pytest 70 例）全绿。

## Global Constraints

- 类型注解**必须**用 `typing.List/Dict/Tuple/Optional`，**禁止** `list[str]` / `X | None`（`requires-python >=3.8,<3.11`）。
- **零新增依赖**；测试仅用 pytest 内置 `monkeypatch`/`capsys`。
- 异步测试用 `asyncio.run()` 在同步 `def test_...()` 内驱动。
- 外部依赖经 `monkeypatch.setattr(runner, "acompletion"/"token_counter"/"sample_prompt"/"sample_output_len", ...)` 注入；到达间隔经 `monkeypatch.setattr(runner.random, "expovariate", ...)` 注入，测试不真实睡眠。
- **闭环路径的输出键集合与形状不变**，既有 48 例测试不得回退。
- 每次源码改动后 `black .` 再 commit；conventional commits。
- 全量回归：`python -m pytest tests/ -v`。

---

### Task 1: runner 开环引擎（`_arrival_offsets` + `_dispatch_at` + 分支派发）

**Files:**
- Modify: `src/llmperf/common_metrics.py`（追加 `SCHED_DELAY`）
- Modify: `src/llmperf/benchmark/runner.py`（`import random`；`BenchmarkConfig.request_rate`；新增 `_arrival_offsets`/`_dispatch_at`；`run_benchmark` 按 `request_rate is None` 分支）
- Test: `tests/test_benchmark_runner.py`（R1–R7，spec §6）

**Interfaces:**
- `_arrival_offsets(rate: float, count: int, horizon: float) -> List[float]` — 纯函数时刻表（spec §3.1）
- `_dispatch_at(config, text, n_in, max_tokens, offset, start, sem, results) -> None` — 睡到时刻→抢名额→重查预算→发射并记 `SCHED_DELAY`（spec §3.2）
- `run_benchmark`：`request_rate <= 0` 抛 `ValueError`；闭环分支代码原样不动

- [x] Step 1: Red — 写 R1–R7 七组失败测试
- [x] Step 2: Green — 实现并跑通 `python -m pytest tests/test_benchmark_runner.py -v`
- [x] Step 3: 全量回归 + `black .` + commit `feat(benchmark): open-loop request-rate engine in runner (Task 1/R)`

### Task 2: metrics 条件扩展（`SCHED_DELAY` 统计块 + 达成速率）

**Files:**
- Modify: `src/llmperf/common_metrics.py`（追加 `ACHIEVED_REQUEST_RATE`）
- Modify: `src/llmperf/benchmark/metrics.py`（`summarize` 末尾条件追加，spec §4.2）
- Test: `tests/test_benchmark_metrics.py`（M1–M2）

- [x] Step 1: Red — M1（含错误请求的调度延迟计入）+ M2（闭环无新键）
- [x] Step 2: Green + 全量回归
- [x] Step 3: `black .` + commit `feat(benchmark): scheduling-delay stats and achieved rate in summarize (Task 2/M)`

### Task 3: CLI 接线（`--request-rate` 参数 + 校验 + 回显）

**Files:**
- Modify: `src/llmperf/benchmark/__main__.py`（参数、B4 式校验、config 接线、summary 条件回显）
- Test: `tests/test_benchmark_cli.py`（C1–C3）

- [x] Step 1: Red — C1（0 与负数退出码 2）+ C2（接线与回显）+ C3（默认零侵入）
- [x] Step 2: Green + 全量回归
- [x] Step 3: `black .` + commit `feat(benchmark): --request-rate CLI flag with validation (Task 3/C)`

### Task 4: 文档同步

**Files:**
- Modify: `docs/benchmark.md`（开环模式用法、语义说明、新增 summary 键）
- Modify: `src/llmperf/benchmark/CLAUDE.md`、`tests/CLAUDE.md`、根 `CLAUDE.md`（状态与测试数）
- Modify: 本 plan 勾选 + spec 回填状态行

- [x] Step 1: 更新用户文档与各级 CLAUDE.md
- [x] Step 2: commit `docs(benchmark): document open-loop request-rate mode (Task 4)`
