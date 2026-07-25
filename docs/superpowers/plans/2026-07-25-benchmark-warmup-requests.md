# benchmark Warmup 请求 Implementation Plan

> 设计规格：`docs/superpowers/specs/2026-07-25-benchmark-warmup-requests-design.md`。
> 开发方式：每个 Task 走 Red → Green → `black .` → commit；零新增依赖；类型注解守 `<3.11` 约束。

## Global Constraints

- 类型注解一律 `typing.List/Dict/Optional`，禁 3.10+ 语法。
- `N=0` 必须与现状**逐字节等价**（代码路径、`results`、summary）。
- `metrics.py` / `common_metrics.py` / `prompts.py` / `http_client.py` **不得改动**。
- 测试不起网络、不起 Ray：假 `_single_request` / 假分词器，`asyncio.run()` 驱动同步 `def test_...`。

---

### Task 0: 恢复绿色测试基线（前置阻塞，必须先做）

**问题**：`import litellm` 当前失败（`pydantic 2.4.2` 缺 `Discriminator`，litellm 1.73.6 需 ≥2.5），benchmark 测试收集期全部 ImportError，TDD 的 Red 跑不起来。根因在 `pyproject.toml`：`pydantic<2.5` 与无上界的 `litellm>=0.1.738` 冲突。

- [x] Step 1: `pyproject.toml` 给 litellm 补兼容上界——`litellm>=0.1.738` → `litellm>=0.1.738,==1.61.13`（或等价的 `>=1.60,<1.62` 区间，取仓库既有可用组合 `1.61.13`），保留 `pydantic<2.5`。
- [x] Step 2: 同步 `.venv`：`pip install 'litellm==1.61.13'`。
- [x] Step 3: 验证 `python -c "import litellm; print(litellm.__version__)"` 成功；`python -m pytest tests/ --collect-only` 无 ImportError（全部测试文件可收集）；`python -m pytest tests/ -q` 全绿（当前全仓 91 个测试函数基线）。
- [x] Step 4: commit `fix(deps): pin litellm==1.61.13 to satisfy pydantic<2.5 (Task 0)`。

> 无 Step 1，后续任何 Red 都无法运行。这是整个计划的地基。

---

### Task 1: `runner.py` — 抽 `_closed_loop` + 两相化 + 新字段

**目标**：把现有闭环 worker-pool 抽成可复用协程 `_closed_loop(config, queue, start, results, executor)`；`run_benchmark` 前置 warmup 相；`BenchmarkConfig` 加 `num_warmup_requests`。删除死变量 `started`。

- [x] Step 1: Red — 在 `tests/test_benchmark_runner.py` 加：
      - T1（`warmup=0`：`_single_request` 调用次数 == `max`，`results` 长度 == `max`，等价现状）
      - T2（`warmup=3, max=5`：`_single_request` 共 8 次，`results` 恰 5 条）
      - T3（计时分离：warmup 假请求 `sleep(δ)`、测量相极快 → `wall_clock_s` < 总耗时）
      - T4（顺序性：前 3 次调用早于测量相首个请求）
      - T5（开环：`request_rate` 设置时 warmup 照发 N，`_arrival_offsets` count == `max`）
      - T6（HTTP 直连：`api_base` 设置时 warmup 走 `single_request_sync`，`acompletion` 未被触及）
- [x] Step 2: Green — 实现：
      - `BenchmarkConfig` 尾部加 `num_warmup_requests: int = 0`。
      - 抽 `async def _closed_loop(config, queue, start, results, executor)`（照搬现 `worker()`，去掉 `started`）。
      - `run_benchmark`：`http_executor` 创建后、测量 `start` 前，插入 warmup 相（`N>0` 时建 N 条 prompt 队列 → `warmup_start=monotonic()` → `await _closed_loop(..., [], http_executor)` 丢弃结果）；测量相 `start` 后移；闭环测量改调 `_closed_loop`。
      - 跑通 `python -m pytest tests/test_benchmark_runner.py -v`。
- [x] Step 3: 全量回归 + `black .` + commit `feat(benchmark): warmup phase via reusable closed-loop, exclude from metrics (Task 1)`。

---

### Task 2: CLI 接线（`--num-warmup-requests` + 校验 + 条件回显）

- [x] Step 1: Red — 在 `tests/test_benchmark_cli.py` 加 T7：
      - `--num-warmup-requests 3` → `config.num_warmup_requests == 3`。
      - 负值 → `SystemExit` 且退出码 2。
      - summary 含 `num_warmup_requests` 当且仅当 `N>0`（`N=0` 时键不存在）。
- [x] Step 2: Green — `__main__.py`：加 `--num-warmup-requests`（`type=int, default=0`）；`< 0` → `parser.error`；接线进 `BenchmarkConfig`；`N>0` 时写 `summary["num_warmup_requests"]`。跑通 CLI 测试 + 全量回归。
- [x] Step 3: `black .` + commit `feat(benchmark): --num-warmup-requests CLI flag (Task 2)`。

---

### Task 3: 文档同步

- [x] Step 1: 更新 `docs/benchmark.md`（新增 `--num-warmup-requests` 用法 + "warmup 独立超时预算/一律闭环/指标丢弃"三条语义说明）；更新根 `CLAUDE.md`、`src/llmperf/benchmark/CLAUDE.md`、`tests/CLAUDE.md`、`docs/code-summary.md` 的 changelog 与测试计数；回填本 plan 各 checkbox。
- [x] Step 2: commit `docs(benchmark): document warmup requests (Task 3)`。

---

## 验收标准

1. Task 0 后全仓测试可收集且全绿（基线恢复）。
2. `--num-warmup-requests 0`（默认）下 summary 与现状逐字节一致。
3. `--num-warmup-requests N>0` 下：多发 N 条请求、其指标不入统计、`wall_clock_s` 不含 warmup 耗时。
4. 三种测量模式均可叠加 warmup；开环泊松调度不受 warmup 干扰。
5. `metrics.py` 全程零改动，其测试无回归。
