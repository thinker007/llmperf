# benchmark HTTP 直连模式 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 `src/llmperf/benchmark/` 新增 `--api-base` HTTP 直连模式：绕开 litellm SDK，用 `requests`（经线程池桥接进 asyncio）直连 OpenAI 兼容 `/chat/completions` 流式端点，手工解析 SSE。不传该参数时 litellm 路径行为与 JSON 输出零变化。

**Architecture:** 新增一个纯发送层模块 `http_client.py`（纯函数 SSE 解析 + 一个自洽的同步请求函数），`runner.py` 抽出共享的 `_build_body` 后把 `_single_request` 改造成分发层，`__main__.py` 接线两个新 CLI 参数。4 个原子任务，各自 Red→Green→`black`→commit；最后文档同步收尾。

**Tech Stack:** Python 3.8–3.10、`requests`（已有依赖）、`concurrent.futures.ThreadPoolExecutor`、`asyncio`（`loop.run_in_executor`，**不用** `asyncio.to_thread`——3.9+ 才有）、`functools.partial`、`litellm.token_counter`（复用）、`pytest`（`monkeypatch`，无 `pytest-asyncio`）、`black`。

**参考 spec:** `docs/superpowers/specs/2026-07-22-benchmark-http-direct-mode-design.md`（§3 并发模型、§4 数据模型、§5 CLI、§6 测试矩阵）。

> **状态：实现已完成，待全量回归验证。** 提交将在本会话内完成（Bash 工具临时不可用期间已用 Write/Edit 完成全部 Task 1-4 的代码与测试）。

## Global Constraints

- 类型注解**必须**用 `typing.List/Dict/Tuple/Optional`，**禁止** `list[str]` / `X | None`（`requires-python >=3.8,<3.11`）。
- **零新增依赖**：仅 `requests`（已有）+ 标准库（`functools`/`concurrent.futures`/`json`）。
- 异步测试用 `asyncio.run()` 在同步 `def test_...()` 内驱动；同步的 `single_request_sync` 直接普通函数调用测试，无需 `asyncio.run`。
- 外部依赖经 `monkeypatch.setattr(http_client, "requests", <fake module/object>)` 注入，不发真实网络请求、不起本地 HTTP server。
- **litellm 路径的输出键集合与形状不变**，既有 70 例测试不得回退。
- 每次源码改动后 `black .` 再 commit；conventional commits。
- 全量回归：`python -m pytest tests/ -v`。

---

### Task 1: `http_client.py` — 纯函数 SSE 解析 + 自洽的同步请求函数

**Files:**
- Create: `src/llmperf/benchmark/http_client.py`
- Test: `tests/test_benchmark_http_client.py`

**Interfaces:**
- `parse_sse_line(raw_line: bytes) -> Optional[Dict[str, Any]]`（spec §3.5）
- `extract_delta_text(data: Dict[str, Any]) -> Optional[str]`（spec §3.5，H3 同款空 `choices` 守卫）
- `single_request_sync(config: "BenchmarkConfig", prompt_text: str, num_input_tokens: int, sampled_max_tokens: int, request_timeout: Optional[float], body: Dict[str, Any]) -> Dict[str, Any]` —— 完整同步实现：构造 headers（`--api-key-env` 读取，缺失时 print 告警、不带 Authorization 头）、`requests.post(..., stream=True)`、逐行 `parse_sse_line`/`extract_delta_text`、TTFT/文本累计、`token_counter` 计数、`empty_response` 判定；**全程 try/except，绝不外抛**（A2 同款契约，直接在本函数内自洽，不依赖调用方再包一层）。

- [ ] Step 1: Red — T1（`parse_sse_line` 四种输入）+ T2（`extract_delta_text` 含空 `choices`）+ T4/T5/T6/T7（`single_request_sync` happy path / 非 200 / 网络异常 / 空流）
- [ ] Step 2: Green — 实现 `http_client.py`，跑通 `python -m pytest tests/test_benchmark_http_client.py -v`
- [ ] Step 3: 全量回归 + `black .` + commit `feat(benchmark): add http_client with pure SSE parsing + self-contained sync request (Task 1)`

### Task 2: `runner.py` 接线 —— 抽出 `_build_body` + `_single_request` 分发 + 专属线程池

**Files:**
- Modify: `src/llmperf/benchmark/runner.py`（`BenchmarkConfig.api_base`/`api_key_env` 字段；抽出 `_build_body`；`_single_request` 分支；`run_benchmark` 按需建线程池）
- Test: `tests/test_benchmark_runner.py`（T3、T8、T9、T10）

**Interfaces:**
- `_build_body(config: BenchmarkConfig, prompt_text: str, sampled_max_tokens: int) -> Dict[str, Any]` —— 两条路径共用的纯函数（spec §3.3）
- `_single_request` 新增分支：`config.api_base is not None` 时经 `loop.run_in_executor(executor, functools.partial(http_client.single_request_sync, ...))` 调度
- `run_benchmark`：检测到 `config.api_base` 时创建 `ThreadPoolExecutor(max_workers=config.num_concurrent_requests)`，运行结束 `shutdown(wait=False)`；闭环/开环两条调度路径复用同一个 executor 实例

- [ ] Step 1: Red — T3（`_build_body` 与现状 litellm body 字段一致的回归测试）+ T8（分发隔离：`api_base` 设置时 `acompletion` 不被触及）+ T9（闭环+开环各一次端到端，配合假 `http_client.single_request_sync`）+ T10（专属线程池峰值并发 == N，不被全局 executor 封顶）
- [ ] Step 2: Green — 跑通 `python -m pytest tests/test_benchmark_runner.py -v`
- [ ] Step 3: 全量回归 + `black .` + commit `feat(benchmark): dispatch to HTTP-direct backend with dedicated thread pool (Task 2)`

### Task 3: CLI 接线（`--api-base` / `--api-key-env` + summary 回显）

**Files:**
- Modify: `src/llmperf/benchmark/__main__.py`（两个新参数、config 接线、`summary["backend"]`/`summary["api_base"]` 条件回显）
- Test: `tests/test_benchmark_cli.py`（T11）

- [ ] Step 1: Red — T11（`--api-base` 接线进 config；缺失 `--api-key-env` 对应环境变量时 stderr 告警但不退出；summary 含 `backend` 键，`api_base` 仅设置时写入）
- [ ] Step 2: Green + 全量回归
- [ ] Step 3: `black .` + commit `feat(benchmark): --api-base/--api-key-env CLI flags (Task 3)`

### Task 4: 文档同步

**Files:**
- Modify: `docs/benchmark.md`（HTTP 直连模式用法、协议假设、线程池限制说明）
- Modify: `src/llmperf/benchmark/CLAUDE.md`、`tests/CLAUDE.md`、根 `CLAUDE.md`（状态与测试数）
- Modify: 本 plan 勾选 + spec 回填状态行

- [ ] Step 1: 更新用户文档与各级 CLAUDE.md
- [ ] Step 2: commit `docs(benchmark): document HTTP-direct mode (Task 4)`
