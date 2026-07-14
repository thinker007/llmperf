# benchmark 生产可用性硬化 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在已完工的 `src/llmperf/benchmark/` 上叠加运行时正确性（A 簇）与健壮性边界（B 簇）硬化，使其从 happy-path 可跑推进到生产可用，不改架构、不改 JSON 输出结构。

**Architecture:** 纯增量修改四个源文件（`runner.py` / `__main__.py`），配套扩充两个测试文件（`test_benchmark_runner.py` / `test_benchmark_cli.py`）。8 个原子任务 A1→A2→A3→A4→B1→B2→B3→B4，各自 Red→Green→`black`→commit；最后一个文档同步任务收尾。

**Tech Stack:** Python 3.8–3.10、`litellm`（`acompletion`/`token_counter`）、`asyncio`、`pytest`（`monkeypatch`/`capsys`，无 `pytest-asyncio`）、`black`。

**参考 spec:** `docs/superpowers/specs/2026-07-15-benchmark-production-hardening-design.md`（§3 A 簇、§4 B 簇、§5 测试矩阵、§6 影响面）。

## Global Constraints

以下为项目级约束，**每个任务都隐式包含**：

- 类型注解**必须**用 `typing.List/Dict/Tuple/Optional`，**禁止** `list[str]` / `X | None`（`requires-python >=3.8,<3.11`）。
- **零新增依赖**：仅 `litellm` + 标准库（`asyncio`/`statistics`/`math`/`pathlib`/`random`/`sys`）。测试仅用 pytest 内置的 `monkeypatch`/`capsys`。
- 异步测试用 `asyncio.run()` 在普通同步 `def test_...()` 内驱动（仓库无 `pytest-asyncio`）。
- 外部依赖在测试中一律通过 `monkeypatch.setattr(runner, "acompletion"/"token_counter"/"sample_prompt"/"sample_output_len", ...)` 注入，不连真实网络、不起 Ray。
- **不改 summary / individual JSON 的键集合与形状**（下游读取脚本零改动）。
- 每个源码改动后先跑 `black .` 再 commit；提交信息用 conventional commits，结尾附 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`。
- 全量回归命令：`python -m pytest tests/ -v`（每个任务 Green 后都应保持全绿，既有 26 用例不得回退）。

---

### Task A1: delta 防御式提取 `extract_content`

**Files:**
- Modify: `src/llmperf/benchmark/runner.py`（新增模块级 `_get`/`extract_content`；重写 `_single_request` 采集循环 62-71）
- Test: `tests/test_benchmark_runner.py`（新增 `extract_content` 四形态用例）

**Interfaces:**
- Produces: `extract_content(delta: Any) -> Optional[str]` —— 从 litellm 流式 delta 取增量文本，兼容 `dict`（含子类，走 `.get`）与 pydantic 对象（走 `getattr`）；优先 `reasoning_content`，其次 `content`。后续 A2/A3/A4/B2 均在 A1 重写后的 `_single_request` 上继续修改。

- [ ] **Step 1: Write the failing tests**

在 `tests/test_benchmark_runner.py` 顶部 `_Delta` 类下方新增一个"只支持 getattr、不支持 .get"的对象桩，并追加四形态测试：

```python
class _ObjDelta:
    """模拟 litellm 新版 pydantic delta：只有属性，无 .get。"""

    def __init__(self, content=None, reasoning_content=None):
        self.content = content
        self.reasoning_content = reasoning_content


def test_extract_content_dict():
    assert runner.extract_content({"content": "x"}) == "x"


def test_extract_content_dict_reasoning_priority():
    assert runner.extract_content(
        {"reasoning_content": "r", "content": "x"}
    ) == "r"


def test_extract_content_object_getattr_only():
    # pydantic-like：无 .get，只能 getattr。
    assert runner.extract_content(_ObjDelta(content="x")) == "x"


def test_extract_content_empty_returns_none():
    assert runner.extract_content({}) is None
    assert runner.extract_content(_ObjDelta()) is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_benchmark_runner.py -k extract_content -v`
Expected: FAIL —— `AttributeError: module 'llmperf.benchmark.runner' has no attribute 'extract_content'`

- [ ] **Step 3: Add `extract_content` + rewrite the collection loop**

在 `runner.py` 中，`_single_request` 定义之前（`from ... import` 之后）新增：

```python
def _get(delta: Any, key: str) -> Optional[str]:
    """dict（含子类）走 .get，对象走 getattr。"""
    if isinstance(delta, dict):
        return delta.get(key, None)
    return getattr(delta, key, None)


def extract_content(delta: Any) -> Optional[str]:
    """从 litellm 流式 delta 取增量文本，兼容 dict 与 pydantic 对象。

    优先 reasoning_content，其次 content（保留 runner 原语义）。
    """
    reasoning = _get(delta, "reasoning_content")
    content = _get(delta, "content")
    return reasoning or content
```

把 `_single_request` 的 `try` 块内采集循环（现 62-71 行）替换为：

```python
        async for tok in response:
            delta = tok.choices[0].delta
            if not delta:
                continue
            piece = extract_content(delta)
            if piece:
                if ttft == 0:
                    ttft = time.monotonic() - start
                generated_text += piece
```

（等价性：原 guard `content or reasoning_content` 触发 TTFT，追加时 `reasoning_content` 优先——新代码 `piece = reasoning or content` 完全一致。）

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_benchmark_runner.py -v`
Expected: PASS —— 新 4 用例 + 既有 5 用例（`ttft_and_text`/`counts_reasoning_content`/`error_extracts`/`completes_bounded`/`concurrency_stays_at_n`）全绿。

- [ ] **Step 5: Format & commit**

```bash
cd /Users/gecanhui/github_thinker007/llmperf && black . -q
git add src/llmperf/benchmark/runner.py tests/test_benchmark_runner.py
git commit -m "fix(benchmark): defensive delta extraction for dict and pydantic objects

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task A2: `_single_request` 绝不外抛（token_counter 纳入兜底）

**Files:**
- Modify: `src/llmperf/benchmark/runner.py`（将 `token_counter` 计算移入 `try`；`num_output_tokens` 预置 0）
- Test: `tests/test_benchmark_runner.py`（新增 2 用例）

**Interfaces:**
- Consumes: A1 重写后的 `_single_request`。
- Produces: `_single_request` 契约收紧——**任何情况下都返回结果 dict，绝不外抛异常**；`token_counter` 抛错时该请求 `error_code` 非 `None`、`num_output_tokens == 0`。`run_benchmark` 因此不再被单点异常整体拖崩。

- [ ] **Step 1: Write the failing tests**

在 `tests/test_benchmark_runner.py` 追加：

```python
def test_single_request_survives_token_counter_error(monkeypatch):
    # token_counter 抛异常时，_single_request 不外抛，返回带 error_code 的结果。
    chunks = [_chunk(content="hi")]
    monkeypatch.setattr(runner, "acompletion", _make_acompletion(chunks))

    def _boom(model, text):
        raise RuntimeError("tokenizer load failed")

    monkeypatch.setattr(runner, "token_counter", _boom)

    m = asyncio.run(runner._single_request(_config(), "p", 1, 50))
    assert m[cm.ERROR_CODE] is not None
    assert m[cm.NUM_OUTPUT_TOKENS] == 0


def test_run_benchmark_keeps_results_when_token_counter_errors(monkeypatch):
    # 单点异常不得让 gather 整体崩溃、丢弃已完成结果。
    monkeypatch.setattr(
        runner, "acompletion", _make_acompletion([_chunk(content="hi")])
    )

    def _boom(model, text):
        raise RuntimeError("tokenizer load failed")

    monkeypatch.setattr(runner, "token_counter", _boom)
    monkeypatch.setattr(runner, "sample_prompt", lambda *a, **k: ("p", 1))
    monkeypatch.setattr(runner, "sample_output_len", lambda *a, **k: 10)

    results, wall = asyncio.run(
        runner.run_benchmark(_config(max_num_completed_requests=4))
    )
    assert len(results) == 4  # 全部保留，未因异常丢失
    assert all(r[cm.ERROR_CODE] is not None for r in results)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_benchmark_runner.py -k "token_counter" -v`
Expected: FAIL —— `token_counter` 在 `try` 外，异常冒出：`test_single_request_survives...` 抛 `RuntimeError`（未被捕获），`test_run_benchmark_keeps_results...` 因 `asyncio.gather` 抛出而失败。

- [ ] **Step 3: Move `token_counter` into the try block**

在 `_single_request` 中：

1. 变量初始化区（`e2e = 0.0` 之后）增加一行 `num_output_tokens = 0`：

```python
    ttft = 0.0
    generated_text = ""
    error_code: Optional[str] = None
    error_msg = ""
    e2e = 0.0
    num_output_tokens = 0
```

2. 将 `try` 末尾 `e2e = time.monotonic() - start` 之后、以及原本位于 `except` 块**之后**的这一行

```python
    num_output_tokens = token_counter(model=config.model, text=generated_text)
```

删除其在 `except` 之后的位置，改为放进 `try` 内、紧接 `e2e = ...`：

```python
        e2e = time.monotonic() - start
        num_output_tokens = token_counter(model=config.model, text=generated_text)
    except Exception as e:
        error_msg = str(e)
        error_code = str(getattr(e, "status_code", None) or "unknown")

    return {
        cm.TTFT: ttft,
        ...
    }
```

（`num_output_tokens` 现由 `try` 内赋值；异常路径下保持初始化的 `0`。）

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_benchmark_runner.py -v`
Expected: PASS —— 新 2 用例 + 既有全绿。

- [ ] **Step 5: Format & commit**

```bash
cd /Users/gecanhui/github_thinker007/llmperf && black . -q
git add src/llmperf/benchmark/runner.py tests/test_benchmark_runner.py
git commit -m "fix(benchmark): never let _single_request escape exceptions to worker pool

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task A3: 空流判失败 `empty_response`

**Files:**
- Modify: `src/llmperf/benchmark/runner.py`（`try` 内 token_counter 后加空流判定）
- Test: `tests/test_benchmark_runner.py`（runner 侧 1 用例）、`tests/test_benchmark_metrics.py`（metrics 侧 1 用例）

**Interfaces:**
- Consumes: A2 后的 `_single_request`（token_counter 已在 `try` 内）。
- Produces: 流正常结束但 `generated_text == ""` 时，`error_code = "empty_response"`。该请求被 `metrics.summarize` 按 `error_code is not None` 归为错误，不进入成功分位。

- [ ] **Step 1: Write the failing tests**

在 `tests/test_benchmark_runner.py` 追加：

```python
def test_single_request_empty_stream_marked_failed(monkeypatch):
    # 流正常但零 content：判为 empty_response，不得当成功。
    monkeypatch.setattr(runner, "acompletion", _make_acompletion([]))
    monkeypatch.setattr(runner, "token_counter", lambda model, text: 0)

    m = asyncio.run(runner._single_request(_config(), "p", 1, 50))
    assert m[cm.ERROR_CODE] == "empty_response"
    assert m[cm.NUM_OUTPUT_TOKENS] == 0
```

在 `tests/test_benchmark_metrics.py` 追加（守护 empty_response 计入错误、不入成功分位）：

```python
def test_summarize_counts_empty_response_as_error():
    from llmperf.benchmark.metrics import summarize

    results = [
        {
            cm.TTFT: 0.0,
            cm.E2E_LAT: 0.0,
            cm.NUM_INPUT_TOKENS: 5,
            cm.NUM_OUTPUT_TOKENS: 0,
            cm.ERROR_CODE: "empty_response",
            cm.ERROR_MSG: "",
        }
    ]
    out = summarize(results, wall_clock_s=1.0)
    assert out[cm.NUM_ERRORS] == 1
    assert out[cm.NUM_COMPLETED_REQUESTS] == 0
    assert out[cm.ERROR_CODE_FREQ]["empty_response"] == 1
```

（`tests/test_benchmark_metrics.py` 顶部若尚未 `from llmperf import common_metrics as cm`，沿用该文件既有的 `cm` 导入；若无则加一行 `from llmperf import common_metrics as cm`。）

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_benchmark_runner.py::test_single_request_empty_stream_marked_failed tests/test_benchmark_metrics.py::test_summarize_counts_empty_response_as_error -v`
Expected: FAIL —— runner 侧 `error_code` 为 `None`（≠ `"empty_response"`）；metrics 侧该请求被计入成功、`num_errors == 0`。

- [ ] **Step 3: Add the empty-stream check**

在 `_single_request` 的 `try` 内，`num_output_tokens = token_counter(...)` 之后追加：

```python
        e2e = time.monotonic() - start
        num_output_tokens = token_counter(model=config.model, text=generated_text)
        if generated_text == "":
            error_code = "empty_response"
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_benchmark_runner.py tests/test_benchmark_metrics.py -v`
Expected: PASS —— 新 2 用例 + 既有全绿（注意：既有 `test_single_request_ttft_and_text` 有非空 content，不受影响）。

- [ ] **Step 5: Format & commit**

```bash
cd /Users/gecanhui/github_thinker007/llmperf && black . -q
git add src/llmperf/benchmark/runner.py tests/test_benchmark_runner.py tests/test_benchmark_metrics.py
git commit -m "fix(benchmark): classify empty stream as empty_response failure

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task A4: `status_code == 0` 不被 `or` 吞掉

**Files:**
- Modify: `src/llmperf/benchmark/runner.py`（`except` 内 status_code 提取改显式判 `None`）
- Test: `tests/test_benchmark_runner.py`（1 用例）

**Interfaces:**
- Consumes: A2 后的 `except` 块。
- Produces: `status_code == 0` 的异常 → `error_code == "0"`（而非 `"unknown"`）。

- [ ] **Step 1: Write the failing test**

在 `tests/test_benchmark_runner.py` 追加：

```python
def test_single_request_status_code_zero_not_swallowed(monkeypatch):
    exc = RuntimeError("boom")
    exc.status_code = 0  # falsy 但合法
    monkeypatch.setattr(runner, "acompletion", _make_acompletion([], raise_exc=exc))
    monkeypatch.setattr(runner, "token_counter", lambda model, text: 0)

    m = asyncio.run(runner._single_request(_config(), "p", 1, 50))
    assert m[cm.ERROR_CODE] == "0"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_benchmark_runner.py::test_single_request_status_code_zero_not_swallowed -v`
Expected: FAIL —— `assert 'unknown' == '0'`（`0 or "unknown"` → `"unknown"`）。

- [ ] **Step 3: Replace the `except` body**

把 `_single_request` 的 `except` 块：

```python
    except Exception as e:
        error_msg = str(e)
        error_code = str(getattr(e, "status_code", None) or "unknown")
```

替换为：

```python
    except Exception as e:
        error_msg = str(e)
        sc = getattr(e, "status_code", None)
        error_code = "unknown" if sc is None else str(sc)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_benchmark_runner.py -v`
Expected: PASS —— 新用例通过，且既有 `test_single_request_error_extracts_from_exception`（`status_code=429` → `"429"`）不回退。

- [ ] **Step 5: Format & commit**

```bash
cd /Users/gecanhui/github_thinker007/llmperf && black . -q
git add src/llmperf/benchmark/runner.py tests/test_benchmark_runner.py
git commit -m "fix(benchmark): preserve status_code 0 instead of coercing to unknown

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task B1: `parse_metadata` 友好报错

**Files:**
- Modify: `src/llmperf/benchmark/__main__.py`（`parse_metadata` 无 `=` 抛 `ArgumentTypeError`；`--metadata` 改 `type=parse_metadata`；`main` 删手工调用）
- Test: `tests/test_benchmark_cli.py`（新增 1 用例；既有 2 回归用例保留）

**Interfaces:**
- Produces: `parse_metadata(raw: str) -> Dict[str, str]`，遇不含 `=` 的项抛 `argparse.ArgumentTypeError`；`--metadata` 经 argparse 以退出码 2 友好报错。`main()` 中 `args.metadata` 自此为 `Dict`。

- [ ] **Step 1: Write the failing test**

在 `tests/test_benchmark_cli.py` 顶部补 `import argparse`，追加：

```python
def test_parse_metadata_missing_equals_raises():
    with pytest.raises(argparse.ArgumentTypeError):
        cli.parse_metadata("foo")
```

（既有 `test_parse_metadata_splits_on_first_equals` 与 `test_parse_metadata_empty` 作为回归保留，不改。）

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_benchmark_cli.py::test_parse_metadata_missing_equals_raises -v`
Expected: FAIL —— 当前 `"foo".split("=", 1)` 解包抛 `ValueError`（非 `ArgumentTypeError`）。

- [ ] **Step 3: Harden `parse_metadata` and wire it into argparse**

`__main__.py` 中把 `parse_metadata` 循环体改为带校验：

```python
def parse_metadata(raw: str) -> Dict[str, str]:
    """'a=1,b=x=y' → {'a':'1','b':'x=y'}。按首个 '=' 分割，重复 key 后覆盖。

    不含 '=' 的项抛 argparse.ArgumentTypeError，交由 argparse 友好报错。
    """
    out: Dict[str, str] = {}
    if not raw:
        return out
    for item in raw.split(","):
        if "=" not in item:
            raise argparse.ArgumentTypeError(
                "metadata item '%s' must be key=value" % item
            )
        key, value = item.split("=", 1)
        out[key] = value
    return out
```

把 `_build_parser` 中的 `--metadata` 定义

```python
    p.add_argument("--metadata", type=str, default="")
```

改为：

```python
    p.add_argument("--metadata", type=parse_metadata, default={})
```

在 `main()` 中删除手工解析行，直接使用 `args.metadata`（已是 `Dict`）：

```python
    args = _build_parser().parse_args(argv)
    # args.metadata 已由 type=parse_metadata 解析为 Dict（删除原手工调用）。
```

（即移除 `user_metadata = parse_metadata(args.metadata)` 这一行；`argparse` 已在文件顶部导入，无需新增 import。）

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_benchmark_cli.py -v`
Expected: PASS —— 新用例 + 2 条回归（`splits_on_first_equals`/`empty`）全绿。

- [ ] **Step 5: Format & commit**

```bash
cd /Users/gecanhui/github_thinker007/llmperf && black . -q
git add src/llmperf/benchmark/__main__.py tests/test_benchmark_cli.py
git commit -m "fix(benchmark): friendly error for malformed --metadata via argparse type

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task B2: 单请求 timeout 内部派生为剩余预算

**Files:**
- Modify: `src/llmperf/benchmark/runner.py`（`_single_request` 增 `request_timeout` 可选参；`body["timeout"]` 用之；`worker` 传剩余预算）
- Test: `tests/test_benchmark_runner.py`（2 用例）

**Interfaces:**
- Consumes: A1–A4 后的 `_single_request` 与 `run_benchmark::worker`。
- Produces: `_single_request(config, prompt_text, num_input_tokens, sampled_max_tokens, request_timeout: Optional[float] = None)`；`request_timeout is None` 时 `body["timeout"]` 回退 `config.timeout`（保持既有直接调用兼容），否则用传入值。`worker` 每次调用前传 `max(0.0, config.timeout - elapsed)`，使任何单请求 HTTP 超时不越过全局 deadline。

- [ ] **Step 1: Write the failing tests**

在 `tests/test_benchmark_runner.py` 追加一个能捕获 `body` 的假 acompletion 与两条断言：

```python
def _make_capturing_acompletion(captured, chunks):
    async def _fake(**body):
        captured.update(body)

        async def _gen():
            for c in chunks:
                yield c

        return _gen()

    return _fake


def test_single_request_passes_explicit_request_timeout(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        runner,
        "acompletion",
        _make_capturing_acompletion(captured, [_chunk(content="x")]),
    )
    monkeypatch.setattr(runner, "token_counter", lambda model, text: 1)

    asyncio.run(
        runner._single_request(_config(timeout=30), "p", 1, 50, 12.5)
    )
    assert captured["timeout"] == 12.5


def test_worker_passes_remaining_budget_not_full_timeout(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        runner,
        "acompletion",
        _make_capturing_acompletion(captured, [_chunk(content="x")]),
    )
    monkeypatch.setattr(runner, "token_counter", lambda model, text: 1)
    monkeypatch.setattr(runner, "sample_prompt", lambda *a, **k: ("p", 1))
    monkeypatch.setattr(runner, "sample_output_len", lambda *a, **k: 10)

    asyncio.run(
        runner.run_benchmark(
            _config(timeout=30, num_concurrent_requests=1, max_num_completed_requests=1)
        )
    )
    # 透传的单请求 timeout 是"剩余预算"，必 <= 全局 timeout。
    assert captured["timeout"] <= 30
    assert captured["timeout"] > 0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_benchmark_runner.py -k "request_timeout or remaining_budget" -v`
Expected: FAIL —— `test_single_request_passes_explicit_request_timeout` 抛 `TypeError`（`_single_request` 尚不接受第 5 个参数）。

- [ ] **Step 3: Add the optional param and derive remaining budget in worker**

修改 `_single_request` 签名，增加末位可选参：

```python
async def _single_request(
    config: BenchmarkConfig,
    prompt_text: str,
    num_input_tokens: int,
    sampled_max_tokens: int,
    request_timeout: Optional[float] = None,
) -> Dict[str, Any]:
```

`body` 中的 `timeout` 改为回退式：

```python
    body: Dict[str, Any] = {
        "model": config.model,
        "messages": messages,
        "stream": True,
        "max_tokens": sampled_max_tokens,
        # 单请求 HTTP 超时 = 剩余全局预算（worker 传入）；直接调用回退全局 timeout。
        "timeout": config.timeout if request_timeout is None else request_timeout,
    }
```

`run_benchmark::worker` 循环改为计算并传入剩余预算（复用循环顶部的 `elapsed`）：

```python
    async def worker() -> None:
        nonlocal started
        while True:
            elapsed = time.monotonic() - start
            if elapsed >= config.timeout:
                break  # 到点停止取新任务，不 cancel 在途请求。
            try:
                text, n_in, max_tokens = queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            started += 1
            request_timeout = max(0.0, config.timeout - elapsed)
            m = await _single_request(
                config, text, n_in, max_tokens, request_timeout
            )
            results.append(m)
```

（因未 break 即 `elapsed < config.timeout`，故 `request_timeout > 0`。）

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_benchmark_runner.py -v`
Expected: PASS —— 新 2 用例 + 既有全绿（既有 3 个直接调用 `_single_request(..., 50)` 的用例因 `request_timeout` 有默认值 `None` 而不受影响）。

- [ ] **Step 5: Format & commit**

```bash
cd /Users/gecanhui/github_thinker007/llmperf && black . -q
git add src/llmperf/benchmark/runner.py tests/test_benchmark_runner.py
git commit -m "fix(benchmark): derive per-request timeout from remaining global budget

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task B3: metadata 冲突——内建键优先 + stderr warning

**Files:**
- Modify: `src/llmperf/benchmark/__main__.py`（新增 `import sys`；`summary.update(user_metadata)` 换逐键合并）
- Test: `tests/test_benchmark_cli.py`（1 用例，用 `monkeypatch` + `capsys`）

**Interfaces:**
- Consumes: B1 后的 `args.metadata`（`Dict`）。
- Produces: 用户 metadata 键与内建键（`model`/`num_concurrent_requests`/`timestamp`/`litellm_version` 等）冲突时**跳过并向 stderr 打 warning**，内建键永不被覆盖；非冲突键正常写入。JSON 结构不变。

- [ ] **Step 1: Write the failing test**

在 `tests/test_benchmark_cli.py` 追加（monkeypatch 掉 `run_benchmark` 避免网络；`summarize` 用真实实现）：

```python
def test_metadata_does_not_override_builtin_keys(monkeypatch, capsys):
    async def _fake_run_benchmark(config):
        return ([], 0.0)

    monkeypatch.setattr(cli, "run_benchmark", _fake_run_benchmark)

    cli.main(
        [
            "--model",
            "real-model",
            "--metadata",
            "model=fake,extra=ok",
        ]
    )
    out, err = capsys.readouterr()
    summary = json.loads(out)
    assert summary["model"] == "real-model"  # 内建键未被覆盖
    assert summary["extra"] == "ok"  # 非冲突键写入
    assert "model" in err and "ignored" in err  # stderr 有冲突 warning


# 文件顶部若无 json 导入需补：import json
```

（`tests/test_benchmark_cli.py` 顶部补 `import json`。）

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_benchmark_cli.py::test_metadata_does_not_override_builtin_keys -v`
Expected: FAIL —— 当前 `summary.update(user_metadata)` 使 `summary["model"] == "fake"`，断言 `== "real-model"` 失败。

- [ ] **Step 3: Replace unconditional update with per-key merge**

`__main__.py` 顶部 import 区新增：

```python
import sys
```

把 `main()` 中的

```python
    summary.update(user_metadata)
```

替换为：

```python
    for k, v in args.metadata.items():
        if k in summary:
            print(
                "warning: metadata key '%s' conflicts with a built-in field "
                "and was ignored" % k,
                file=sys.stderr,
            )
            continue
        summary[k] = v
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_benchmark_cli.py -v`
Expected: PASS —— 新用例 + 既有全绿。

- [ ] **Step 5: Format & commit**

```bash
cd /Users/gecanhui/github_thinker007/llmperf && black . -q
git add src/llmperf/benchmark/__main__.py tests/test_benchmark_cli.py
git commit -m "fix(benchmark): built-in summary keys win over --metadata, warn on conflict

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task B4: 非法数值 sanity 校验

**Files:**
- Modify: `src/llmperf/benchmark/__main__.py`（`main()` 组装 config 前校验并 `parser.error`）
- Test: `tests/test_benchmark_cli.py`（1 用例，参数化三种非法输入）

**Interfaces:**
- Consumes: B1 后的 `_build_parser()` / `args`。
- Produces: `num_concurrent_requests < 1` / `max_num_completed_requests < 1` / `timeout <= 0` 任一触发 → `parser.error(...)` → `SystemExit(2)` + 友好信息，不再静默产空报告。

- [ ] **Step 1: Write the failing test**

在 `tests/test_benchmark_cli.py` 追加：

```python
@pytest.mark.parametrize(
    "bad_args",
    [
        ["--num-concurrent-requests", "0"],
        ["--max-num-completed-requests", "-1"],
        ["--timeout", "0"],
    ],
)
def test_main_rejects_non_positive_numeric_args(bad_args):
    with pytest.raises(SystemExit) as exc:
        cli.main(["--model", "m"] + bad_args)
    assert exc.value.code == 2  # parser.error 退出码
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_benchmark_cli.py::test_main_rejects_non_positive_numeric_args -v`
Expected: FAIL —— 无校验时 `main` 会带非法值进入 `asyncio.run(run_benchmark(...))`（`num_concurrent=0` → 0 个 worker → 空结果返回，不抛 `SystemExit`），测试 `pytest.raises(SystemExit)` 失败。

- [ ] **Step 3: Add sanity validation in `main`**

在 `main()` 中，`parser = _build_parser()` 拿到 parser 后、组装 `config` 之前插入校验。将开头两行

```python
def main(argv: Optional[List[str]] = None) -> None:
    args = _build_parser().parse_args(argv)
```

改为：

```python
def main(argv: Optional[List[str]] = None) -> None:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.num_concurrent_requests < 1:
        parser.error("--num-concurrent-requests must be >= 1")
    if args.max_num_completed_requests < 1:
        parser.error("--max-num-completed-requests must be >= 1")
    if args.timeout <= 0:
        parser.error("--timeout must be > 0")
```

（`parser.error` 内部 `sys.exit(2)`；`SystemExit` 由 argparse 抛出。）

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_benchmark_cli.py -v`
Expected: PASS —— 三参数化用例 + 既有全绿。

- [ ] **Step 5: Run full suite & commit**

```bash
cd /Users/gecanhui/github_thinker007/llmperf && black . -q
python -m pytest tests/ -v   # 全量回归：应约 34+ 全绿
git add src/llmperf/benchmark/__main__.py tests/test_benchmark_cli.py
git commit -m "fix(benchmark): reject non-positive numeric CLI args with exit code 2

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task Docs: 同步模块文档与 spec 状态

**Files:**
- Modify: `src/llmperf/benchmark/CLAUDE.md`（测试数、语义变化、`extract_content`/`request_timeout` 接口）
- Modify: `docs/benchmark.md`（空流判失败、timeout 剩余预算语义、metadata 冲突行为、数值校验）
- Modify: `docs/superpowers/specs/2026-07-15-benchmark-production-hardening-design.md`（回填完成状态）
- Modify: `docs/superpowers/plans/2026-07-15-benchmark-production-hardening.md`（本文件 checkbox 回填）

（纯文档同步，无测试步骤；文档更新前先 `Read` 目标文件核对现状——遵循"先读后写"。）

- [ ] **Step 1: 更新 `src/llmperf/benchmark/CLAUDE.md`**

- 测试矩阵表：runner 用例 5→约 12、cli 用例 4→约 8，总数 26→约 34+。
- `runner.py` 接口段补 `extract_content(delta)` 纯函数与"防御式提取兼容 pydantic 对象"说明；`_single_request` 补 `request_timeout: Optional[float]=None`（剩余预算派生）。
- FAQ 或口径段新增：**空流 → `empty_response` 失败**（有意语义收紧）；**单请求 timeout = 剩余全局预算**（最坏 wall clock 从 ~2× 压回 ~1×）。
- Changelog 追加一条 2026-07-15 记录。

- [ ] **Step 2: 更新 `docs/benchmark.md`**

- 新增/更新"行为语义"小节：空流判失败、timeout 剩余预算、`--metadata` 内建键优先 + warning、`num_concurrent`/`max_num`/`timeout` 非正数拒绝（退出码 2）。

- [ ] **Step 3: 回填 spec 与 plan 状态**

- spec §7 或文末标注"已实施完成，见 plan 2026-07-15"。
- 本 plan 全部 checkbox 勾选。

- [ ] **Step 4: Commit**

```bash
cd /Users/gecanhui/github_thinker007/llmperf
git add src/llmperf/benchmark/CLAUDE.md docs/benchmark.md docs/superpowers/specs/2026-07-15-benchmark-production-hardening-design.md docs/superpowers/plans/2026-07-15-benchmark-production-hardening.md
git commit -m "docs(benchmark): sync module docs and spec/plan status for hardening iteration

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Self-Review

**1. Spec coverage（逐条核对）:**

| Spec 条目 | 对应任务 |
|---|---|
| §3 A1 delta 防御式提取 | Task A1 |
| §3 A2 `_single_request` 绝不外抛 | Task A2 |
| §3 A3 空流判失败 | Task A3 |
| §3 A4 status_code 0 不被吞 | Task A4 |
| §4 B1 parse_metadata 友好报错 | Task B1 |
| §4 B2 单请求 timeout 派生 | Task B2 |
| §4 B3 metadata 冲突内建键优先 | Task B3 |
| §4 B4 非法数值 sanity 校验 | Task B4 |
| §5 测试矩阵 8 行 | 各任务 Step 1 全覆盖 |
| §6 影响面（文档/语义变化） | Task Docs |

无遗漏。§1 非目标（C 簇）明确不实现，各任务未触碰。

**2. Placeholder scan:** 无 "TBD/TODO/待补"；每个 code step 均含完整可粘贴代码与精确 old→new 替换；每个 test step 含完整测试函数体。

**3. Type consistency:**
- `extract_content(delta: Any) -> Optional[str]`：A1 定义，A1 之后 `_single_request` 消费，命名一致。
- `_single_request(..., request_timeout: Optional[float] = None)`：B2 定义，worker（同任务）以 5 位置参调用，一致。
- `parse_metadata(raw: str) -> Dict[str, str]`：B1 强化，签名不变；B3 消费 `args.metadata`（B1 起为 `Dict`），一致。
- 既有直接调用 `_single_request(cfg, "p", 1, 50)`（4 参）与 B2 新签名（第 5 参默认 `None`）兼容——已在 B2 Step 4 显式说明既有用例不回退。

**执行顺序依赖：** A2 依赖 A1 的循环重写；A3 依赖 A2 的 token_counter 入 try；A4 依赖 A2 的 except 结构；B2 依赖 A1–A4 后的 `_single_request`；B3 依赖 B1 的 `args.metadata` 为 Dict；B4 依赖 B1 的 parser 变量化。必须按 A1→A2→A3→A4→B1→B2→B3→B4→Docs 顺序执行。
