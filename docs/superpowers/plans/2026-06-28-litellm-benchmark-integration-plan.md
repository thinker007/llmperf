# llmperf 内置 asyncio 压测模块 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** 在 llmperf 仓库内新增 `src/llmperf/benchmark/` 模块——用 asyncio worker-pool 压测 `litellm.acompletion()`，保留 llmperf 的度量灵魂（TTFT/ITL/E2E/吞吐/错误率），扔掉 Ray/threading/transformers/pandas 脚手架。

**Architecture:** 纯函数（prompts 采样、metrics 聚合）+ asyncio 并发引擎（runner）+ argparse CLI（__main__），四文件各司一职。与现有 `token_benchmark_ray.py` / `ray_clients/` **并存**，互不影响，零侵入旧代码。新 CLI：`python -m llmperf.benchmark`。

**Tech Stack:** Python 3.8+、asyncio（stdlib）、statistics（stdlib）、litellm（已是依赖 `litellm>=0.1.738`，提供 `acompletion`/`token_counter`）。**零新增依赖。**

## Global Constraints

每个任务的要求都隐含包含本节，值逐字照抄自 spec / 仓库实测：

- **落地仓库**：仅 llmperf。**绝不修改 litellm 仓库**，litellm 只作为被 import 的库。
- **Python 版本**：`requires-python = ">=3.8, <3.11"`（pyproject.toml:11）。类型注解**必须用 `typing.List/Dict/Tuple/Optional`**，**禁用** `list[str]` / `dict[...]` / `X | None`（3.10+ 语法，3.8 会 SyntaxError）。
- **模块路径**：`src/llmperf/benchmark/`（src 布局）。测试落点：扁平 `tests/`（沿用现有 `tests/test_anthropic_messages_client.py` 约定）。
- **异步测试**：用 `asyncio.run()` 在普通 `def test_...()` 同步函数里驱动。**不引入 pytest-asyncio**（仓库未装，dev 依赖只有 black），**不配 asyncio_mode**。
- **指标常量**：从 `llmperf.common_metrics` import 复用（DRY），不重新定义字符串字面量。
- **门禁**：`black` 格式化 + 全量 `pytest` 绿。**不涉及** litellm 的 `ruff-strict` / `type_discipline_gate`。
- **ITL 口径**：`(e2e − ttft) / (out − 1)` 当 out>1，否则 0（修正 llmperf 原版 `sum÷chunk` 不纯净口径，见 spec §7 决策 2）。
- **错误提取**：`except Exception as e:` 里从 `e` 真实取值——`error_msg = str(e)`、`error_code = str(getattr(e, "status_code", None) or "unknown")`。**不照搬** `litellm_client.py:89-94` 的常量吞噬 bug。

> **源码事实基线**（已逐行核对，照此移植）：
>
> | 用途 | 源文件（llmperf 仓库） | 关键行 |
> |------|----------------------|--------|
> | 流式采集逻辑（照搬骨架，sync→async） | `src/llmperf/ray_clients/litellm_client.py` | 61-103 |
> | 错误吞噬 bug（**修正**，不照搬） | 同上 | 89-94 |
> | prompt 采样（移植，分词器换 token_counter） | `src/llmperf/utils.py` | 59-118、121-134 |
> | output 长度采样（移植） | `src/llmperf/token_benchmark_ray.py` | 78-81（`sample_random_positive_int` 生成 max_tokens） |
> | 指标常量名（import 复用） | `src/llmperf/common_metrics.py` | 2-17 |
> | 文件名清洗正则（照搬） | `src/llmperf/token_benchmark_ray.py` | 339-341 |
> | metadata 解析 bug（**修正** `split("=")`→`split("=",1)`） | 同上 | 480-482 |
> | 采样语料（复制） | `src/llmperf/sonnet.txt` | 整文件 |

---

## Task 1: `prompts.py` — 采样移植（分词器换 token_counter）

**Files:**
- Create: `src/llmperf/benchmark/__init__.py`（空占位，本任务先建包目录；Task 4 填导出）
- Create: `src/llmperf/benchmark/sonnet.txt`（从 `src/llmperf/sonnet.txt` 复制）
- Create: `src/llmperf/benchmark/prompts.py`
- Test: `tests/test_benchmark_prompts.py`

**Interfaces:**
- Consumes: `litellm.token_counter(model: str, text: str) -> int`（外部库）。
- Produces（Task 3 依赖这些精确签名）：
  - `sample_random_positive_int(mean: int, stddev: int) -> int`
  - `sample_prompt(model: str, mean_input_tokens: int, stddev_input_tokens: int, expect_output_tokens: int) -> Tuple[str, int]` — 返回 `(prompt_text, num_input_tokens)`，其中 `num_input_tokens` 是对**最终文本**用 `token_counter` 重算的实际值（与 utils.py:118 返回采样目标值不同——这是 spec §5.2「口径自洽」的有意改进）。
  - `sample_output_len(mean_output_tokens: int, stddev_output_tokens: int) -> int`

- [x] **Step 1: 建包目录 + 复制语料 + 冒烟 token_counter**

```bash
mkdir -p src/llmperf/benchmark
touch src/llmperf/benchmark/__init__.py
cp src/llmperf/sonnet.txt src/llmperf/benchmark/sonnet.txt
python -c "import litellm; print(litellm.token_counter(model='gpt-4o', text='hello world'))"
```
Expected: 打印一个正整数（如 `2`），确认 litellm 可用。

- [x] **Step 2: 写失败测试 `tests/test_benchmark_prompts.py`**

```python
"""prompts.py 采样逻辑单测：纯逻辑，注入确定性 token_counter，无网络。"""

from llmperf.benchmark import prompts


def test_sample_random_positive_int_always_positive():
    # 移植自 utils.py:121-134，gauss 可能出负数，必须重采样到正。
    for _ in range(1000):
        assert prompts.sample_random_positive_int(5, 100) > 0


def test_sample_output_len_positive():
    assert prompts.sample_output_len(150, 80) > 0


def test_sample_prompt_returns_actual_token_count(monkeypatch):
    # 确定性计数器：按空格分词，1 词 = 1 token。
    monkeypatch.setattr(
        prompts, "token_counter", lambda model, text: len(text.split())
    )
    text, n_in = prompts.sample_prompt(
        model="gpt-4o",
        mean_input_tokens=80,
        stddev_input_tokens=0,
        expect_output_tokens=50,
    )
    # 口径自洽：返回的 token 数 == 对最终文本重算的计数（spec §5.2）。
    assert n_in == len(text.split())
    assert n_in >= 1
    assert "output tokens" in text  # 基底 prompt 前缀保留（utils.py:88-90）


def test_sample_prompt_converges_with_small_mean(monkeypatch):
    # mean 远小于基底前缀时，utils.py:96-99 的 while 重采样保证不死循环。
    monkeypatch.setattr(
        prompts, "token_counter", lambda model, text: len(text.split())
    )
    text, n_in = prompts.sample_prompt("gpt-4o", 5, 0, 10)
    assert n_in > 0
```

- [x] **Step 3: 跑测试确认失败**

Run: `python -m pytest tests/test_benchmark_prompts.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'llmperf.benchmark.prompts'`

- [x] **Step 4: 实现 `src/llmperf/benchmark/prompts.py`**

```python
"""按目标 token 数采样 sonnet prompt。移植自 llmperf.utils，分词器换为 litellm.token_counter。"""

import math
import pathlib
import random
from typing import Tuple

from litellm import token_counter

_SONNET_PATH = pathlib.Path(__file__).parent / "sonnet.txt"


def sample_random_positive_int(mean: int, stddev: int) -> int:
    """从高斯分布采样直到得到正整数。移植自 utils.py:121-134。"""
    ret = -1
    while ret <= 0:
        ret = int(random.gauss(mean, stddev))
    return ret


def sample_output_len(mean_output_tokens: int, stddev_output_tokens: int) -> int:
    """为单个请求采样 max_tokens。对应 token_benchmark_ray.py:78-81。"""
    return sample_random_positive_int(mean_output_tokens, stddev_output_tokens)


def sample_prompt(
    model: str,
    mean_input_tokens: int,
    stddev_input_tokens: int,
    expect_output_tokens: int,
) -> Tuple[str, int]:
    """采样一条 prompt，返回 (文本, 实际输入 token 数)。移植自 utils.py:59-118。

    与原版差异：返回值的 token 数对最终文本用 token_counter 重算（口径自洽），
    而非 utils.py:118 返回的采样目标值。
    """
    get_len = lambda text: token_counter(model=model, text=text)

    prompt = (
        "Randomly stream lines from the following text "
        f"with {expect_output_tokens} output tokens. "
        "Don't generate eos tokens:\n\n"
    )
    num_prompt_tokens = sample_random_positive_int(mean_input_tokens, stddev_input_tokens)
    while num_prompt_tokens < get_len(prompt):
        num_prompt_tokens = sample_random_positive_int(mean_input_tokens, stddev_input_tokens)
    remaining = num_prompt_tokens - get_len(prompt)

    with open(_SONNET_PATH, "r") as f:
        sonnet_lines = f.readlines()
    random.shuffle(sonnet_lines)

    sampling = True
    while sampling:
        for line in sonnet_lines:
            line_to_add = line
            if remaining - get_len(line_to_add) < 0:
                line_to_add = line_to_add[: int(math.ceil(remaining))]
                sampling = False
                prompt += line_to_add
                break
            prompt += line_to_add
            remaining -= get_len(line_to_add)

    return prompt, get_len(prompt)
```

- [x] **Step 5: 跑测试确认通过**

Run: `python -m pytest tests/test_benchmark_prompts.py -v`
Expected: PASS（4 passed）

- [x] **Step 6: 格式化 + commit**

```bash
black src/llmperf/benchmark/prompts.py tests/test_benchmark_prompts.py
git add src/llmperf/benchmark/__init__.py src/llmperf/benchmark/sonnet.txt src/llmperf/benchmark/prompts.py tests/test_benchmark_prompts.py
git commit -m "feat(benchmark): add prompt sampling with litellm.token_counter"
```

---

## Task 2: `metrics.py` — 纯函数聚合（换算 + 边界封装）

**Files:**
- Create: `src/llmperf/benchmark/metrics.py`
- Test: `tests/test_benchmark_metrics.py`

**Interfaces:**
- Consumes: `llmperf.common_metrics`（常量名）。
- Produces（Task 3/4 依赖）：
  - `derive_request_metrics(raw: Dict[str, Any]) -> Dict[str, Any]` — 输入单请求 raw（含 `TTFT/E2E_LAT/NUM_INPUT_TOKENS/NUM_OUTPUT_TOKENS`），返回补齐 `INTER_TOKEN_LAT/REQ_OUTPUT_THROUGHPUT/NUM_TOTAL_TOKENS` 的新 dict。纯函数。
  - `summarize(results: List[Dict[str, Any]], wall_clock_s: float) -> Dict[str, Any]` — 聚合（剔除错误请求算 6 key 分位/均值 + 整体吞吐/错误率）。
  - `_safe_quantiles(data: List[float]) -> Dict[str, Optional[float]]`
  - `_safe_stdev(data: List[float]) -> Optional[float]`
  - `_safe_div(numerator: float, denominator: float) -> Optional[float]`

raw 单请求 dict 的键（Task 3 的 `_single_request` 产出，本任务测试构造）：
`TTFT`(float) / `E2E_LAT`(float) / `NUM_INPUT_TOKENS`(int) / `NUM_OUTPUT_TOKENS`(int) / `ERROR_CODE`(Optional[str]) / `ERROR_MSG`(str)。成功请求 `ERROR_CODE is None`。

- [x] **Step 1: 写失败测试 `tests/test_benchmark_metrics.py`**

```python
"""metrics.py 纯函数单测：换算正确性 + 全部除零/统计边界。无网络。"""

import json

from llmperf import common_metrics as cm
from llmperf.benchmark import metrics


def _raw(ttft, e2e, n_in, n_out, error_code=None, error_msg=""):
    return {
        cm.TTFT: ttft,
        cm.E2E_LAT: e2e,
        cm.NUM_INPUT_TOKENS: n_in,
        cm.NUM_OUTPUT_TOKENS: n_out,
        cm.ERROR_CODE: error_code,
        cm.ERROR_MSG: error_msg,
    }


# ---- derive_request_metrics 换算 ----

def test_itl_normal():
    d = metrics.derive_request_metrics(_raw(0.1, 1.1, 10, 5))
    assert d[cm.INTER_TOKEN_LAT] == (1.1 - 0.1) / (5 - 1)  # 0.25


def test_throughput_and_total():
    d = metrics.derive_request_metrics(_raw(0.1, 2.0, 10, 6))
    assert d[cm.REQ_OUTPUT_THROUGHPUT] == 6 / 2.0  # 3.0
    assert d[cm.NUM_TOTAL_TOKENS] == 16


def test_itl_zero_when_out_le_1():
    assert metrics.derive_request_metrics(_raw(0.1, 1.1, 10, 1))[cm.INTER_TOKEN_LAT] == 0
    assert metrics.derive_request_metrics(_raw(0.1, 1.1, 10, 0))[cm.INTER_TOKEN_LAT] == 0


def test_throughput_zero_when_e2e_zero():
    assert metrics.derive_request_metrics(_raw(0.0, 0.0, 10, 5))[cm.REQ_OUTPUT_THROUGHPUT] == 0


# ---- _safe_* 边界 ----

def test_safe_quantiles_empty_returns_none():
    q = metrics._safe_quantiles([])
    assert all(v is None for v in q.values())


def test_safe_quantiles_single_returns_value():
    q = metrics._safe_quantiles([0.5])
    assert q["p50"] == 0.5
    assert q["p99"] == 0.5


def test_safe_stdev_edges():
    assert metrics._safe_stdev([]) is None
    assert metrics._safe_stdev([0.5]) == 0
    assert metrics._safe_stdev([1.0, 3.0]) > 0


def test_safe_div_zero_denominator():
    assert metrics._safe_div(5, 0) is None
    assert metrics._safe_div(5, 2) == 2.5


# ---- summarize 聚合 + 整体边界 ----

def test_summarize_excludes_errored_from_quantiles():
    results = [
        _raw(0.1, 1.1, 10, 5),
        _raw(0.0, 0.0, 0, 0, error_code="500", error_msg="boom"),
    ]
    out = metrics.summarize(results, wall_clock_s=2.0)
    # 成功 1 条进分位数；错误条计入 error_rate。
    assert out[cm.TTFT]["mean"] == 0.1
    assert out[cm.NUM_ERRORS] == 1
    assert out[cm.ERROR_RATE] == 0.5
    assert out[cm.NUM_REQ_STARTED] == 2


def test_summarize_all_failed_returns_none_quantiles():
    results = [_raw(0.0, 0.0, 0, 0, error_code="500", error_msg="x")]
    out = metrics.summarize(results, wall_clock_s=2.0)
    assert out[cm.TTFT]["mean"] is None
    assert out[cm.ERROR_RATE] == 1.0


def test_summarize_zero_wall_clock_returns_none_throughput():
    out = metrics.summarize([_raw(0.1, 1.1, 10, 5)], wall_clock_s=0.0)
    assert out[cm.OUTPUT_THROUGHPUT] is None
    assert out[cm.COMPLETED_REQUESTS_PER_MIN] is None


def test_summarize_zero_started_returns_none_error_rate():
    out = metrics.summarize([], wall_clock_s=2.0)
    assert out[cm.ERROR_RATE] is None
    assert out[cm.NUM_REQ_STARTED] == 0


def test_summarize_error_code_keys_are_str_and_json_safe():
    # status_code 可能是 int/None，归一为 str，json.dumps 不抛 TypeError（spec I2）。
    results = [
        _raw(0.0, 0.0, 0, 0, error_code="500", error_msg="a"),
        _raw(0.0, 0.0, 0, 0, error_code="unknown", error_msg="b"),
    ]
    out = metrics.summarize(results, wall_clock_s=2.0)
    freq = out[cm.ERROR_CODE_FREQ]
    assert all(isinstance(k, str) for k in freq)
    json.dumps(out)  # 不抛异常
```

- [x] **Step 2: 跑测试确认失败**

Run: `python -m pytest tests/test_benchmark_metrics.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'llmperf.benchmark.metrics'`

- [x] **Step 3: 实现 `src/llmperf/benchmark/metrics.py`**

```python
"""聚合换算：单请求派生指标 + 跨请求分位数/吞吐/错误率。纯函数，无网络。

ITL 口径与原 Ray 版不同：(e2e-ttft)/(out-1)，见 spec §7 决策 2。
"""

import statistics
from typing import Any, Dict, List, Optional

from llmperf import common_metrics as cm

_QUANTILE_KEYS = [
    cm.INTER_TOKEN_LAT,
    cm.TTFT,
    cm.E2E_LAT,
    cm.REQ_OUTPUT_THROUGHPUT,
    cm.NUM_INPUT_TOKENS,
    cm.NUM_OUTPUT_TOKENS,
]
_PERCENTILES = [25, 50, 75, 90, 95, 99]


def _safe_div(numerator: float, denominator: float) -> Optional[float]:
    """统一除零口径：分母 <= 0 返回 None。"""
    if denominator <= 0:
        return None
    return numerator / denominator


def _safe_stdev(data: List[float]) -> Optional[float]:
    if len(data) == 0:
        return None
    if len(data) == 1:
        return 0
    return statistics.stdev(data)


def _safe_quantiles(data: List[float]) -> Dict[str, Optional[float]]:
    """n=0 全 None；n=1 唯一值；n>=2 用 statistics.quantiles 取百分位。"""
    if len(data) == 0:
        return {f"p{p}": None for p in _PERCENTILES}
    if len(data) == 1:
        return {f"p{p}": data[0] for p in _PERCENTILES}
    # quantiles(n=100) 返回 99 个切点，下标 p-1 即第 p 百分位。
    cuts = statistics.quantiles(data, n=100, method="inclusive")
    return {f"p{p}": cuts[p - 1] for p in _PERCENTILES}


def derive_request_metrics(raw: Dict[str, Any]) -> Dict[str, Any]:
    """补齐单请求派生指标：ITL / 吞吐 / total。纯函数，不改入参。"""
    out = dict(raw)
    ttft = raw[cm.TTFT]
    e2e = raw[cm.E2E_LAT]
    n_in = raw[cm.NUM_INPUT_TOKENS]
    n_out = raw[cm.NUM_OUTPUT_TOKENS]

    if n_out > 1:
        out[cm.INTER_TOKEN_LAT] = (e2e - ttft) / (n_out - 1)
    else:
        out[cm.INTER_TOKEN_LAT] = 0

    out[cm.REQ_OUTPUT_THROUGHPUT] = (n_out / e2e) if e2e > 0 else 0
    out[cm.NUM_TOTAL_TOKENS] = n_in + n_out
    return out


def summarize(results: List[Dict[str, Any]], wall_clock_s: float) -> Dict[str, Any]:
    """聚合所有请求 → 分位数/均值（仅成功）+ 整体吞吐/错误率。"""
    ret: Dict[str, Any] = {}
    num_started = len(results)

    successful = [
        derive_request_metrics(r) for r in results if r.get(cm.ERROR_CODE) is None
    ]

    for key in _QUANTILE_KEYS:
        data = [r[key] for r in successful]
        block: Dict[str, Any] = {"quantiles": _safe_quantiles(data)}
        block["mean"] = statistics.mean(data) if data else None
        block["min"] = min(data) if data else None
        block["max"] = max(data) if data else None
        block["stddev"] = _safe_stdev(data)
        ret[key] = block

    # 错误统计：error_code 已是 str（Task 3 归一），这里直接计频。
    error_codes = [r[cm.ERROR_CODE] for r in results if r.get(cm.ERROR_CODE) is not None]
    num_errors = len(error_codes)
    freq: Dict[str, int] = {}
    for code in error_codes:
        freq[code] = freq.get(code, 0) + 1

    ret[cm.NUM_REQ_STARTED] = num_started
    ret[cm.NUM_ERRORS] = num_errors
    ret[cm.ERROR_RATE] = _safe_div(num_errors, num_started)
    ret[cm.ERROR_CODE_FREQ] = freq

    total_output = sum(r[cm.NUM_OUTPUT_TOKENS] for r in successful)
    ret[cm.OUTPUT_THROUGHPUT] = _safe_div(total_output, wall_clock_s)
    ret[cm.NUM_COMPLETED_REQUESTS] = len(successful)
    per_min = _safe_div(len(successful), wall_clock_s)
    ret[cm.COMPLETED_REQUESTS_PER_MIN] = per_min * 60 if per_min is not None else None

    return ret
```

- [x] **Step 4: 跑测试确认通过**

Run: `python -m pytest tests/test_benchmark_metrics.py -v`
Expected: PASS（13 passed）

- [x] **Step 5: 格式化 + commit**

```bash
black src/llmperf/benchmark/metrics.py tests/test_benchmark_metrics.py
git add src/llmperf/benchmark/metrics.py tests/test_benchmark_metrics.py
git commit -m "feat(benchmark): add pure-function metrics aggregation with safe boundaries"
```

---

## Task 3: `runner.py` — asyncio worker-pool + 单请求流式采集

**Files:**
- Create: `src/llmperf/benchmark/runner.py`
- Test: `tests/test_benchmark_runner.py`

**Interfaces:**
- Consumes: `prompts.sample_prompt` / `prompts.sample_output_len`（Task 1）；`litellm.acompletion`（外部，mock）；`litellm.token_counter`（外部，mock）。
- Produces（Task 4 依赖）：
  - `@dataclass class BenchmarkConfig`，字段：`model: str`、`mean_input_tokens: int = 550`、`stddev_input_tokens: int = 150`、`mean_output_tokens: int = 150`、`stddev_output_tokens: int = 80`、`num_concurrent_requests: int = 10`、`max_num_completed_requests: int = 10`、`timeout: int = 90`、`additional_sampling_params: Dict[str, Any] = field(default_factory=dict)`
  - `async def run_benchmark(config: BenchmarkConfig) -> Tuple[List[Dict[str, Any]], float]` — 返回 `(results, wall_clock_s)`。
  - `async def _single_request(config: BenchmarkConfig, prompt_text: str, num_input_tokens: int, sampled_max_tokens: int) -> Dict[str, Any]`

- [x] **Step 1: 写失败测试 `tests/test_benchmark_runner.py`**

```python
"""runner.py 单测：mock acompletion 异步流式，用 asyncio.run 驱动（无 pytest-asyncio）。"""

import asyncio

from llmperf import common_metrics as cm
from llmperf.benchmark import runner


class _Delta(dict):
    """模拟 litellm chunk 的 delta：支持 .get 且 truthy。"""


def _chunk(content=None, reasoning=None):
    delta = _Delta()
    if content is not None:
        delta["content"] = content
    if reasoning is not None:
        delta["reasoning_content"] = reasoning

    class _Choice:
        def __init__(self, d):
            self.delta = d

    class _Chunk:
        def __init__(self, d):
            self.choices = [_Choice(d)]

    return _Chunk(delta)


def _make_acompletion(chunks, raise_exc=None):
    async def _fake(**body):
        if raise_exc is not None:
            raise raise_exc

        async def _gen():
            for c in chunks:
                yield c

        return _gen()

    return _fake


def _config(**kw):
    base = dict(model="gpt-4o", num_concurrent_requests=2, max_num_completed_requests=4, timeout=30)
    base.update(kw)
    return runner.BenchmarkConfig(**base)


def test_single_request_ttft_and_text(monkeypatch):
    chunks = [_chunk(content="Hello"), _chunk(content=" world")]
    monkeypatch.setattr(runner, "acompletion", _make_acompletion(chunks))
    monkeypatch.setattr(runner, "token_counter", lambda model, text: len(text.split()))

    m = asyncio.run(runner._single_request(_config(), "prompt", 3, 50))
    assert m[cm.ERROR_CODE] is None
    assert m[cm.TTFT] > 0
    assert m[cm.NUM_OUTPUT_TOKENS] == 2  # "Hello world" → 2 词
    assert m[cm.NUM_INPUT_TOKENS] == 3


def test_single_request_counts_reasoning_content(monkeypatch):
    # 推理模型把思维链放 reasoning_content，必须计入 generated_text（源码 78-81）。
    chunks = [_chunk(reasoning="think"), _chunk(content="answer")]
    monkeypatch.setattr(runner, "acompletion", _make_acompletion(chunks))
    captured = {}

    def _counter(model, text):
        captured["text"] = text
        return len(text.split())

    monkeypatch.setattr(runner, "token_counter", _counter)
    m = asyncio.run(runner._single_request(_config(), "p", 1, 50))
    assert captured["text"] == "thinkanswer"  # 两段拼接
    assert m[cm.NUM_OUTPUT_TOKENS] == 1


def test_single_request_error_extracts_from_exception(monkeypatch):
    # 修正源码 bug：error_code 从 e 真实提取，非 -1 常量（spec §13）。
    exc = RuntimeError("rate limited")
    exc.status_code = 429
    monkeypatch.setattr(runner, "acompletion", _make_acompletion([], raise_exc=exc))
    monkeypatch.setattr(runner, "token_counter", lambda model, text: 0)

    m = asyncio.run(runner._single_request(_config(), "p", 1, 50))
    assert m[cm.ERROR_CODE] == "429"
    assert m[cm.ERROR_MSG] == "rate limited"


def test_run_benchmark_completes_bounded(monkeypatch):
    chunks = [_chunk(content="a"), _chunk(content="b")]
    monkeypatch.setattr(runner, "acompletion", _make_acompletion(chunks))
    monkeypatch.setattr(runner, "token_counter", lambda model, text: len(text.split()))
    monkeypatch.setattr(runner, "sample_prompt", lambda *a, **k: ("prompt text", 2))
    monkeypatch.setattr(runner, "sample_output_len", lambda *a, **k: 50)

    results, wall = asyncio.run(runner.run_benchmark(_config(max_num_completed_requests=4)))
    assert len(results) <= 4  # 有界队列软上限（spec §4.3/Z1）
    assert len(results) == 4
    assert wall >= 0


def test_run_benchmark_concurrency_stays_at_n(monkeypatch):
    # worker-pool 验证：在途数稳定 == N，不出现批处理的 N→1→N 塌陷（spec §10）。
    state = {"inflight": 0, "peak": 0, "samples": []}

    def _make_slow():
        async def _fake(**body):
            state["inflight"] += 1
            state["peak"] = max(state["peak"], state["inflight"])
            await asyncio.sleep(0.01)

            async def _gen():
                state["samples"].append(state["inflight"])
                yield _chunk(content="x")
                state["inflight"] -= 1

            return _gen()

        return _fake

    monkeypatch.setattr(runner, "acompletion", _make_slow())
    monkeypatch.setattr(runner, "token_counter", lambda model, text: 1)
    monkeypatch.setattr(runner, "sample_prompt", lambda *a, **k: ("p", 1))
    monkeypatch.setattr(runner, "sample_output_len", lambda *a, **k: 10)

    asyncio.run(runner.run_benchmark(_config(num_concurrent_requests=3, max_num_completed_requests=9)))
    assert state["peak"] == 3  # 峰值 == N
    # 队列充足期间，至少多数采样保持满载 N（不长期塌陷）。
    assert max(state["samples"]) == 3
```

- [x] **Step 2: 跑测试确认失败**

Run: `python -m pytest tests/test_benchmark_runner.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'llmperf.benchmark.runner'`

- [x] **Step 3: 实现 `src/llmperf/benchmark/runner.py`**

```python
"""asyncio worker-pool 并发引擎 + 单请求流式采集。

并发模型见 spec §4.2（钉死 worker-pool，不用 gather 批处理）。
采集骨架照搬 litellm_client.py:61-103，仅 sync for → async for；
错误提取修正源码 89-94 的常量吞噬 bug。
"""

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from litellm import acompletion, token_counter

from llmperf import common_metrics as cm
from llmperf.benchmark.prompts import sample_output_len, sample_prompt


@dataclass
class BenchmarkConfig:
    model: str
    mean_input_tokens: int = 550
    stddev_input_tokens: int = 150
    mean_output_tokens: int = 150
    stddev_output_tokens: int = 80
    num_concurrent_requests: int = 10
    max_num_completed_requests: int = 10
    timeout: int = 90
    additional_sampling_params: Dict[str, Any] = field(default_factory=dict)


async def _single_request(
    config: BenchmarkConfig,
    prompt_text: str,
    num_input_tokens: int,
    sampled_max_tokens: int,
) -> Dict[str, Any]:
    """发一条流式请求，采集 TTFT/E2E/output。骨架照搬 litellm_client.py:61-103。"""
    messages = [
        {"role": "system", "content": ""},
        {"role": "user", "content": prompt_text},
    ]
    body: Dict[str, Any] = {
        "model": config.model,
        "messages": messages,
        "stream": True,
        "max_tokens": sampled_max_tokens,
        # HTTP 层兜底超时，防单请求 hang 架空整体 timeout（spec Y2）。
        "timeout": config.timeout,
    }
    body.update(config.additional_sampling_params or {})

    ttft = 0.0
    generated_text = ""
    error_code: Optional[str] = None
    error_msg = ""
    e2e = 0.0

    try:
        start = time.monotonic()
        response = await acompletion(**body)
        async for tok in response:
            if tok.choices[0].delta:
                delta = tok.choices[0].delta
                if delta.get("content", None) or delta.get("reasoning_content", None):
                    if ttft == 0:
                        ttft = time.monotonic() - start
                    if delta.get("reasoning_content", None):
                        generated_text += delta["reasoning_content"]
                    else:
                        generated_text += delta["content"]
        e2e = time.monotonic() - start
    except Exception as e:  # 修正源码吞噬 bug：从 e 真实提取（spec §13）。
        error_msg = str(e)
        error_code = str(getattr(e, "status_code", None) or "unknown")

    num_output_tokens = token_counter(model=config.model, text=generated_text)
    return {
        cm.TTFT: ttft,
        cm.E2E_LAT: e2e,
        cm.NUM_INPUT_TOKENS: num_input_tokens,
        cm.NUM_OUTPUT_TOKENS: num_output_tokens,
        cm.ERROR_CODE: error_code,
        cm.ERROR_MSG: error_msg,
    }


async def run_benchmark(
    config: BenchmarkConfig,
) -> Tuple[List[Dict[str, Any]], float]:
    """worker-pool 调度。有界队列预填恰好 max 条，N 个 worker 循环取任务。"""
    queue: asyncio.Queue = asyncio.Queue()
    for _ in range(config.max_num_completed_requests):
        text, n_in = sample_prompt(
            config.model,
            config.mean_input_tokens,
            config.stddev_input_tokens,
            config.mean_output_tokens,
        )
        max_tokens = sample_output_len(config.mean_output_tokens, config.stddev_output_tokens)
        await queue.put((text, n_in, max_tokens))

    results: List[Dict[str, Any]] = []
    started = 0
    start = time.monotonic()

    async def worker() -> None:
        nonlocal started
        while True:
            if time.monotonic() - start >= config.timeout:
                break  # 到点停止取新任务，不 cancel 在途请求（spec §4.3）。
            try:
                text, n_in, max_tokens = queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            started += 1
            m = await _single_request(config, text, n_in, max_tokens)
            results.append(m)

    await asyncio.gather(*[worker() for _ in range(config.num_concurrent_requests)])
    wall_clock_s = time.monotonic() - start
    return results, wall_clock_s
```

- [x] **Step 4: 跑测试确认通过**

Run: `python -m pytest tests/test_benchmark_runner.py -v`
Expected: PASS（5 passed）

- [x] **Step 5: 格式化 + commit**

```bash
black src/llmperf/benchmark/runner.py tests/test_benchmark_runner.py
git add src/llmperf/benchmark/runner.py tests/test_benchmark_runner.py
git commit -m "feat(benchmark): add asyncio worker-pool runner with streaming collection"
```

---

## Task 4: `__main__.py` + `__init__.py` — CLI 与公共入口

**Files:**
- Modify: `src/llmperf/benchmark/__init__.py`（填导出）
- Create: `src/llmperf/benchmark/__main__.py`
- Test: `tests/test_benchmark_cli.py`

**Interfaces:**
- Consumes: `runner.BenchmarkConfig` / `runner.run_benchmark`（Task 3）；`metrics.summarize`（Task 2）。
- Produces：
  - `parse_metadata(raw: str) -> Dict[str, str]`
  - `clean_filename(model: str, mean_input: int, mean_output: int) -> str`
  - `main(argv: Optional[List[str]] = None) -> None`

- [x] **Step 1: 写失败测试 `tests/test_benchmark_cli.py`**

```python
"""CLI 解析单测：metadata/JSON 边界 + 文件名清洗。无网络。"""

import json

import pytest

from llmperf.benchmark import __main__ as cli


def test_parse_metadata_splits_on_first_equals():
    # val 可含 '='；重复 key 后者覆盖（spec I3）。
    assert cli.parse_metadata("a=1,b=x=y,a=2") == {"a": "2", "b": "x=y"}


def test_parse_metadata_empty():
    assert cli.parse_metadata("") == {}


def test_clean_filename_replaces_special_chars():
    # 照搬 token_benchmark_ray.py:339-341 两步正则。
    assert cli.clean_filename("openai/gpt-4o", 550, 150) == "openai-gpt-4o_550_150"


def test_additional_sampling_params_invalid_json_exits():
    # type=json.loads 让 argparse 对非法 JSON 直接报错退出（spec I4）。
    with pytest.raises(SystemExit):
        cli.main(["--model", "gpt-4o", "--additional-sampling-params", "{not json}"])
```

- [x] **Step 2: 跑测试确认失败**

Run: `python -m pytest tests/test_benchmark_cli.py -v`
Expected: FAIL — `ImportError`（`parse_metadata` 等未定义）

- [x] **Step 3: 实现 `src/llmperf/benchmark/__init__.py`**

```python
"""llmperf 内置 asyncio 压测模块。CLI: python -m llmperf.benchmark"""

from llmperf.benchmark.metrics import summarize
from llmperf.benchmark.runner import BenchmarkConfig, run_benchmark

__all__ = ["run_benchmark", "summarize", "BenchmarkConfig"]
```

- [x] **Step 4: 实现 `src/llmperf/benchmark/__main__.py`**

```python
"""argparse CLI → 组装 config → asyncio.run(run_benchmark) → summarize → stdout/落盘。"""

import argparse
import asyncio
import json
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import litellm

from llmperf.benchmark.metrics import summarize
from llmperf.benchmark.runner import BenchmarkConfig, run_benchmark


def parse_metadata(raw: str) -> Dict[str, str]:
    """'a=1,b=x=y' → {'a':'1','b':'x=y'}。按首个 '=' 分割，重复 key 后覆盖。"""
    out: Dict[str, str] = {}
    if not raw:
        return out
    for item in raw.split(","):
        key, value = item.split("=", 1)
        out[key] = value
    return out


def clean_filename(model: str, mean_input: int, mean_output: int) -> str:
    """照搬 token_benchmark_ray.py:339-341 的两步清洗。"""
    filename = f"{model}_{mean_input}_{mean_output}"
    filename = re.sub(r"[^\w\d-]+", "-", filename)
    filename = re.sub(r"-{2,}", "-", filename)
    return filename


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m llmperf.benchmark",
        description="Async SDK load test for litellm.acompletion (TTFT/ITL/throughput).",
    )
    p.add_argument("--model", type=str, required=True)
    p.add_argument("--mean-input-tokens", type=int, default=550)
    p.add_argument("--stddev-input-tokens", type=int, default=150)
    p.add_argument("--mean-output-tokens", type=int, default=150)
    p.add_argument("--stddev-output-tokens", type=int, default=80)
    p.add_argument("--num-concurrent-requests", type=int, default=10)
    p.add_argument("--max-num-completed-requests", type=int, default=10)
    p.add_argument("--timeout", type=int, default=90)
    # 非法 JSON 由 argparse 直接报错退出（spec I4）。
    p.add_argument("--additional-sampling-params", type=json.loads, default={})
    p.add_argument("--results-dir", type=str, default="")
    p.add_argument("--metadata", type=str, default="")
    return p


def main(argv: Optional[List[str]] = None) -> None:
    args = _build_parser().parse_args(argv)
    user_metadata = parse_metadata(args.metadata)

    config = BenchmarkConfig(
        model=args.model,
        mean_input_tokens=args.mean_input_tokens,
        stddev_input_tokens=args.stddev_input_tokens,
        mean_output_tokens=args.mean_output_tokens,
        stddev_output_tokens=args.stddev_output_tokens,
        num_concurrent_requests=args.num_concurrent_requests,
        max_num_completed_requests=args.max_num_completed_requests,
        timeout=args.timeout,
        additional_sampling_params=args.additional_sampling_params,
    )

    results, wall_clock_s = asyncio.run(run_benchmark(config))
    summary = summarize(results, wall_clock_s)
    summary["model"] = args.model
    summary["num_concurrent_requests"] = args.num_concurrent_requests
    summary["timestamp"] = int(time.time())
    summary["litellm_version"] = getattr(litellm, "__version__", "unknown")
    summary.update(user_metadata)

    print(json.dumps(summary, indent=4, default=str))

    if args.results_dir:
        results_dir = Path(args.results_dir)
        if not results_dir.exists():
            results_dir.mkdir(parents=True)
        base = clean_filename(args.model, args.mean_input_tokens, args.mean_output_tokens)
        with open(results_dir / f"{base}_summary.json", "w") as f:
            json.dump(summary, f, indent=4, default=str)
        with open(results_dir / f"{base}_individual_responses.json", "w") as f:
            json.dump(results, f, indent=4, default=str)


if __name__ == "__main__":
    main()
```

- [x] **Step 5: 跑测试确认通过**

Run: `python -m pytest tests/test_benchmark_cli.py -v`
Expected: PASS（4 passed）

- [x] **Step 6: CLI 冒烟（--help 不需网络）**

Run: `python -m llmperf.benchmark --help`
Expected: 打印 usage，含所有参数，退出码 0。

- [x] **Step 7: 格式化 + commit**

```bash
black src/llmperf/benchmark/__init__.py src/llmperf/benchmark/__main__.py tests/test_benchmark_cli.py
git add src/llmperf/benchmark/__init__.py src/llmperf/benchmark/__main__.py tests/test_benchmark_cli.py
git commit -m "feat(benchmark): add CLI entrypoint and public exports"
```

---

## Task 5: 文档 + 全量门禁收尾

**Files:**
- Create: `docs/benchmark.md`（llmperf 仓库内）
- Modify: `README.md`（加一节指向新模块，可选）

**Interfaces:** 无（收尾任务）。

- [x] **Step 1: 写 `docs/benchmark.md`**

```markdown
# Async SDK Benchmark (`llmperf.benchmark`)

轻量 asyncio 压测入口，直接压 `litellm.acompletion()`，无需 Ray。

## 用法

```bash
python -m llmperf.benchmark \
  --model gpt-4o \
  --mean-input-tokens 550 --stddev-input-tokens 150 \
  --mean-output-tokens 150 --stddev-output-tokens 80 \
  --num-concurrent-requests 10 \
  --max-num-completed-requests 100 \
  --timeout 600 \
  --results-dir ./results \
  --metadata "env=prod,version=1.0"
```

## 与原 Ray 版（`token_benchmark_ray.py`）的差异

- **并发引擎**：asyncio worker-pool 替代 Ray + threading，零重依赖。
- **分词器**：`litellm.token_counter`（按 model 精确计）替代固定 Llama 分词器。
- **⚠️ ITL 口径不同**：本模块 `ITL = (e2e − ttft) / (output_tokens − 1)`，语义纯净、与 output token 口径一致；原 Ray 版是 `sum(time_to_next_token) / chunk数`（首项含 TTFT、分母用 chunk 数）。**两者数值不可直接对比。**
```

- [x] **Step 2: 全量格式化**

Run: `black src/llmperf/benchmark/ tests/test_benchmark_*.py`
Expected: `All done` / 已格式化无 diff。

- [x] **Step 3: 全量测试绿**

Run: `python -m pytest tests/test_benchmark_prompts.py tests/test_benchmark_metrics.py tests/test_benchmark_runner.py tests/test_benchmark_cli.py -v`
Expected: PASS（全部，26 passed）

- [x] **Step 4: 确认未触碰旧代码**

Run: `git diff --name-only HEAD~5 -- token_benchmark_ray.py src/llmperf/ray_clients/ src/llmperf/utils.py`
Expected: 空输出（旧 Ray 机器零改动，验证并存无侵入）。

- [x] **Step 5: 文档 commit**

```bash
git add docs/benchmark.md README.md
git commit -m "docs(benchmark): add usage page noting ITL semantics differ from Ray version"
```

---

## 自审（spec 覆盖核对）

| spec 章节 / 要求 | 落点 |
|------|------|
| §3 模块结构（prompts/metrics/runner/__main__/__init__/sonnet.txt） | Task 1-4 全覆盖 |
| §4.2 worker-pool 不用 gather 批处理 | Task 3 Step 3 `run_benchmark` + Step 1 并发不塌陷测试 |
| §4.3 timeout 停发不 cancel / 有界队列软上限 | Task 3 worker `break` + `test_run_benchmark_completes_bounded` |
| §5.2 ITL/吞吐/total 换算 | Task 2 `derive_request_metrics` + 测试 |
| §5.3 statistics 边界 + 整体除零 | Task 2 `_safe_quantiles/_safe_stdev/_safe_div` + 边界测试 |
| §7 决策 1 token_counter | Task 1 `sample_prompt` |
| §7 决策 2 ITL 修正 + 文档标注 | Task 2 + Task 5 docs |
| §8 CLI 参数（含 metadata/JSON 解析） | Task 4 `_build_parser` + 测试 |
| §9 落盘双文件 + 文件名清洗 | Task 4 `main` + `clean_filename` |
| §13 错误吞噬修正（从 e 提取） | Task 3 `_single_request` except + `test_..._error_extracts` |
| §13 I2 error_code 转 str / JSON 安全 | Task 3 归一 + Task 2 `test_..._json_safe` |
| Y2 防 hang（HTTP timeout 透传） | Task 3 body `"timeout"` |
| Y4/Y5 队列元素 (text, n_in, max_tokens) | Task 3 `run_benchmark` 入队 |

**未自动化、留实现期人工验证**（spec 已声明不猜）：
- litellm 0.1.738 的 `acompletion` 真实异步流式行为（Task 4 Step 6 仅冒烟 --help；端到端真实压测需有效 API key，超出单测范围）。
- `token_counter` 对极端 model 名的兼容性（非阻塞）。
