"""runner.py 单测：mock acompletion 异步流式，用 asyncio.run 驱动（无 pytest-asyncio）。"""

import asyncio

from llmperf import common_metrics as cm
from llmperf.benchmark import runner


class _Delta(dict):
    """模拟 litellm chunk 的 delta：支持 .get 且 truthy。"""


class _ObjDelta:
    """模拟 litellm 新版 pydantic delta：只有属性，无 .get。"""

    def __init__(self, content=None, reasoning_content=None):
        self.content = content
        self.reasoning_content = reasoning_content


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
    base = dict(
        model="gpt-4o",
        num_concurrent_requests=2,
        max_num_completed_requests=4,
        timeout=30,
    )
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


def test_single_request_non_dict_sampling_params_no_escape(monkeypatch):
    # 库直调路径（绕过 CLI 校验）：additional_sampling_params 非 dict 时，
    # body.update 会抛 TypeError；A2 要求它被就地转成失败记录，绝不逃逸出
    # _single_request 把整个 worker 池带崩（评审 finding #1 的运行时兜底）。
    def _boom(**body):
        raise AssertionError("acompletion 不应被触及——应在 body 组装期就失败")

    monkeypatch.setattr(runner, "acompletion", _boom)
    monkeypatch.setattr(runner, "token_counter", lambda model, text: 0)

    m = asyncio.run(
        runner._single_request(_config(additional_sampling_params=[1, 2]), "p", 1, 50)
    )
    assert m[cm.ERROR_CODE] is not None  # 转成失败记录，而非抛出
    assert m[cm.ERROR_MSG] != ""


def test_run_benchmark_completes_bounded(monkeypatch):
    chunks = [_chunk(content="a"), _chunk(content="b")]
    monkeypatch.setattr(runner, "acompletion", _make_acompletion(chunks))
    monkeypatch.setattr(runner, "token_counter", lambda model, text: len(text.split()))
    monkeypatch.setattr(runner, "sample_prompt", lambda *a, **k: ("prompt text", 2))
    monkeypatch.setattr(runner, "sample_output_len", lambda *a, **k: 50)

    results, wall = asyncio.run(
        runner.run_benchmark(_config(max_num_completed_requests=4))
    )
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

    asyncio.run(
        runner.run_benchmark(
            _config(num_concurrent_requests=3, max_num_completed_requests=9)
        )
    )
    assert state["peak"] == 3  # 峰值 == N
    # 队列充足期间，至少多数采样保持满载 N（不长期塌陷）。
    assert max(state["samples"]) == 3


def test_extract_content_dict():
    assert runner.extract_content({"content": "x"}) == "x"


def test_extract_content_dict_reasoning_priority():
    assert runner.extract_content({"reasoning_content": "r", "content": "x"}) == "r"


def test_extract_content_object_getattr_only():
    # pydantic-like：无 .get，只能 getattr。
    assert runner.extract_content(_ObjDelta(content="x")) == "x"


def test_extract_content_empty_returns_none():
    assert runner.extract_content({}) is None
    assert runner.extract_content(_ObjDelta()) is None


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


def test_single_request_empty_stream_marked_failed(monkeypatch):
    # 流正常但零 content：判为 empty_response，不得当成功。
    monkeypatch.setattr(runner, "acompletion", _make_acompletion([]))
    monkeypatch.setattr(runner, "token_counter", lambda model, text: 0)

    m = asyncio.run(runner._single_request(_config(), "p", 1, 50))
    assert m[cm.ERROR_CODE] == "empty_response"
    assert m[cm.NUM_OUTPUT_TOKENS] == 0


def test_single_request_status_code_zero_not_swallowed(monkeypatch):
    exc = RuntimeError("boom")
    exc.status_code = 0  # falsy 但合法
    monkeypatch.setattr(runner, "acompletion", _make_acompletion([], raise_exc=exc))
    monkeypatch.setattr(runner, "token_counter", lambda model, text: 0)

    m = asyncio.run(runner._single_request(_config(), "p", 1, 50))
    assert m[cm.ERROR_CODE] == "0"


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

    asyncio.run(runner._single_request(_config(timeout=30), "p", 1, 50, 12.5))
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


def test_worker_derives_shrinking_budget(monkeypatch):
    # 推进 time.monotonic：每次调用 +1s，使 worker 计算出的 elapsed > 0，
    # 从而透传的单请求 timeout 严格小于全局 timeout（证明确实按剩余预算派生，
    # 而非无条件透传 config.timeout）。
    ticks = {"t": 0.0}

    def _fake_monotonic():
        ticks["t"] += 1.0
        return ticks["t"]

    captured = {}
    monkeypatch.setattr(runner.time, "monotonic", _fake_monotonic)
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
    # elapsed 已 > 0，故透传 timeout 必严格小于全局 30，且仍为正。
    assert 0 < captured["timeout"] < 30
