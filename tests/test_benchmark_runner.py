"""runner.py 单测：mock acompletion 异步流式，用 asyncio.run 驱动（无 pytest-asyncio）。"""

import asyncio
import time

import pytest

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


def _empty_choices_chunk():
    """模拟 include_usage 末帧：choices=[]，仅带 usage。"""

    class _Chunk:
        def __init__(self):
            self.choices = []

    return _Chunk()


def test_single_request_survives_usage_only_final_frame(monkeypatch):
    # H3：开 stream_options.include_usage 时，流末尾会单独发一个 choices=[] 的 usage 帧。
    # 该帧不得触发 IndexError 把已成功的请求整条误判为失败。
    chunks = [_chunk(content="Hello"), _chunk(content=" world"), _empty_choices_chunk()]
    monkeypatch.setattr(runner, "acompletion", _make_acompletion(chunks))
    monkeypatch.setattr(runner, "token_counter", lambda model, text: len(text.split()))

    m = asyncio.run(runner._single_request(_config(), "p", 3, 50))
    assert m[cm.ERROR_CODE] is None  # 成功，而非 "unknown"
    assert m[cm.NUM_OUTPUT_TOKENS] == 2  # "Hello world" 两段保留


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


# ---- 开环速率模式（spec 2026-07-20 §3/§6 R1-R7） ----


def test_arrival_offsets_poisson_schedule(monkeypatch):
    # R1：首到达为 0；间隔逐个来自 expovariate(rate)；horizon 截断。
    calls = []

    def _fake_expovariate(rate):
        calls.append(rate)
        return 2.0

    monkeypatch.setattr(runner.random, "expovariate", _fake_expovariate)
    offsets = runner._arrival_offsets(rate=1.5, count=10, horizon=5.0)
    assert offsets == [0.0, 2.0, 4.0]  # 6.0 越过 horizon=5.0 被截断
    assert all(c == 1.5 for c in calls)  # 间隔采样传入的是 rate 本身


def test_arrival_offsets_count_truncation(monkeypatch):
    # R1：horizon 充裕时恰好生成 count 个。
    monkeypatch.setattr(runner.random, "expovariate", lambda rate: 1.0)
    assert runner._arrival_offsets(rate=1.0, count=3, horizon=1000.0) == [
        0.0,
        1.0,
        2.0,
    ]


def test_open_loop_completes_and_records_sched_delay(monkeypatch):
    # R2：expovariate→0 全部即刻到达；全部完成且每条记录带 SCHED_DELAY。
    monkeypatch.setattr(runner, "acompletion", _make_acompletion([_chunk(content="x")]))
    monkeypatch.setattr(runner, "token_counter", lambda model, text: 1)
    monkeypatch.setattr(runner, "sample_prompt", lambda *a, **k: ("p", 1))
    monkeypatch.setattr(runner, "sample_output_len", lambda *a, **k: 10)
    monkeypatch.setattr(runner.random, "expovariate", lambda rate: 0.0)

    results, wall = asyncio.run(
        runner.run_benchmark(_config(request_rate=100.0, max_num_completed_requests=4))
    )
    assert len(results) == 4
    assert all(cm.SCHED_DELAY in r for r in results)
    assert all(r[cm.SCHED_DELAY] >= 0 for r in results)


def test_open_loop_semaphore_caps_inflight(monkeypatch):
    # R3：高速率 + 慢请求下，在途数被 num_concurrent_requests 封顶，
    # 被顶住的请求产生正的调度延迟。
    state = {"inflight": 0, "peak": 0}

    async def _slow(**body):
        state["inflight"] += 1
        state["peak"] = max(state["peak"], state["inflight"])
        await asyncio.sleep(0.02)

        async def _gen():
            yield _chunk(content="x")
            state["inflight"] -= 1

        return _gen()

    monkeypatch.setattr(runner, "acompletion", _slow)
    monkeypatch.setattr(runner, "token_counter", lambda model, text: 1)
    monkeypatch.setattr(runner, "sample_prompt", lambda *a, **k: ("p", 1))
    monkeypatch.setattr(runner, "sample_output_len", lambda *a, **k: 10)
    monkeypatch.setattr(runner.random, "expovariate", lambda rate: 0.0)

    results, _ = asyncio.run(
        runner.run_benchmark(
            _config(
                request_rate=1000.0,
                num_concurrent_requests=2,
                max_num_completed_requests=6,
            )
        )
    )
    assert state["peak"] == 2  # 信号量封顶
    assert len(results) == 6
    # 全部同时到达但仅 2 个名额：后到手的必然排过队。
    assert max(r[cm.SCHED_DELAY] for r in results) > 0


def test_dispatch_at_skips_when_budget_exhausted(monkeypatch):
    # R4：抢到名额时预算已尽 → 不触碰 acompletion、不追加结果（到点停发）。
    def _boom(**body):
        raise AssertionError("预算耗尽后不得发射请求")

    monkeypatch.setattr(runner, "acompletion", _boom)
    results = []
    sem = asyncio.Semaphore(1)
    config = _config(timeout=30, request_rate=1.0)

    asyncio.run(
        runner._dispatch_at(
            config, "p", 1, 10, 0.0, time.monotonic() - 100.0, sem, results
        )
    )
    assert results == []


def test_open_loop_passes_remaining_budget(monkeypatch):
    # R5：开环透传的单请求 timeout 仍是剩余预算（B2 语义保持）。
    captured = {}
    monkeypatch.setattr(
        runner,
        "acompletion",
        _make_capturing_acompletion(captured, [_chunk(content="x")]),
    )
    monkeypatch.setattr(runner, "token_counter", lambda model, text: 1)
    monkeypatch.setattr(runner, "sample_prompt", lambda *a, **k: ("p", 1))
    monkeypatch.setattr(runner, "sample_output_len", lambda *a, **k: 10)
    monkeypatch.setattr(runner.random, "expovariate", lambda rate: 0.0)

    asyncio.run(
        runner.run_benchmark(
            _config(timeout=30, request_rate=10.0, max_num_completed_requests=1)
        )
    )
    assert 0 < captured["timeout"] <= 30


def test_closed_loop_results_have_no_sched_delay(monkeypatch):
    # R6：rate=None 走闭环，结果不含 SCHED_DELAY 键（输出零侵入）。
    monkeypatch.setattr(runner, "acompletion", _make_acompletion([_chunk(content="x")]))
    monkeypatch.setattr(runner, "token_counter", lambda model, text: 1)
    monkeypatch.setattr(runner, "sample_prompt", lambda *a, **k: ("p", 1))
    monkeypatch.setattr(runner, "sample_output_len", lambda *a, **k: 10)

    results, _ = asyncio.run(
        runner.run_benchmark(_config(max_num_completed_requests=2))
    )
    assert all(cm.SCHED_DELAY not in r for r in results)


def test_run_benchmark_rejects_non_positive_rate():
    # R7：库直调路径（绕过 CLI）传非法速率必须立刻抛 ValueError。
    with pytest.raises(ValueError):
        asyncio.run(runner.run_benchmark(_config(request_rate=0.0)))
    with pytest.raises(ValueError):
        asyncio.run(runner.run_benchmark(_config(request_rate=-1.5)))


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


# ---- HTTP 直连模式（spec 2026-07-22 §3/§6 T3/T8/T9/T10） ----


def test_build_body_matches_litellm_shape():
    # T3：_build_body 是两条后端共用的唯一请求体来源，字段须与现状一致。
    body = runner._build_body(
        _config(additional_sampling_params={"temperature": 0.5}), "p", 42
    )
    assert body["model"] == "gpt-4o"
    assert body["messages"] == [
        {"role": "system", "content": ""},
        {"role": "user", "content": "p"},
    ]
    assert body["stream"] is True
    assert body["max_tokens"] == 42
    assert body["temperature"] == 0.5
    assert "timeout" not in body  # timeout 由各自路径单独附加，不进 JSON body


def test_single_request_dispatches_to_http_when_api_base_set(monkeypatch):
    # T8：api_base 设置时不得触碰 acompletion，须转发给 http_client。
    def _boom_acompletion(**body):
        raise AssertionError("api_base 设置时不应调用 litellm.acompletion")

    monkeypatch.setattr(runner, "acompletion", _boom_acompletion)

    captured = {}

    def _fake_sync(model, api_base, api_key_env, num_input_tokens, timeout, body):
        captured.update(
            model=model,
            api_base=api_base,
            api_key_env=api_key_env,
            num_input_tokens=num_input_tokens,
            timeout=timeout,
        )
        return {
            cm.TTFT: 0.05,
            cm.E2E_LAT: 0.2,
            cm.NUM_INPUT_TOKENS: num_input_tokens,
            cm.NUM_OUTPUT_TOKENS: 3,
            cm.ERROR_CODE: None,
            cm.ERROR_MSG: "",
        }

    monkeypatch.setattr(runner.http_client, "single_request_sync", _fake_sync)

    config = _config(api_base="http://localhost:8000/v1", api_key_env="MY_KEY")
    m = asyncio.run(runner._single_request(config, "p", 7, 50))
    assert m[cm.ERROR_CODE] is None
    assert m[cm.NUM_OUTPUT_TOKENS] == 3
    assert captured["api_base"] == "http://localhost:8000/v1"
    assert captured["api_key_env"] == "MY_KEY"
    assert captured["num_input_tokens"] == 7


def test_run_benchmark_closed_loop_http_backend(monkeypatch):
    # T9：闭环端到端配合假 HTTP 后端，管线（queue/worker/结果收集）零改动可用。
    def _fake_sync(model, api_base, api_key_env, num_input_tokens, timeout, body):
        return {
            cm.TTFT: 0.01,
            cm.E2E_LAT: 0.02,
            cm.NUM_INPUT_TOKENS: num_input_tokens,
            cm.NUM_OUTPUT_TOKENS: 2,
            cm.ERROR_CODE: None,
            cm.ERROR_MSG: "",
        }

    monkeypatch.setattr(runner.http_client, "single_request_sync", _fake_sync)
    monkeypatch.setattr(runner, "sample_prompt", lambda *a, **k: ("p", 1))
    monkeypatch.setattr(runner, "sample_output_len", lambda *a, **k: 10)

    config = _config(api_base="http://localhost:8000/v1", max_num_completed_requests=4)
    results, wall = asyncio.run(runner.run_benchmark(config))
    assert len(results) == 4
    assert all(r[cm.ERROR_CODE] is None for r in results)


def test_run_benchmark_open_loop_http_backend(monkeypatch):
    # T9：开环端到端配合假 HTTP 后端。
    def _fake_sync(model, api_base, api_key_env, num_input_tokens, timeout, body):
        return {
            cm.TTFT: 0.01,
            cm.E2E_LAT: 0.02,
            cm.NUM_INPUT_TOKENS: num_input_tokens,
            cm.NUM_OUTPUT_TOKENS: 2,
            cm.ERROR_CODE: None,
            cm.ERROR_MSG: "",
        }

    monkeypatch.setattr(runner.http_client, "single_request_sync", _fake_sync)
    monkeypatch.setattr(runner, "sample_prompt", lambda *a, **k: ("p", 1))
    monkeypatch.setattr(runner, "sample_output_len", lambda *a, **k: 10)
    monkeypatch.setattr(runner.random, "expovariate", lambda rate: 0.0)

    config = _config(
        api_base="http://localhost:8000/v1",
        request_rate=100.0,
        max_num_completed_requests=4,
    )
    results, wall = asyncio.run(runner.run_benchmark(config))
    assert len(results) == 4
    assert all(cm.SCHED_DELAY in r for r in results)


def test_run_benchmark_http_uses_dedicated_executor_sized_to_n(monkeypatch):
    # T10：专属线程池尺寸对齐 num_concurrent_requests，峰值并发 == N，
    # 不被 asyncio 默认 executor（容量 32）隐性封顶。
    state = {"inflight": 0, "peak": 0}
    lock = __import__("threading").Lock()

    def _slow_sync(model, api_base, api_key_env, num_input_tokens, timeout, body):
        with lock:
            state["inflight"] += 1
            state["peak"] = max(state["peak"], state["inflight"])
        time.sleep(0.05)
        with lock:
            state["inflight"] -= 1
        return {
            cm.TTFT: 0.01,
            cm.E2E_LAT: 0.05,
            cm.NUM_INPUT_TOKENS: num_input_tokens,
            cm.NUM_OUTPUT_TOKENS: 1,
            cm.ERROR_CODE: None,
            cm.ERROR_MSG: "",
        }

    monkeypatch.setattr(runner.http_client, "single_request_sync", _slow_sync)
    monkeypatch.setattr(runner, "sample_prompt", lambda *a, **k: ("p", 1))
    monkeypatch.setattr(runner, "sample_output_len", lambda *a, **k: 10)

    config = _config(
        api_base="http://localhost:8000/v1",
        num_concurrent_requests=6,
        max_num_completed_requests=6,
        timeout=30,
    )
    results, wall = asyncio.run(runner.run_benchmark(config))
    assert len(results) == 6
    assert state["peak"] == 6


# ---- Warmup 请求（spec 2026-07-25 §6 T1-T6） ----


def _make_counting_single_request(calls, warmup_sleep=0.0, warmup_n=0):
    """替身 _single_request：记录每次调用入口时刻；前 warmup_n 次可选 sleep。"""

    async def _fake(
        config,
        prompt_text,
        num_input_tokens,
        sampled_max_tokens,
        request_timeout=None,
        executor=None,
    ):
        idx = len(calls)
        calls.append(time.monotonic())
        if idx < warmup_n and warmup_sleep > 0:
            await asyncio.sleep(warmup_sleep)
        return {
            cm.TTFT: 0.01,
            cm.E2E_LAT: 0.02,
            cm.NUM_INPUT_TOKENS: num_input_tokens,
            cm.NUM_OUTPUT_TOKENS: 2,
            cm.ERROR_CODE: None,
            cm.ERROR_MSG: "",
        }

    return _fake


def _patch_sampling(monkeypatch):
    monkeypatch.setattr(runner, "sample_prompt", lambda *a, **k: ("p", 1))
    monkeypatch.setattr(runner, "sample_output_len", lambda *a, **k: 10)


def test_warmup_zero_equivalent_to_current(monkeypatch):
    # T1：num_warmup_requests=0（默认）→ _single_request 恰调 max 次，results == max。
    calls = []
    monkeypatch.setattr(runner, "_single_request", _make_counting_single_request(calls))
    _patch_sampling(monkeypatch)

    results, _ = asyncio.run(
        runner.run_benchmark(
            _config(max_num_completed_requests=5, num_warmup_requests=0)
        )
    )
    assert len(calls) == 5
    assert len(results) == 5


def test_warmup_requests_discarded(monkeypatch):
    # T2：warmup=3, max=5 → _single_request 共 8 次；返回 results 恰 5 条（warmup 丢弃）。
    calls = []
    monkeypatch.setattr(runner, "_single_request", _make_counting_single_request(calls))
    _patch_sampling(monkeypatch)

    results, _ = asyncio.run(
        runner.run_benchmark(
            _config(max_num_completed_requests=5, num_warmup_requests=3)
        )
    )
    assert len(calls) == 8  # 3 warmup + 5 measured
    assert len(results) == 5  # warmup 结果不进 results


def test_warmup_excluded_from_wall_clock(monkeypatch):
    # T3：warmup 请求耗时、测量相极快 → 返回的 wall_clock_s 不含 warmup 耗时。
    calls = []
    monkeypatch.setattr(
        runner,
        "_single_request",
        _make_counting_single_request(calls, warmup_sleep=0.1, warmup_n=2),
    )
    _patch_sampling(monkeypatch)

    t0 = time.monotonic()
    results, wall = asyncio.run(
        runner.run_benchmark(
            _config(
                num_concurrent_requests=2,
                max_num_completed_requests=3,
                num_warmup_requests=2,
            )
        )
    )
    total = time.monotonic() - t0
    assert len(results) == 3
    assert total >= 0.1  # warmup 确实发生并耗时
    assert wall < total - 0.05  # 返回的 wall 明显小于含 warmup 的总耗时


def test_warmup_precedes_measurement(monkeypatch):
    # T4：所有 warmup 请求（前 N 次）都早于测量相首个请求。
    calls = []
    monkeypatch.setattr(
        runner,
        "_single_request",
        _make_counting_single_request(calls, warmup_sleep=0.05, warmup_n=3),
    )
    _patch_sampling(monkeypatch)

    asyncio.run(
        runner.run_benchmark(
            _config(
                num_concurrent_requests=3,
                max_num_completed_requests=4,
                num_warmup_requests=3,
            )
        )
    )
    assert len(calls) == 7
    warmup_times = calls[:3]
    measured_times = calls[3:]
    assert max(warmup_times) < min(measured_times)


def test_warmup_closed_loop_under_open_loop(monkeypatch):
    # T5：request_rate 设置时 warmup 仍闭环发 N 条；_arrival_offsets 的 count == max（不含 N）。
    calls = []
    monkeypatch.setattr(runner, "_single_request", _make_counting_single_request(calls))
    _patch_sampling(monkeypatch)
    monkeypatch.setattr(runner.random, "expovariate", lambda rate: 0.0)

    captured = {}
    orig = runner._arrival_offsets

    def _spy(rate, count, horizon):
        captured["count"] = count
        return orig(rate, count, horizon)

    monkeypatch.setattr(runner, "_arrival_offsets", _spy)

    results, _ = asyncio.run(
        runner.run_benchmark(
            _config(
                request_rate=100.0,
                max_num_completed_requests=4,
                num_warmup_requests=2,
            )
        )
    )
    assert len(calls) == 6  # 2 warmup + 4 measured
    assert len(results) == 4
    assert captured["count"] == 4  # 泊松到达只排 max 个，warmup 不掺入
    assert all(cm.SCHED_DELAY in r for r in results)


def test_warmup_uses_http_backend_when_api_base_set(monkeypatch):
    # T6：api_base 设置时 warmup 与测量都走 http_client.single_request_sync，
    # acompletion 全程未被触及。
    def _boom_acompletion(**body):
        raise AssertionError("api_base 设置时不应调用 acompletion")

    monkeypatch.setattr(runner, "acompletion", _boom_acompletion)

    count = {"n": 0}

    def _fake_sync(model, api_base, api_key_env, num_input_tokens, timeout, body):
        count["n"] += 1
        return {
            cm.TTFT: 0.01,
            cm.E2E_LAT: 0.02,
            cm.NUM_INPUT_TOKENS: num_input_tokens,
            cm.NUM_OUTPUT_TOKENS: 2,
            cm.ERROR_CODE: None,
            cm.ERROR_MSG: "",
        }

    monkeypatch.setattr(runner.http_client, "single_request_sync", _fake_sync)
    _patch_sampling(monkeypatch)

    config = _config(
        api_base="http://localhost:8000/v1",
        max_num_completed_requests=4,
        num_warmup_requests=2,
    )
    results, _ = asyncio.run(runner.run_benchmark(config))
    assert count["n"] == 6  # 2 warmup + 4 measured 都走 HTTP 后端
    assert len(results) == 4
