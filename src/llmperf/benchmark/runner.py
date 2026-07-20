"""asyncio worker-pool 并发引擎 + 单请求流式采集。

并发模型见 spec §4.2（钉死 worker-pool，不用 gather 批处理）。
采集骨架照搬 litellm_client.py:61-103，仅 sync for → async for；
错误提取修正源码 89-94 的常量吞噬 bug。
"""

import asyncio
import random
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from litellm import acompletion, token_counter

from llmperf import common_metrics as cm
from llmperf.benchmark.prompts import sample_output_len, sample_prompt


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
    # None=闭环（默认）；>0 = 开环，每秒到达请求数（泊松过程），此时
    # num_concurrent_requests 语义变为最大在途请求数上限。
    request_rate: Optional[float] = None


async def _single_request(
    config: BenchmarkConfig,
    prompt_text: str,
    num_input_tokens: int,
    sampled_max_tokens: int,
    request_timeout: Optional[float] = None,
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
        # 单请求 HTTP 超时 = 剩余全局预算（worker 传入）；直接调用回退全局 timeout。
        "timeout": config.timeout if request_timeout is None else request_timeout,
    }

    ttft = 0.0
    generated_text = ""
    error_code: Optional[str] = None
    error_msg = ""
    e2e = 0.0
    num_output_tokens = 0

    try:
        # body 组装放进 try：非 dict 的 additional_sampling_params 会让 update 抛
        # TypeError，A2 要求它被就地转成失败记录而非逃逸崩池（评审 finding #1）。
        body.update(config.additional_sampling_params or {})
        start = time.monotonic()
        response = await acompletion(**body)
        async for tok in response:
            # include_usage 末帧/心跳帧会带空 choices，越过 choices[0] 会抛
            # IndexError 把已成功的请求整条误判为失败（H3）。
            if not tok.choices:
                continue
            delta = tok.choices[0].delta
            if not delta:
                continue
            piece = extract_content(delta)
            if piece:
                if ttft == 0:
                    ttft = time.monotonic() - start
                generated_text += piece
        e2e = time.monotonic() - start
        num_output_tokens = token_counter(model=config.model, text=generated_text)
        if generated_text == "":
            error_code = "empty_response"
    except Exception as e:  # 修正源码吞噬 bug：从 e 真实提取（spec §13）。
        error_msg = str(e)
        sc = getattr(e, "status_code", None)
        error_code = "unknown" if sc is None else str(sc)
    return {
        cm.TTFT: ttft,
        cm.E2E_LAT: e2e,
        cm.NUM_INPUT_TOKENS: num_input_tokens,
        cm.NUM_OUTPUT_TOKENS: num_output_tokens,
        cm.ERROR_CODE: error_code,
        cm.ERROR_MSG: error_msg,
    }


def _arrival_offsets(rate: float, count: int, horizon: float) -> List[float]:
    """泊松过程到达时刻表（相对压测起点的秒偏移）。

    首个到达在 0 时刻；间隔 ~ Exp(rate)；生成 count 个或越过 horizon 即止
    （计划到达已超全局预算的请求不生成，对齐闭环"到点不取新任务"语义）。
    """
    offsets: List[float] = []
    t = 0.0
    for _ in range(count):
        if t >= horizon:
            break
        offsets.append(t)
        t += random.expovariate(rate)
    return offsets


async def _dispatch_at(
    config: BenchmarkConfig,
    prompt_text: str,
    num_input_tokens: int,
    sampled_max_tokens: int,
    arrival_offset: float,
    start: float,
    sem: asyncio.Semaphore,
    results: List[Dict[str, Any]],
) -> None:
    """开环单发射：睡到计划到达时刻 → 抢在途名额 → 重查预算 → 发射。

    调度延迟 = 拿到名额时刻 − 计划到达时刻（对所有已发射请求记录，含失败，
    见 spec §3.4：客户端排队与请求成败无关）。
    """
    delay = (start + arrival_offset) - time.monotonic()
    if delay > 0:
        await asyncio.sleep(delay)
    async with sem:
        now = time.monotonic()
        elapsed = now - start
        if elapsed >= config.timeout:
            return  # 预算已尽不再发射，该到达不产生任何记录。
        request_timeout = max(0.0, config.timeout - elapsed)
        m = await _single_request(
            config, prompt_text, num_input_tokens, sampled_max_tokens, request_timeout
        )
        m[cm.SCHED_DELAY] = max(0.0, now - (start + arrival_offset))
        results.append(m)


async def run_benchmark(
    config: BenchmarkConfig,
) -> Tuple[List[Dict[str, Any]], float]:
    """调度入口。request_rate=None 走闭环 worker-pool；>0 走开环泊松发射。"""
    if config.request_rate is not None and config.request_rate <= 0:
        raise ValueError("request_rate must be > 0 (or None for closed-loop)")
    queue: asyncio.Queue = asyncio.Queue()
    for _ in range(config.max_num_completed_requests):
        text, n_in = sample_prompt(
            config.model,
            config.mean_input_tokens,
            config.stddev_input_tokens,
            config.mean_output_tokens,
        )
        max_tokens = sample_output_len(
            config.mean_output_tokens, config.stddev_output_tokens
        )
        await queue.put((text, n_in, max_tokens))

    results: List[Dict[str, Any]] = []
    started = 0
    start = time.monotonic()

    if config.request_rate is not None:
        sem = asyncio.Semaphore(config.num_concurrent_requests)
        offsets = _arrival_offsets(
            config.request_rate,
            config.max_num_completed_requests,
            float(config.timeout),
        )
        tasks = []
        for offset in offsets:
            text, n_in, max_tokens = queue.get_nowait()
            tasks.append(
                asyncio.create_task(
                    _dispatch_at(
                        config, text, n_in, max_tokens, offset, start, sem, results
                    )
                )
            )
        if tasks:
            await asyncio.gather(*tasks)
        return results, time.monotonic() - start

    async def worker() -> None:
        nonlocal started
        while True:
            elapsed = time.monotonic() - start
            if elapsed >= config.timeout:
                break  # 到点停止取新任务，不 cancel 在途请求（spec §4.3）。
            try:
                text, n_in, max_tokens = queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            started += 1
            request_timeout = max(0.0, config.timeout - elapsed)
            m = await _single_request(config, text, n_in, max_tokens, request_timeout)
            results.append(m)

    await asyncio.gather(*[worker() for _ in range(config.num_concurrent_requests)])
    wall_clock_s = time.monotonic() - start
    return results, wall_clock_s
