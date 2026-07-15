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
        max_tokens = sample_output_len(
            config.mean_output_tokens, config.stddev_output_tokens
        )
        await queue.put((text, n_in, max_tokens))

    results: List[Dict[str, Any]] = []
    started = 0
    start = time.monotonic()

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
