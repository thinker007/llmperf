"""HTTP 直连模式：绕开 litellm，直接对 OpenAI 兼容 `/chat/completions` 发流式请求。

纯函数（parse_sse_line/extract_delta_text）可直接单测，不连网络；
single_request_sync 是唯一含 I/O 的入口，自洽处理全部异常——任何环节
抛出都被就地转成带 error_code/error_msg 的结果字典，绝不外抛（与
runner.py::_single_request 的 A2 契约一致，见 spec 2026-07-22 §3.4）。
"""

import json
import os
import sys
import time
from typing import Any, Dict, Optional

import requests
from litellm import token_counter

from llmperf import common_metrics as cm


def parse_sse_line(raw_line: bytes) -> Optional[Dict[str, Any]]:
    """'data: {...}' -> dict。'data: [DONE]' / 空行 / 非 data: 行（如 ': ping'）-> None。"""
    line = raw_line.strip()
    if not line:
        return None
    stem = b"data: "
    if not line.startswith(stem):
        return None
    payload = line[len(stem) :]
    if payload == b"[DONE]":
        return None
    return json.loads(payload)


def extract_delta_text(data: Dict[str, Any]) -> Optional[str]:
    """从解析出的 chunk dict 取 delta 文本，优先 reasoning_content 其次 content。

    choices 为空列表时（include_usage 末帧）安全返回 None，不做 [0] 下标
    （H3 教训：runner.py 的 litellm 路径就是踩了这个坑才补的守卫）。
    """
    choices = data.get("choices")
    if not choices:
        return None
    delta = choices[0].get("delta") or {}
    return delta.get("reasoning_content") or delta.get("content")


def _build_url(api_base: str) -> str:
    address = api_base
    if not address.endswith("/"):
        address += "/"
    return address + "chat/completions"


def _build_headers(api_key_env: str) -> Dict[str, str]:
    key = os.environ.get(api_key_env)
    if not key:
        print(
            "warning: env var '%s' not set, sending request without "
            "Authorization header" % api_key_env,
            file=sys.stderr,
        )
        return {}
    return {"Authorization": "Bearer %s" % key}


def single_request_sync(
    model: str,
    api_base: str,
    api_key_env: str,
    num_input_tokens: int,
    request_timeout: float,
    body: Dict[str, Any],
) -> Dict[str, Any]:
    """完整同步实现：POST + 消费 SSE + 计费。绝不抛出，任何异常都转成失败记录。

    在专属线程池里执行（见 runner.py::run_benchmark 的 http_executor），
    因此故意保持完全同步，不做任何 asyncio 相关操作。
    """
    url = _build_url(api_base)
    headers = _build_headers(api_key_env)

    ttft = 0.0
    generated_text = ""
    error_code: Optional[str] = None
    error_msg = ""
    e2e = 0.0
    num_output_tokens = 0
    start = time.monotonic()

    try:
        with requests.post(
            url,
            json=body,
            headers=headers,
            stream=True,
            timeout=request_timeout,
        ) as response:
            if response.status_code != 200:
                error_msg = response.text
                error_code = str(response.status_code)
                response.raise_for_status()
            for raw_line in response.iter_lines(chunk_size=None):
                data = parse_sse_line(raw_line)
                if data is None:
                    continue
                piece = extract_delta_text(data)
                if piece:
                    if ttft == 0:
                        ttft = time.monotonic() - start
                    generated_text += piece
        e2e = time.monotonic() - start
        num_output_tokens = token_counter(model=model, text=generated_text)
        if generated_text == "":
            error_code = "empty_response"
    except Exception as e:
        error_msg = error_msg or str(e)
        if error_code is None:
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
