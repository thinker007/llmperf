import json
import os
import time
from typing import Any, Dict, Optional

import ray
import requests

from llmperf.ray_llm_client import LLMClient
from llmperf.models import RequestConfig
from llmperf import common_metrics

# Anthropic requires max_tokens; fall back to this when the caller omits it.
DEFAULT_MAX_TOKENS = 512


def parse_data_line(chunk: bytes) -> Optional[Dict[str, Any]]:
    """Extract the JSON payload from an Anthropic SSE line.

    The Messages API interleaves ``event:`` lines, ``data:`` lines, ``: ping``
    heartbeats and blank separators. Only ``data:`` lines carry JSON we care
    about; everything else returns ``None``.
    """
    if not chunk:
        return None
    stem = b"data: "
    if not chunk.startswith(stem):
        return None
    payload = chunk[len(stem) :]
    return json.loads(payload)


def extract_delta_text(data: Dict[str, Any]) -> Optional[str]:
    """Return the incremental text from a ``content_block_delta`` event.

    Handles both regular ``text_delta`` blocks and ``thinking_delta`` blocks
    (reasoning models), mirroring the reasoning_content handling in the OpenAI
    client so the first thinking token counts toward TTFT. Control frames
    (message_start/stop, content_block_start/stop, message_delta) return None.
    """
    if data.get("type") != "content_block_delta":
        return None
    delta = data.get("delta", {})
    if delta.get("type") == "text_delta":
        return delta.get("text")
    if delta.get("type") == "thinking_delta":
        return delta.get("thinking")
    return None


@ray.remote
class AnthropicMessagesClient(LLMClient):
    """Client for the Anthropic Messages API (POST /v1/messages)."""

    def llm_request(self, request_config: RequestConfig) -> Dict[str, Any]:
        prompt = request_config.prompt
        prompt, prompt_len = prompt

        message = [
            {"role": "user", "content": prompt},
        ]
        model = request_config.model
        body = {
            "model": model,
            "messages": message,
            "stream": True,
        }
        sampling_params = request_config.sampling_params
        body.update(sampling_params or {})
        # max_tokens is required by the Messages API; ensure it is present.
        body.setdefault("max_tokens", DEFAULT_MAX_TOKENS)

        time_to_next_token = []
        tokens_received = 0
        ttft = 0
        error_response_code = -1
        generated_text = ""
        error_msg = ""
        output_throughput = 0
        total_request_time = 0

        metrics = {}

        metrics[common_metrics.ERROR_CODE] = None
        metrics[common_metrics.ERROR_MSG] = ""

        start_time = time.monotonic()
        most_recent_received_token_time = time.monotonic()

        address = os.environ.get("ANTHROPIC_API_BASE", "https://api.anthropic.com")
        if not address:
            raise ValueError("the environment variable ANTHROPIC_API_BASE must be set.")
        key = os.environ.get("ANTHROPIC_API_KEY")
        if not key:
            raise ValueError("the environment variable ANTHROPIC_API_KEY must be set.")
        version = os.environ.get("ANTHROPIC_VERSION", "2023-06-01")
        headers = {
            "x-api-key": key,
            "anthropic-version": version,
            "content-type": "application/json",
        }
        if not address.endswith("/"):
            address = address + "/"
        address += "v1/messages"
        try:
            with requests.post(
                address,
                json=body,
                stream=True,
                timeout=180,
                headers=headers,
            ) as response:
                if response.status_code != 200:
                    error_msg = response.text
                    error_response_code = response.status_code
                    response.raise_for_status()
                for chunk in response.iter_lines(chunk_size=None):
                    chunk = chunk.strip()
                    data = parse_data_line(chunk)
                    if data is None:
                        continue

                    if data.get("type") == "error":
                        error = data.get("error", {})
                        error_msg = error.get("message", "")
                        error_response_code = error.get("type", "")
                        raise RuntimeError(error_msg)

                    delta_text = extract_delta_text(data)
                    if delta_text:
                        if ttft == 0:
                            ttft = time.monotonic() - start_time
                            time_to_next_token.append(ttft)
                        else:
                            time_to_next_token.append(
                                time.monotonic() - most_recent_received_token_time
                            )
                        most_recent_received_token_time = time.monotonic()
                        tokens_received += 1
                        generated_text += delta_text

            total_request_time = time.monotonic() - start_time
            output_throughput = tokens_received / total_request_time

        except Exception as e:
            metrics[common_metrics.ERROR_MSG] = error_msg
            metrics[common_metrics.ERROR_CODE] = error_response_code
            tokens_received = 1
            print("-------------body-------------")
            print(body)
            print("-------------body-------------")
            print(f"Warning Or Error: {e}")
            print(error_response_code)
            print(error_msg)

        metrics[common_metrics.INTER_TOKEN_LAT] = sum(time_to_next_token)
        metrics[common_metrics.TTFT] = ttft
        metrics[common_metrics.E2E_LAT] = total_request_time
        metrics[common_metrics.REQ_OUTPUT_THROUGHPUT] = output_throughput
        metrics[common_metrics.NUM_TOTAL_TOKENS] = tokens_received + prompt_len
        metrics[common_metrics.NUM_OUTPUT_TOKENS] = tokens_received
        metrics[common_metrics.NUM_INPUT_TOKENS] = prompt_len

        return metrics, generated_text, request_config
