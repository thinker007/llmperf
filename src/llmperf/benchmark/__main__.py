"""argparse CLI → 组装 config → asyncio.run(run_benchmark) → summarize → stdout/落盘。"""

import argparse
import asyncio
import json
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import litellm

from llmperf.benchmark.metrics import summarize
from llmperf.benchmark.runner import BenchmarkConfig, run_benchmark


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


def parse_json_dict(raw: str) -> Dict[str, Any]:
    """--additional-sampling-params：必须解析成 JSON 对象。

    非法 JSON 由 json.loads 抛 ValueError、合法但非对象（list/int/str/bool）由此处
    抛 ArgumentTypeError——两者都被 argparse 转成友好报错并以退出码 2 退出，避免
    非 dict 值流到下游 body.update() 抛 TypeError 崩掉整个压测池。
    """
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise argparse.ArgumentTypeError(
            "--additional-sampling-params must be a JSON object, got %s"
            % type(value).__name__
        )
    return value


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
    # 开环模式：每秒到达请求数（泊松过程）。不传=闭环；传了之后
    # --num-concurrent-requests 语义变为最大在途请求数上限（spec 2026-07-20 §5）。
    p.add_argument("--request-rate", type=float, default=None)
    p.add_argument("--num-concurrent-requests", type=int, default=10)
    p.add_argument("--max-num-completed-requests", type=int, default=10)
    # 正式测量前先发 N 条 warmup 请求（指标丢弃、不计入测量计时），排除冷启动
    # 对分位数的污染（spec 2026-07-25）。0=不 warmup（默认，等价现状）。
    p.add_argument("--num-warmup-requests", type=int, default=0)
    p.add_argument("--timeout", type=int, default=90)
    # 非法 JSON 或非对象由 argparse 直接友好报错退出（spec I4 + 评审 finding #1）。
    p.add_argument("--additional-sampling-params", type=parse_json_dict, default={})
    p.add_argument("--results-dir", type=str, default="")
    p.add_argument("--metadata", type=parse_metadata, default={})
    # HTTP 直连模式（spec 2026-07-22）：设置后绕开 litellm SDK，直连该 base URL
    # 的 OpenAI 兼容 /chat/completions 端点。不传 = 现状不变（走 litellm）。
    p.add_argument("--api-base", type=str, default=None)
    p.add_argument("--api-key-env", type=str, default="OPENAI_API_KEY")
    return p


def main(argv: Optional[List[str]] = None) -> None:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.num_concurrent_requests < 1:
        parser.error("--num-concurrent-requests must be >= 1")
    if args.max_num_completed_requests < 1:
        parser.error("--max-num-completed-requests must be >= 1")
    if args.timeout <= 0:
        parser.error("--timeout must be > 0")
    # M2：mean<=0 会让 sample_random_positive_int 死循环，必须在此拦截。
    if args.mean_input_tokens <= 0:
        parser.error("--mean-input-tokens must be > 0")
    if args.mean_output_tokens <= 0:
        parser.error("--mean-output-tokens must be > 0")
    if args.stddev_input_tokens < 0:
        parser.error("--stddev-input-tokens must be >= 0")
    if args.stddev_output_tokens < 0:
        parser.error("--stddev-output-tokens must be >= 0")
    if args.request_rate is not None and args.request_rate <= 0:
        parser.error("--request-rate must be > 0")
    if args.num_warmup_requests < 0:
        parser.error("--num-warmup-requests must be >= 0")

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
        request_rate=args.request_rate,
        api_base=args.api_base,
        api_key_env=args.api_key_env,
        num_warmup_requests=args.num_warmup_requests,
    )

    results, wall_clock_s = asyncio.run(run_benchmark(config))
    summary = summarize(results, wall_clock_s)
    summary["model"] = args.model
    summary["num_concurrent_requests"] = args.num_concurrent_requests
    if args.request_rate is not None:
        summary["request_rate"] = args.request_rate
    if args.num_warmup_requests > 0:
        summary["num_warmup_requests"] = args.num_warmup_requests
    summary["backend"] = "http" if args.api_base is not None else "litellm"
    if args.api_base is not None:
        summary["api_base"] = args.api_base
    summary["timestamp"] = int(time.time())
    summary["litellm_version"] = getattr(litellm, "__version__", "unknown")
    for k, v in args.metadata.items():
        if k in summary:
            print(
                "warning: metadata key '%s' conflicts with a built-in field "
                "and was ignored" % k,
                file=sys.stderr,
            )
            continue
        summary[k] = v

    print(json.dumps(summary, indent=4, default=str))

    if args.results_dir:
        results_dir = Path(args.results_dir)
        if not results_dir.exists():
            results_dir.mkdir(parents=True)
        base = clean_filename(
            args.model, args.mean_input_tokens, args.mean_output_tokens
        )
        with open(results_dir / f"{base}_summary.json", "w") as f:
            json.dump(summary, f, indent=4, default=str)
        with open(results_dir / f"{base}_individual_responses.json", "w") as f:
            json.dump(results, f, indent=4, default=str)


if __name__ == "__main__":
    main()
