"""argparse CLI → 组装 config → asyncio.run(run_benchmark) → summarize → stdout/落盘。"""

import argparse
import asyncio
import json
import re
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional

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
    p.add_argument("--metadata", type=parse_metadata, default={})
    return p


def main(argv: Optional[List[str]] = None) -> None:
    args = _build_parser().parse_args(argv)

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
