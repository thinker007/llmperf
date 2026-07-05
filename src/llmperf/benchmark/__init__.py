"""llmperf 内置 asyncio 压测模块。CLI: python -m llmperf.benchmark"""

from llmperf.benchmark.metrics import summarize
from llmperf.benchmark.runner import BenchmarkConfig, run_benchmark

__all__ = ["run_benchmark", "summarize", "BenchmarkConfig"]
