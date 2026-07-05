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
    error_codes = [
        r[cm.ERROR_CODE] for r in results if r.get(cm.ERROR_CODE) is not None
    ]
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
