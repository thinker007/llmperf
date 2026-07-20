"""metrics.py 纯函数单测：换算正确性 + 全部除零/统计边界。无网络。"""

import json

from llmperf import common_metrics as cm
from llmperf.benchmark import metrics


def _raw(ttft, e2e, n_in, n_out, error_code=None, error_msg=""):
    return {
        cm.TTFT: ttft,
        cm.E2E_LAT: e2e,
        cm.NUM_INPUT_TOKENS: n_in,
        cm.NUM_OUTPUT_TOKENS: n_out,
        cm.ERROR_CODE: error_code,
        cm.ERROR_MSG: error_msg,
    }


# ---- derive_request_metrics 换算 ----


def test_itl_normal():
    d = metrics.derive_request_metrics(_raw(0.1, 1.1, 10, 5))
    assert d[cm.INTER_TOKEN_LAT] == (1.1 - 0.1) / (5 - 1)  # 0.25


def test_throughput_and_total():
    d = metrics.derive_request_metrics(_raw(0.1, 2.0, 10, 6))
    assert d[cm.REQ_OUTPUT_THROUGHPUT] == 6 / 2.0  # 3.0
    assert d[cm.NUM_TOTAL_TOKENS] == 16


def test_itl_zero_when_out_le_1():
    assert (
        metrics.derive_request_metrics(_raw(0.1, 1.1, 10, 1))[cm.INTER_TOKEN_LAT] == 0
    )
    assert (
        metrics.derive_request_metrics(_raw(0.1, 1.1, 10, 0))[cm.INTER_TOKEN_LAT] == 0
    )


def test_throughput_zero_when_e2e_zero():
    assert (
        metrics.derive_request_metrics(_raw(0.0, 0.0, 10, 5))[cm.REQ_OUTPUT_THROUGHPUT]
        == 0
    )


# ---- _safe_* 边界 ----


def test_safe_quantiles_empty_returns_none():
    q = metrics._safe_quantiles([])
    assert all(v is None for v in q.values())


def test_safe_quantiles_single_returns_value():
    q = metrics._safe_quantiles([0.5])
    assert q["p50"] == 0.5
    assert q["p99"] == 0.5


def test_safe_stdev_edges():
    assert metrics._safe_stdev([]) is None
    assert metrics._safe_stdev([0.5]) == 0
    assert metrics._safe_stdev([1.0, 3.0]) > 0


def test_safe_div_zero_denominator():
    assert metrics._safe_div(5, 0) is None
    assert metrics._safe_div(5, 2) == 2.5


# ---- summarize 聚合 + 整体边界 ----


def test_summarize_excludes_errored_from_quantiles():
    results = [
        _raw(0.1, 1.1, 10, 5),
        _raw(0.0, 0.0, 0, 0, error_code="500", error_msg="boom"),
    ]
    out = metrics.summarize(results, wall_clock_s=2.0)
    # 成功 1 条进分位数；错误条计入 error_rate。
    assert out[cm.TTFT]["mean"] == 0.1
    assert out[cm.NUM_ERRORS] == 1
    assert out[cm.ERROR_RATE] == 0.5
    assert out[cm.NUM_REQ_STARTED] == 2


def test_summarize_all_failed_returns_none_quantiles():
    results = [_raw(0.0, 0.0, 0, 0, error_code="500", error_msg="x")]
    out = metrics.summarize(results, wall_clock_s=2.0)
    assert out[cm.TTFT]["mean"] is None
    assert out[cm.ERROR_RATE] == 1.0


def test_summarize_zero_wall_clock_returns_none_throughput():
    out = metrics.summarize([_raw(0.1, 1.1, 10, 5)], wall_clock_s=0.0)
    assert out[cm.OUTPUT_THROUGHPUT] is None
    assert out[cm.COMPLETED_REQUESTS_PER_MIN] is None


def test_summarize_zero_started_returns_none_error_rate():
    out = metrics.summarize([], wall_clock_s=2.0)
    assert out[cm.ERROR_RATE] is None
    assert out[cm.NUM_REQ_STARTED] == 0


def test_summarize_error_code_keys_are_str_and_json_safe():
    # status_code 可能是 int/None，归一为 str，json.dumps 不抛 TypeError（spec I2）。
    results = [
        _raw(0.0, 0.0, 0, 0, error_code="500", error_msg="a"),
        _raw(0.0, 0.0, 0, 0, error_code="unknown", error_msg="b"),
    ]
    out = metrics.summarize(results, wall_clock_s=2.0)
    freq = out[cm.ERROR_CODE_FREQ]
    assert all(isinstance(k, str) for k in freq)
    json.dumps(out)  # 不抛异常


# ---- 开环速率模式（spec 2026-07-20 §4/§6 M1-M2） ----


def test_summarize_open_loop_adds_sched_delay_and_achieved_rate():
    # M1：结果带 SCHED_DELAY 键即开环运行 → summary 追加统计块与达成速率；
    # 错误请求的调度延迟同样计入（客户端排队与请求成败无关，spec §3.4）。
    ok = _raw(0.1, 1.1, 10, 5)
    ok[cm.SCHED_DELAY] = 0.2
    bad = _raw(0.0, 0.0, 0, 0, error_code="500", error_msg="boom")
    bad[cm.SCHED_DELAY] = 0.6
    out = metrics.summarize([ok, bad], wall_clock_s=2.0)

    block = out[cm.SCHED_DELAY]
    assert block["mean"] == (0.2 + 0.6) / 2  # 错误请求计入
    assert block["min"] == 0.2
    assert block["max"] == 0.6
    assert "quantiles" in block and "stddev" in block  # 与其它指标同形
    assert out[cm.ACHIEVED_REQUEST_RATE] == 2 / 2.0  # started / wall
    json.dumps(out)  # JSON 安全


def test_summarize_closed_loop_has_no_open_loop_keys():
    # M2：闭环结果不带 SCHED_DELAY 键 → summary 键集合与现状逐字节一致。
    out = metrics.summarize([_raw(0.1, 1.1, 10, 5)], wall_clock_s=2.0)
    assert cm.SCHED_DELAY not in out
    assert cm.ACHIEVED_REQUEST_RATE not in out


def test_summarize_counts_empty_response_as_error():
    results = [
        {
            cm.TTFT: 0.0,
            cm.E2E_LAT: 0.0,
            cm.NUM_INPUT_TOKENS: 5,
            cm.NUM_OUTPUT_TOKENS: 0,
            cm.ERROR_CODE: "empty_response",
            cm.ERROR_MSG: "",
        }
    ]
    out = metrics.summarize(results, wall_clock_s=1.0)
    assert out[cm.NUM_ERRORS] == 1
    assert out[cm.NUM_COMPLETED_REQUESTS] == 0
    assert out[cm.ERROR_CODE_FREQ]["empty_response"] == 1
