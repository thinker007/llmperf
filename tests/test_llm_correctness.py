"""复现并守护 llm_correctness 的除零崩溃 bug（发现 C）。

评分收尾处 `error_rate = num_errored_requests / num_completed_requests`
（`llm_correctness.py:132`，以及紧邻的 `mismatch_rate`）没有零保护。
若测试超时且一条请求都没完成（`num_completed_requests == 0`），
这里就是 `0 / 0` → 直接抛 ZeroDivisionError，函数无法返回汇总。
上游 ray-project/llmperf open issue #87 报的正是同族的 ZeroDivisionError。

用 test_timeout_s=0 让 while 主循环一次都不进（`time - start < 0` 立即为假），
完成数恒为 0；用 monkeypatch 注入假 construct_clients/RequestsLauncher，
全程不起 Ray、不连网络。
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import llm_correctness as lc


class _FakeLauncher:
    """launch/get_next_ready 全 no-op —— 模拟"零请求完成"。"""

    def __init__(self, _clients):
        pass

    def launch_requests(self, _cfg):
        pass

    def get_next_ready(self, _block=False):
        return []


def _run(monkeypatch):
    monkeypatch.setattr(
        lc, "construct_clients", lambda llm_api, num_clients: [object()]
    )
    monkeypatch.setattr(lc, "RequestsLauncher", _FakeLauncher)
    # test_timeout_s=0 → 主循环不执行 → num_completed_requests 恒为 0。
    return lc.llm_correctness(
        model="fake-model",
        num_concurrent_requests=1,
        max_num_completed_requests=1,
        test_timeout_s=0,
        llm_api="openai",
    )


def test_zero_completed_requests_does_not_crash(monkeypatch):
    # 修复前：此调用会抛 ZeroDivisionError；修复后：正常返回。
    summary, raw_results = _run(monkeypatch)
    assert raw_results == []
    assert summary[lc.common_metrics.NUM_COMPLETED_REQUESTS] == 0


def test_zero_completed_requests_rates_are_zero(monkeypatch):
    summary, _ = _run(monkeypatch)
    assert summary["error_rate"] == 0
    assert summary["mismatch_rate"] == 0
