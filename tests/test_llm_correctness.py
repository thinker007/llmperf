"""复现并守护 llm_correctness 的两处审计发现（C、D）。

发现 C：评分收尾处 `error_rate = num_errored_requests / num_completed_requests`
（`llm_correctness.py:132`，以及紧邻的 `mismatch_rate`）没有零保护。
若测试超时且一条请求都没完成（`num_completed_requests == 0`），
这里就是 `0 / 0` → 直接抛 ZeroDivisionError，函数无法返回汇总。
上游 ray-project/llmperf open issue #87 报的正是同族的 ZeroDivisionError。

发现 D：`llm_correctness()` 的 `llm_api` 参数默认值是陈旧字符串 `"chat"`，
docstring 也写"chat 或 litellm"；但 `llmperf.common._CLIENT_REGISTRY`（即
`SUPPORTED_APIS`）里根本没有 `"chat"` 这个厂商，真传这个默认值给
`construct_clients` 会直接报错。目前被 CLI 层显式传入的 `"openai"` 掩盖，
但任何人直接调用这个库函数而不传 `llm_api` 就会踩坑。

用 test_timeout_s=0 让 while 主循环一次都不进（`time - start < 0` 立即为假），
完成数恒为 0；用 monkeypatch 注入假 construct_clients/RequestsLauncher，
全程不起 Ray、不连网络。
"""

import inspect
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


def test_default_llm_api_is_a_registered_supported_api():
    # 默认值必须是 _CLIENT_REGISTRY/SUPPORTED_APIS 里真实存在的厂商，而不是
    # 陈旧的 "chat"——否则不传 llm_api 直接调库函数就会被 construct_clients
    # 拒绝。断言"默认值 ∈ 注册表"这一契约本身，而不是硬编码某个具体默认值，
    # 避免未来注册表变化时测试跟着漂移。
    default = inspect.signature(lc.llm_correctness).parameters["llm_api"].default
    assert default in lc.SUPPORTED_APIS


def test_docstring_no_longer_names_the_unregistered_chat_api():
    doc = lc.llm_correctness.__doc__
    assert '"chat"' not in doc
