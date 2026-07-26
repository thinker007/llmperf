"""复现并守护 token_benchmark_ray 收尾段的结果污染 bug（发现 B）。

收尾段（"check one last time..."）把残余结果并入 completed_requests 时，
错误地对单个 request_metrics（dict）调 list.extend —— extend 会迭代 dict 的
键，把 "ttft_s"/"error_code" 等字符串塞进结果列表，随后 metrics_summary 里
pd.DataFrame + df[ERROR_CODE] 会崩。正确应为 append(request_metrics)。

用 test_timeout_s=0 让 worker 线程不进主循环、直接命中收尾捞取路径；
用 monkeypatch 注入假分词器/假发射器，全程不起 Ray、不连网络。
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import token_benchmark_ray as tbr
from llmperf import common_metrics as cm


class _FakeTok:
    def encode(self, text):
        return list(text)


class _FakeTokCls:
    @staticmethod
    def from_pretrained(name):
        return _FakeTok()


def _make_metrics():
    return {
        cm.TTFT: 0.5,
        cm.E2E_LAT: 1.0,
        cm.INTER_TOKEN_LAT: 2.2,
        cm.NUM_INPUT_TOKENS: 7,
        cm.ERROR_CODE: None,
        cm.ERROR_MSG: "",
    }


class _FakeLauncher:
    """launch 全 no-op；首个调用 get_next_ready 的实例（即收尾捞取器）返回一条残余。"""

    def __init__(self, clients):
        self._served = False

    def launch_requests(self, cfg):
        pass

    def get_next_ready(self, block=False):
        if self._served:
            return []
        self._served = True
        return [(_make_metrics(), "hello world", None)]


def test_cleanup_residual_kept_as_dict_not_polluted(monkeypatch):
    monkeypatch.setattr(tbr, "LlamaTokenizerFast", _FakeTokCls)
    monkeypatch.setattr(
        tbr, "construct_clients", lambda llm_api, num_clients: [object()]
    )
    monkeypatch.setattr(tbr, "RequestsLauncher", _FakeLauncher)
    monkeypatch.setattr(tbr, "sample_random_positive_int", lambda mean, stddev: 5)
    monkeypatch.setattr(
        tbr,
        "randomly_sample_sonnet_lines_prompt",
        lambda **kwargs: ("a prompt", 7),
    )

    # test_timeout_s=0 → worker 线程立即跳出主循环，残余只在收尾段被捞取。
    summary, individual = tbr.get_token_throughput_latencies(
        model="fake-model",
        mean_input_tokens=550,
        stddev_input_tokens=150,
        mean_output_tokens=150,
        stddev_output_tokens=10,
        num_concurrent_requests=1,
        max_num_completed_requests=1,
        test_timeout_s=0,
        llm_api="openai",
    )

    # bug 下 individual 会是 ["ttft_s", "error_code", ...] 一堆字符串，且
    # metrics_summary 会先抛 KeyError 让本函数根本返回不了。
    assert len(individual) == 1
    assert all(isinstance(r, dict) for r in individual), individual
    assert individual[0][cm.NUM_OUTPUT_TOKENS] == len("hello world")
