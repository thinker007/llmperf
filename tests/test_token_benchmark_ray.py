"""复现并守护 token_benchmark_ray 的审计发现（B、A）。

发现 B：收尾段（"check one last time..."）把残余结果并入 completed_requests 时，
错误地对单个 request_metrics（dict）调 list.extend —— extend 会迭代 dict 的
键，把 "ttft_s"/"error_code" 等字符串塞进结果列表，随后 metrics_summary 里
pd.DataFrame + df[ERROR_CODE] 会崩。正确应为 append(request_metrics)。

发现 A：主循环把 Inter-Token Latency 除以 request_metrics 里客户端回传、尚未
被覆盖的 NUM_OUTPUT_TOKENS（陈旧帧数），收尾段却除以 get_token_length 重算
出的 num_output_tokens（Llama 分词器口径，也是最终写回 NUM_OUTPUT_TOKENS/
NUM_TOTAL_TOKENS/REQ_OUTPUT_THROUGHPUT 的口径）。当客户端回传的计数与本地
分词器重算结果不一致时，同一批请求走"主循环正常完成"与"收尾捞取"两条路，
算出的 ITL 会不一致，结果内部自相矛盾。

用 test_timeout_s=0 让 worker 线程不进主循环、直接命中收尾捞取路径；
用足够大的 test_timeout_s 让请求在主循环内完成。全程用 monkeypatch 注入假
分词器/假发射器，不起 Ray、不连网络。
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


def _make_metrics_with_stale_client_token_count():
    """模拟客户端回传的 NUM_OUTPUT_TOKENS（如流式帧数）与后续用分词器重算的
    值不同——真实场景里两者本就可能不等（例如多字节 token 一帧多个字符）。"""
    return {
        cm.TTFT: 0.5,
        cm.E2E_LAT: 1.0,
        cm.INTER_TOKEN_LAT: 2.2,
        cm.NUM_INPUT_TOKENS: 7,
        cm.NUM_OUTPUT_TOKENS: 5,  # 陈旧值：真实生成文本重算后是 11
        cm.ERROR_CODE: None,
        cm.ERROR_MSG: "",
    }


class _OneShotLauncher:
    """launch 全 no-op；每个实例的首次 get_next_ready 吐出一条结果，此后吐空。"""

    def __init__(self, clients):
        self._served = False

    def launch_requests(self, cfg):
        pass

    def get_next_ready(self, block=False):
        if self._served:
            return []
        self._served = True
        return [(_make_metrics_with_stale_client_token_count(), "hello world", None)]


def _run_single_request(monkeypatch, test_timeout_s):
    monkeypatch.setattr(tbr, "LlamaTokenizerFast", _FakeTokCls)
    monkeypatch.setattr(
        tbr, "construct_clients", lambda llm_api, num_clients: [object()]
    )
    monkeypatch.setattr(tbr, "RequestsLauncher", _OneShotLauncher)
    monkeypatch.setattr(tbr, "sample_random_positive_int", lambda mean, stddev: 5)
    monkeypatch.setattr(
        tbr,
        "randomly_sample_sonnet_lines_prompt",
        lambda **kwargs: ("a prompt", 7),
    )

    return tbr.get_token_throughput_latencies(
        model="fake-model",
        mean_input_tokens=550,
        stddev_input_tokens=150,
        mean_output_tokens=150,
        stddev_output_tokens=10,
        num_concurrent_requests=1,
        max_num_completed_requests=1,
        test_timeout_s=test_timeout_s,
        llm_api="openai",
    )


def test_main_loop_itl_uses_recomputed_token_count_not_stale_client_count(monkeypatch):
    # test_timeout_s>0 → 请求在主循环内正常完成（不经过收尾捞取路径）。
    _, individual = _run_single_request(monkeypatch, test_timeout_s=10)

    assert len(individual) == 1
    # "hello world" 经假分词器（1 字符 = 1 token）重算为 11 个 token，而不是
    # 客户端回传的陈旧值 5。ITL 必须用重算值 11 做除数，不是陈旧值 5。
    assert individual[0][cm.NUM_OUTPUT_TOKENS] == 11
    assert individual[0][cm.INTER_TOKEN_LAT] == pytest.approx(2.2 / 11)


def test_main_loop_and_cleanup_paths_produce_identical_itl(monkeypatch):
    # 同一份带陈旧 NUM_OUTPUT_TOKENS 的输入，分别走主循环路径（timeout>0）与
    # 收尾捞取路径（timeout=0），两路算出的 ITL 必须完全一致。
    _, via_main_loop = _run_single_request(monkeypatch, test_timeout_s=10)
    _, via_cleanup = _run_single_request(monkeypatch, test_timeout_s=0)

    assert via_main_loop[0][cm.INTER_TOKEN_LAT] == via_cleanup[0][cm.INTER_TOKEN_LAT]
    assert (
        via_main_loop[0][cm.NUM_OUTPUT_TOKENS] == via_cleanup[0][cm.NUM_OUTPUT_TOKENS]
    )
