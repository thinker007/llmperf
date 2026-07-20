"""prompts.py 采样逻辑单测：纯逻辑，注入确定性 token_counter，无网络。"""

import pytest

from llmperf.benchmark import prompts


def fake_token_counter(model: str, text: str) -> int:
    # 确定性计数器：按空格分词，1 词 = 1 token。参数名须与
    # litellm.token_counter 的关键字调用 (model=, text=) 保持一致。
    del model
    return len(text.split())


def test_sample_random_positive_int_always_positive():
    # 移植自 utils.py:121-134，gauss 可能出负数，必须重采样到正。
    for _ in range(1000):
        assert prompts.sample_random_positive_int(5, 100) > 0


def test_sample_output_len_positive():
    assert prompts.sample_output_len(150, 80) > 0


def test_sample_prompt_returns_actual_token_count(monkeypatch):
    monkeypatch.setattr(prompts, "token_counter", fake_token_counter)
    text, n_in = prompts.sample_prompt(
        model="gpt-4o",
        mean_input_tokens=80,
        stddev_input_tokens=0,
        expect_output_tokens=50,
    )
    # 口径自洽：返回的 token 数 == 对最终文本重算的计数（spec §5.2）。
    assert n_in == len(text.split())
    assert n_in >= 1
    assert "output tokens" in text  # 基底 prompt 前缀保留（utils.py:88-90）


def test_sample_prompt_converges_with_small_mean(monkeypatch):
    # mean 远小于基底前缀时，utils.py:96-99 的 while 重采样保证不死循环。
    monkeypatch.setattr(prompts, "token_counter", fake_token_counter)
    _, n_in = prompts.sample_prompt("gpt-4o", 5, 0, 10)
    assert n_in > 0


@pytest.mark.parametrize("mean,stddev", [(0, 0), (-1, 0), (-100, 50)])
def test_sample_random_positive_int_rejects_non_positive_mean(mean, stddev):
    # M2：与 utils.py 同款死循环——mean<=0 永远采不出正数（stddev>0 也概率性空转），必须快速失败。
    with pytest.raises(ValueError):
        prompts.sample_random_positive_int(mean, stddev)
