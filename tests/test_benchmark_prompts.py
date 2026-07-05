"""prompts.py 采样逻辑单测：纯逻辑，注入确定性 token_counter，无网络。"""

from llmperf.benchmark import prompts


def test_sample_random_positive_int_always_positive():
    # 移植自 utils.py:121-134，gauss 可能出负数，必须重采样到正。
    for _ in range(1000):
        assert prompts.sample_random_positive_int(5, 100) > 0


def test_sample_output_len_positive():
    assert prompts.sample_output_len(150, 80) > 0


def test_sample_prompt_returns_actual_token_count(monkeypatch):
    # 确定性计数器：按空格分词，1 词 = 1 token。
    monkeypatch.setattr(prompts, "token_counter", lambda model, text: len(text.split()))
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
    monkeypatch.setattr(prompts, "token_counter", lambda model, text: len(text.split()))
    text, n_in = prompts.sample_prompt("gpt-4o", 5, 0, 10)
    assert n_in > 0
