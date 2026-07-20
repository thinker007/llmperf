"""utils.py 纯函数单测：不连网络、注入确定性 tokenizer。"""

import inspect

import pytest

from llmperf import utils


class _FakeTokenizer:
    """确定性分词器：按空格分词，1 词 = 1 token。"""

    def encode(self, text):
        return text.split()


def test_sonnet_prompt_tokenizer_not_loaded_at_import():
    # H2：tokenizer 不得作为默认参数在 import 期加载重型分词器
    # （否则任何 import llmperf.utils 都会触发下载，连累 llm_correctness 离线 import 失败）。
    default = (
        inspect.signature(utils.randomly_sample_sonnet_lines_prompt)
        .parameters["tokenizer"]
        .default
    )
    assert default is None


def test_sonnet_prompt_accepts_injected_tokenizer():
    # 注入 tokenizer 时不应触碰默认加载路径，且返回口径自洽。
    text, n = utils.randomly_sample_sonnet_lines_prompt(
        prompt_tokens_mean=80,
        prompt_tokens_stddev=0,
        expect_output_tokens=50,
        tokenizer=_FakeTokenizer(),
    )
    assert n > 0
    assert "output tokens" in text  # 基底 prompt 前缀保留


@pytest.mark.parametrize("mean,stddev", [(0, 0), (-5, 10)])
def test_sample_random_positive_int_rejects_non_positive_mean(mean, stddev):
    # M2：mean<=0 时（stddev=0 必然、stddev>0 概率性）永远采不出正数 → 死循环。
    # 快速失败而不是挂死整个压测进程。
    with pytest.raises(ValueError):
        utils.sample_random_positive_int(mean, stddev)


def test_sonnet_prompt_stddev_zero_below_base_does_not_hang():
    # M2：stddev=0 且 mean 小于基底 prompt 长度时，utils.py 的重采样循环
    # 永远采到同一个值 → 死循环。prompts.py:47-50 已修，此处回移同款 clamp。
    text, n = utils.randomly_sample_sonnet_lines_prompt(
        prompt_tokens_mean=5,
        prompt_tokens_stddev=0,
        expect_output_tokens=50,
        tokenizer=_FakeTokenizer(),
    )
    assert n >= 5  # clamp 到基底长度，而非低于它
