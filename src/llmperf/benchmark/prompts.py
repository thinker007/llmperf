"""按目标 token 数采样 sonnet prompt。移植自 llmperf.utils，分词器换为 litellm.token_counter。"""

import math
import pathlib
import random
from typing import Tuple

from litellm import token_counter

_SONNET_PATH = pathlib.Path(__file__).parent / "sonnet.txt"


def sample_random_positive_int(mean: int, stddev: int) -> int:
    """从高斯分布采样直到得到正整数。移植自 utils.py:121-134。"""
    ret = -1
    while ret <= 0:
        ret = int(random.gauss(mean, stddev))
    return ret


def sample_output_len(mean_output_tokens: int, stddev_output_tokens: int) -> int:
    """为单个请求采样 max_tokens。对应 token_benchmark_ray.py:78-81。"""
    return sample_random_positive_int(mean_output_tokens, stddev_output_tokens)


def sample_prompt(
    model: str,
    mean_input_tokens: int,
    stddev_input_tokens: int,
    expect_output_tokens: int,
) -> Tuple[str, int]:
    """采样一条 prompt，返回 (文本, 实际输入 token 数)。移植自 utils.py:59-118。

    与原版差异：返回值的 token 数对最终文本用 token_counter 重算（口径自洽），
    而非 utils.py:118 返回的采样目标值。
    """
    get_len = lambda text: token_counter(model=model, text=text)

    prompt = (
        "Randomly stream lines from the following text "
        f"with {expect_output_tokens} output tokens. "
        "Don't generate eos tokens:\n\n"
    )
    num_prompt_tokens = sample_random_positive_int(
        mean_input_tokens, stddev_input_tokens
    )
    if stddev_input_tokens == 0:
        # gauss(mean, 0) is deterministic: resampling below can never exceed
        # get_len(prompt), so clamp instead of looping forever.
        num_prompt_tokens = max(num_prompt_tokens, get_len(prompt))
    else:
        while num_prompt_tokens < get_len(prompt):
            num_prompt_tokens = sample_random_positive_int(
                mean_input_tokens, stddev_input_tokens
            )
    remaining = num_prompt_tokens - get_len(prompt)

    with open(_SONNET_PATH, "r") as f:
        sonnet_lines = f.readlines()
    random.shuffle(sonnet_lines)

    sampling = True
    while sampling:
        for line in sonnet_lines:
            line_to_add = line
            if remaining - get_len(line_to_add) < 0:
                line_to_add = line_to_add[: int(math.ceil(remaining))]
                sampling = False
                prompt += line_to_add
                break
            prompt += line_to_add
            remaining -= get_len(line_to_add)

    return prompt, get_len(prompt)
