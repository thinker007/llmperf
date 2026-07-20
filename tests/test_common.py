"""common.py 客户端注册表单测：不实例化 actor，只验证注册与报错路径。"""

import pytest

from llmperf import common


def test_supported_apis_covers_all_registered_clients():
    # M4：if/elif 链时代 SUPPORTED_APIS 漏了 sagemaker/vertexai，
    # 报错信息与实际支持范围不一致。注册表化后两者必须同源。
    assert set(common.SUPPORTED_APIS) == {
        "openai",
        "anthropic",
        "sagemaker",
        "vertexai",
        "litellm",
    }


def test_construct_clients_rejects_unknown_api():
    with pytest.raises(ValueError, match="supported LLM APIs"):
        common.construct_clients(llm_api="bogus", num_clients=1)
