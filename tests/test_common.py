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


def test_resolve_llm_api_explicit_flag_wins():
    env = {"ANTHROPIC_API_KEY": "ak"}
    assert common.resolve_llm_api("openai", env) == "openai"
    assert common.resolve_llm_api("anthropic", {"OPENAI_API_KEY": "ok"}) == "anthropic"


def test_resolve_llm_api_infers_anthropic_from_anthropic_key():
    # 未显式给 --llm-api 时，只配了 ANTHROPIC_API_KEY 就走原生 /v1/messages。
    assert common.resolve_llm_api(None, {"ANTHROPIC_API_KEY": "ak"}) == "anthropic"


def test_resolve_llm_api_infers_openai_from_openai_key_or_none():
    assert common.resolve_llm_api(None, {"OPENAI_API_KEY": "ok"}) == "openai"
    assert common.resolve_llm_api(None, {}) == "openai"


def test_resolve_llm_api_both_keys_keeps_openai_default():
    # 两个都配时不猜，保持历史默认 openai，老脚本行为不变。
    env = {"ANTHROPIC_API_KEY": "ak", "OPENAI_API_KEY": "ok"}
    assert common.resolve_llm_api(None, env) == "openai"
