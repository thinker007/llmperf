from typing import List, Mapping, Optional
from llmperf.ray_clients.anthropic_messages_client import AnthropicMessagesClient
from llmperf.ray_clients.litellm_client import LiteLLMClient
from llmperf.ray_clients.openai_chat_completions_client import (
    OpenAIChatCompletionsClient,
)
from llmperf.ray_clients.sagemaker_client import SageMakerClient
from llmperf.ray_clients.vertexai_client import VertexAIClient
from llmperf.ray_llm_client import LLMClient

# Registry keeps SUPPORTED_APIS and the constructor branches in sync: the error
# message can never again claim an API the code doesn't actually build.
_CLIENT_REGISTRY = {
    "openai": OpenAIChatCompletionsClient,
    "anthropic": AnthropicMessagesClient,
    "sagemaker": SageMakerClient,
    "vertexai": VertexAIClient,
    "litellm": LiteLLMClient,
}

SUPPORTED_APIS = list(_CLIENT_REGISTRY)


def resolve_llm_api(llm_api: Optional[str], env: Mapping[str, str]) -> str:
    """Pick the client when ``--llm-api`` is omitted.

    An explicit value always wins. Otherwise only ``ANTHROPIC_API_KEY`` set
    (no ``OPENAI_API_KEY``) selects the native Messages client; every other
    case keeps the historical ``openai`` default, so scripts with both keys
    do not silently switch protocol.
    """
    if llm_api:
        return llm_api
    if env.get("ANTHROPIC_API_KEY") and not env.get("OPENAI_API_KEY"):
        return "anthropic"
    return "openai"


def construct_clients(llm_api: str, num_clients: int) -> List[LLMClient]:
    """Construct LLMClients that will be used to make requests to the LLM API.

    Args:
        llm_api: The name of the LLM API to use.
        num_clients: The number of concurrent requests to make.

    Returns:
        The constructed LLMCLients

    """
    try:
        client_cls = _CLIENT_REGISTRY[llm_api]
    except KeyError:
        raise ValueError(
            f"llm_api must be one of the supported LLM APIs: {SUPPORTED_APIS}"
        )

    return [client_cls.remote() for _ in range(num_clients)]
