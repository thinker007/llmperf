from typing import List
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
