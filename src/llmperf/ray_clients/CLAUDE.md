[根目录](../../../CLAUDE.md) > [src](../../) > [llmperf](../) > **ray_clients**

## 模块职责

`ray_clients/` 收纳 Ray 压测路线的 5 个厂商客户端。每个客户端都是一个 `@ray.remote` 装饰的 `LLMClient` 子类，被 `RequestsLauncher`/`ActorPool` 派发执行。它们是"一个插座配多种插头"设计里的**插头**：主调度逻辑（`token_benchmark_ray.py` + `requests_launcher.py`）与厂商 API 差异在此隔离，新增厂商只需新增一个客户端 + 在 `common.py::construct_clients()` 加一个分支。

## 统一接口契约

所有客户端**必须**实现（见 `../ray_llm_client.py`）：

```python
def llm_request(self, request_config: RequestConfig) -> Tuple[Dict[str, Any], str, RequestConfig]:
    ...  # 返回 (metrics, generated_text, request_config) 三元组
```

- `metrics` 的键统一走 `common_metrics` 常量（`TTFT` / `INTER_TOKEN_LAT` / `E2E_LAT` / `REQ_OUTPUT_THROUGHPUT` / `NUM_*` / `ERROR_CODE` / `ERROR_MSG`）。
- 分词口径统一用固定的 Llama 分词器（`hf-internal-testing/llama-tokenizer`），保证跨模型 token 计数可比。
- 易错的协议解析（SSE 分帧）应抽成不依赖 Ray/网络的纯函数，便于普通 pytest 断言——`anthropic_messages_client.py` 的 `parse_data_line` / `extract_delta_text` 是**范例**，也是唯一被单测覆盖的客户端。

## 文件清单

| 文件 | 厂商 / 用途 | 采集方式 | 备注 |
|---|---|---|---|
| `openai_chat_completions_client.py` | OpenAI 兼容 `/chat/completions`（默认、最常用） | SSE 流式，内联解析 | 支持 `reasoning_content`；SSE 解析内联未抽纯函数（未被单测覆盖） |
| `anthropic_messages_client.py` | Anthropic 原生 `/v1/messages` | SSE 流式，纯函数分帧 | 由"借道 LiteLLM"改为原生客户端以获得更纯净延迟测量；有单测 |
| `litellm_client.py` | LiteLLM 支持的 100+ 家（万能兜底） | `litellm.completion` 流式 | — |
| `sagemaker_client.py` | AWS SageMaker Endpoint | `invoke_endpoint_with_response_stream` + `LineIterator` | 需 `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`/`AWS_REGION_NAME` |
| `vertexai_client.py` | GCP VertexAI Endpoint | 非流式 `:predict`，TTFT 记 -1、ITL 由均值回填 | 需 `GCLOUD_PROJECT_ID`/`GCLOUD_REGION`/`VERTEXAI_ENDPOINT_ID`/`GCLOUD_ACCESS_TOKEN` |

## 已知口径差异与遗留问题（改动前必读）

- **`INTER_TOKEN_LAT` 存储口径不一致**：openai/anthropic/litellm 存**标量** `sum(time_to_next_token)`；sagemaker/vertexai 存**列表**。`token_benchmark_ray.py` 主循环对该键做 `/= NUM_OUTPUT_TOKENS`，对列表会抛 `TypeError`。经 sagemaker/vertexai 入口压测前需先统一（建议向标量对齐）。**此为遗留问题，尚未修复。**
- 5 个客户端的返回类型注解仍写作 `-> Dict[str, Any]`，与契约 `-> Tuple[...]` 不符（运行期返回值正确，仅注解误导）。
- 5 个客户端末尾的 metrics 装配块高度重复（DRY 异味），是"漏更新某个变量"类 bug 的温床——建议后续抽 `assemble_metrics(...)` 收口。

## 新增一个客户端的标准流程

1. 在本目录新建 `xxx_client.py`，实现 `@ray.remote class XxxClient(LLMClient)` 与 `llm_request`（返回三元组）。
2. 在 `../common.py::construct_clients()` 加分支，并把 `xxx` 加入 `SUPPORTED_APIS`。
3. 把易错的分帧/解析抽成纯函数，在 `tests/` 加纯函数单测（参考 anthropic）。

## 变更记录 (Changelog)

- 2026-07-19（高危修复）：**H1** `openai_chat_completions_client.py` 补上 content 分支内 `most_recent_received_token_time = time.monotonic()` 更新，修复逐字延迟(ITL)退化为累计值的测量错误（此前该变量初始化后从未更新）。**H4/H5** `vertexai_client.py`：`response_code` 预置为 `None`（避免 `requests.post` 抛错时 except 分支 `UnboundLocalError` 掩盖真异常）；`GCLOUD_ACCESS_TOKEN` 的 `.strip()` 延后到非空校验之后（避免未设置时 `None.strip()` 抛 `AttributeError` 使友好 `ValueError` 不可达）；去掉重复的 `print(response_code)`。
- 此前：`anthropic` 由借道 LiteLLM 改为原生 `/v1/messages` 客户端，去中间层以获得更纯净的延迟测量。
