# Spec: `benchmark` HTTP 直连模式（`--api-base`，原规划中的"Proxy 模式"）

- 日期：2026-07-22
- 范围：`src/llmperf/benchmark/`（新增 `http_client.py` + `runner.py`/`__main__.py` 增量接线）
- 类型：增量新功能。**不传 `--api-base` 时 litellm 路径行为与 summary/individual JSON 输出结构完全不变**
- 前置：`benchmark` 模块已完工、生产硬化、开环速率模式均已落地（57 测试函数 / pytest 70 例全绿）

## 1. 动机与目标

`runner.py::_single_request` 目前唯一的发射方式是 `litellm.acompletion(**body)`——所有请求都要经过 litellm 的 provider 识别/参数翻译层。这在两类场景下不够用：

1. **自建/网关兼容端点**：用户自己起了一个 vLLM/TGI/LM Studio 或内部网关，暴露的是"看起来像 OpenAI 但 litellm 不认识这个 model 名字前缀"的 `/chat/completions`。虽然 litellm 支持 `api_base` 覆盖，但仍要经过其内部的 provider 判定与请求体转换逻辑。
2. **测"裸延迟"**：本仓库在 Ray 路线上已有过一次同类决策——`--llm-api anthropic` 从"借道 litellm"改造成原生 `/v1/messages` 客户端，`src/llmperf/ray_clients/CLAUDE.md` 明确记录了理由是"去掉中间商，把延迟测准"。`benchmark` 子模块目前没有对等能力：所有 litellm 路径的延迟里都混了一层 SDK 开销。

### 目标

- 新增 `--api-base &lt;url&gt;`（可选）。传入后，本次运行的所有请求改用 `requests` 直接 POST 目标 `{api_base}/chat/completions`，手工解析 SSE，完全绕开 litellm。
- 复用现有 `metrics.py`/`summarize`/开环调度（`_dispatch_at`）/闭环调度（`worker`）：只替换"怎么发一条请求"这一层，`_single_request` 对外的返回字典形状不变，其余管线**零改动**。
- 保持"零新增依赖"：`requests` 已是仓库既有运行时依赖（`ray_clients/openai_chat_completions_client.py` 已在用），不引入 `aiohttp`/`httpx`。

### 非目标（本轮明确不做）

- 支持非 OpenAI 兼容的响应形状（Anthropic 风格 SSE 等）——HTTP 直连模式假定目标端点遵循 OpenAI `/chat/completions` 流式协议，这覆盖了 vLLM/TGI/LM Studio/绝大多数网关的默认行为。真要测原生 Anthropic 协议，走 Ray 路线的 `anthropic_messages_client.py`。
- 非流式（`stream: false`）响应——`benchmark` 模块的 TTFT/ITL 指标本就依赖流式，非流式模式没有意义。
- URL 格式的严格校验——非法 `--api-base` 会在 `requests.post` 阶段自然抛错，被下述统一异常兜底转成失败记录，不必提前校验（YAGNI）。

## 2. 开发方式

全程 TDD：Red → Green → `black` → commit。新协议解析逻辑（SSE 分帧、delta 取值）抽成纯函数，直接单测，不连网络——对齐 `anthropic_messages_client.py::parse_data_line`/`extract_delta_text` 范例，以及本模块 `runner.py::extract_content` 的既有写法。

I/O 部分（真正发 HTTP 请求的同步函数）用 `monkeypatch` 替换模块级 `requests` 引用来测试，不起真实服务器。类型注解遵守 `&lt;3.11` 约束（`typing.Optional/Dict/...`）。

## 3. 并发模型设计

### 3.1 为什么用 `requests`（同步）而不是异步 HTTP 库

`requests.Response.iter_lines()` 是阻塞式同步生成器，`aiohttp`/`httpx` 才有原生异步流式 API——但引入任一个都违反"零新增依赖"。折中方案：把"POST + 完整消费流式响应"整体写成一个**同步**函数 `_http_single_request_sync(...)`，通过 `loop.run_in_executor(executor, func, *args)` 丢进线程池执行，对外仍是一个可 `await` 的协程。

**Python 3.8 兼容性约束**：`asyncio.to_thread`（3.9+ 才有）不能用，必须用 `loop.run_in_executor`——它不接受关键字参数，需要 `functools.partial` 包一层。

### 3.2 线程池尺寸：不依赖全局默认 executor

`asyncio` 的默认线程池执行器容量是 `min(32, os.cpu_count()+4)`。如果用户传 `--num-concurrent-requests 50 --api-base ...`，全局默认 executor 会成为隐性瓶颈（超过 32 的请求排队等线程，压测测出的是线程池限流而非目标端点的真实并发承载）。

**方案**：`run_benchmark` 在检测到 `config.api_base is not None` 时，创建一个专属的 `concurrent.futures.ThreadPoolExecutor(max_workers=config.num_concurrent_requests)`，通过 `loop.run_in_executor(executor, ...)` 使用，运行结束后 `executor.shutdown(wait=False)`。闭环/开环两条调度路径都通过它（因为都只是在调用 `_single_request`，改动不外溢到调度层）。

### 3.3 请求体构造去重（DRY）

现有 `_single_request` 内联构造 `body` dict（model/messages/stream/max_tokens/timeout + `additional_sampling_params` 合并）。本次抽出共享纯函数：

```python
def _build_body(config: BenchmarkConfig, prompt_text: str, sampled_max_tokens: int) -> Dict[str, Any]:
    """两条后端（litellm / HTTP 直连）共用的请求体构造，纯函数、不含 timeout（各自处理方式不同）。"""
```

litellm 路径在其基础上加 `"timeout": ...` 字段直接传给 `acompletion`；HTTP 路径把它转成 `requests.post(..., timeout=...)` 的调用参数，不写进 JSON body（这是两条路径本质不同之处：HTTP 层超时 vs. SDK 参数）。

### 3.4 `_single_request` 变为薄分发层

```python
async def _single_request(config, prompt_text, num_input_tokens, sampled_max_tokens, request_timeout=None) -> Dict[str, Any]:
    if config.api_base is not None:
        loop = asyncio.get_event_loop()
        fn = functools.partial(
            http_client.single_request_sync,
            config, prompt_text, num_input_tokens, sampled_max_tokens, request_timeout,
        )
        return await loop.run_in_executor(_get_http_executor(config), fn)
    # 原 litellm 分支，逻辑不变
    ...
```

`http_client.single_request_sync` 自身内部做完整的 try/except（网络异常、非 200、SSE 解析异常都不外抛，直接返回带 `error_code`/`error_msg` 的结果字典）——与 A2"异常绝不逃逸"的既有契约保持一致，但实现上是同步函数自洽，不依赖调用方再包一层 try。

### 3.5 SSE 解析：复用踩过的坑（H3 空 choices 帧）

```python
def parse_sse_line(raw_line: bytes) -> Optional[Dict[str, Any]]:
    """'data: {...}' -> dict；'data: [DONE]'/空行/非 data: 行 -> None。"""

def extract_delta_text(data: Dict[str, Any]) -> Optional[str]:
    """从解析出的 JSON dict 里取 choices[0].delta 的 content/reasoning_content；
    choices 为空列表（include_usage 末帧）时安全返回 None，不做 [0] 下标（H3 教训）。"""
```

两者都是纯函数，直接単测，不需要 mock `requests`。

## 4. 数据模型与指标：零改动

`http_client.single_request_sync` 返回的字典与 `_single_request` 现有返回值**同形**（`TTFT`/`E2E_LAT`/`NUM_INPUT_TOKENS`/`NUM_OUTPUT_TOKENS`/`ERROR_CODE`/`ERROR_MSG`），`derive_request_metrics`/`summarize` 完全不感知后端差异，不用改一行。`NUM_OUTPUT_TOKENS` 复用 `litellm.token_counter(model=config.model, text=generated_text)`——即使目标模型不被 litellm 识别为已知 provider，`token_counter` 对未知模型仍有 `cl100k_base` 兜底编码器，计数口径与 litellm 路径完全一致（DRY，无需引入第二套分词逻辑）。

`BenchmarkConfig` 新增两个可选字段：

```python
api_base: Optional[str] = None      # None=走 litellm（默认）；设置后走 HTTP 直连
api_key_env: str = "OPENAI_API_KEY" # 从该环境变量读 Bearer token；未设置时不发 Authorization 头（非致命，仅告警）
```

## 5. CLI 扩展

- `--api-base`，`type=str`，`default=None`。不传 = 现状不变。
- `--api-key-env`，`type=str`，`default="OPENAI_API_KEY"`——只在 `--api-base` 生效时才被读取；对应环境变量未设置时打印一条 stderr 告警（`warning: env var '...' not set, sending request without Authorization header`）后继续，而不是像 Ray 路线的 openai 客户端那样硬报错退出——本模式的典型场景是本地/内网自建端点，很多压根不做鉴权，不应强制要求 key。
- summary 回显：仅当 `--api-base` 被传时写入 `summary["api_base"]` 与 `summary["backend"] = "http"`；不传时 `summary["backend"] = "litellm"`（始终写入，供下游区分口径，属于新增键，不影响既有读取脚本按键取值）。

## 6. 测试矩阵

| 编号 | 测试 | 守护 | 文件 |
|---|---|---|---|
| T1 | `parse_sse_line`：合法 `data:` 行、`[DONE]`、空行、非 `data:` 行（如 `: ping`）四种输入 | SSE 分帧 | `test_benchmark_http_client.py` |
| T2 | `extract_delta_text`：content/reasoning_content 优先级、`choices=[]` 安全返回 None（H3 同款） | delta 提取 | `test_benchmark_http_client.py` |
| T3 | `_build_body`：两条路径共用，字段与现状 litellm body 逐键一致（回归安全网） | DRY 重构不变形 | `test_benchmark_runner.py` |
| T4 | `single_request_sync` happy path：monkeypatch `http_client.requests.post` 返回假流式 response，断言 ttft&gt;0、文本拼接、token 计数 | 主路径 | `test_benchmark_http_client.py` |
| T5 | `single_request_sync` 非 200：断言 `error_code`=状态码、`error_msg`=响应体文本，不抛出 | 错误捕获 | `test_benchmark_http_client.py` |
| T6 | `single_request_sync` 网络异常（`requests.exceptions.ConnectionError`）：转失败记录不逃逸 | A2 同款契约 | `test_benchmark_http_client.py` |
| T7 | `single_request_sync` 空流：`error_code == "empty_response"`（A3 同款语义） | 口径一致 | `test_benchmark_http_client.py` |
| T8 | `runner._single_request(config_with_api_base, ...)`：断言 `acompletion` 未被触及、HTTP 路径被调用（隔离测试） | 分发正确 | `test_benchmark_runner.py` |
| T9 | `run_benchmark` 端到端（闭环 + 开环各一次）配合假 HTTP 后端：全部完成，管线零改动可用 | 集成 | `test_benchmark_runner.py` |
| T10 | 专属线程池：慢速假 handler 下峰值并发 == `num_concurrent_requests`（不被全局默认 executor 封顶在 32），对齐 `test_run_benchmark_concurrency_stays_at_n` 的验证手法 | 线程池尺寸 | `test_benchmark_runner.py` |
| T11 | CLI：`--api-base` 接线进 config；`--api-key-env` 缺失时 stderr 告警但不退出；summary 含 `backend`/`api_base` 键，不传 `--api-base` 时 `backend=="litellm"` 且无 `api_base` 键 | 接线与回显 | `test_benchmark_cli.py` |

## 7. 影响面与兼容性

- **litellm 路径（默认）**：`_single_request` 重构为"共享 body 构造 + 分发"，但对外行为、summary/individual JSON 键集合与形状**完全不变**，既有 70 例测试不得回退。
- **HTTP 直连（新）**：summary 新增 `backend`（始终写入）与 `api_base`（仅设置时写入）两键；individual 记录形状不变（无新增字段）。
- **依赖**：零新增（`requests` 已是运行时依赖；`functools`/`concurrent.futures` 为标准库）。
- **已知限制（文档需写明）**：仅支持 OpenAI 兼容流式 `/chat/completions` 协议；线程池并发上限与 `--num-concurrent-requests` 直接挂钩，过大的值会产生等量的操作系统线程（压测工具本身的资源开销，需用户知悉）。
