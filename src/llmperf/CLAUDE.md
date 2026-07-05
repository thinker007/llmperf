[根目录](../../CLAUDE.md) > [src](../) > **llmperf**

## 模块职责

`src/llmperf` 是整个压测框架的核心库：定义统一的请求数据结构（`RequestConfig`）、统一的客户端接口（`LLMClient`）、指标常量命名、请求调度器（基于 Ray `ActorPool`）以及一批工具函数（prompt 采样、结果落盘、S3 上传）。它本身不直接发 HTTP 请求——具体的厂商协议实现在子包 `ray_clients/`（见其独立文档）。

本模块文档同时覆盖两个**物理上位于仓库根目录、但在逻辑上是本包唯一消费者**的入口脚本：`token_benchmark_ray.py`（性能压测主循环）与 `llm_correctness.py`（正确性校验主循环）。它们把本包的各个组件粘合成可执行的 CLI 工具。

## 入口与启动

- `token_benchmark_ray.py`：性能压测入口。`argparse` 定义 CLI 参数 → `run_token_benchmark()` → `get_token_throughput_latencies()`（核心压测循环）→ `metrics_summary()`（用 pandas 计算分位数）→ 落盘为 `<model>_<in>_<out>_summary.json` 和 `..._individual_responses.json`。
- `llm_correctness.py`：正确性校验入口。`argparse` → `run()` → `llm_correctness()`（把随机数转英文单词发给模型，核对模型返回的数字是否一致）→ 落盘 `<model>_correctness_summary.json`。
- 两者都在 `if __name__ == "__main__":` 里先 `ray.init(runtime_env={"env_vars": dict(os.environ)})`——**这一步是必需的**：Ray Actor 运行在独立进程中，不会自动继承父进程环境变量（如 `OPENAI_API_KEY`），必须显式通过 `runtime_env` 传递，否则客户端会因读不到密钥而报错。

## 对外接口

### `models.py::RequestConfig`（pydantic `BaseModel`）
```python
class RequestConfig(BaseModel):
    model: str
    prompt: Tuple[str, int]              # (prompt文本, 该文本的 token 数)
    sampling_params: Optional[Dict[str, Any]] = None
    llm_api: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
```
这是贯穿全链路的核心数据结构，所有客户端、调度器、压测循环都围绕它传递数据。

### `ray_llm_client.py::LLMClient`（抽象基类，所有客户端插头的"接口形状"）
```python
class LLMClient:
    @abc.abstractmethod
    def llm_request(self, request_config: RequestConfig) -> Tuple[Dict[str, Any], str, RequestConfig]:
        """返回 (性能指标字典, 生成的文本, 原始请求配置)"""
```
**坑点**：`LLMClient` 是普通类而非 `abc.ABCMeta` 元类，`@abc.abstractmethod` 装饰器**不会**在实例化时强制报错——它只是一个约定/文档标记。子类若忘记实现 `llm_request` 不会在导入期或实例化期报错，只会在真正调用时抛 `NotImplementedError` 风格的错误（实际上因为没有 ABCMeta，甚至可以被静默跳过，取决于 Python 版本行为）。新增客户端时务必手动核对方法签名一致，不要依赖运行时强制检查。

### `common.py::construct_clients(llm_api, num_clients) -> List[LLMClient]`
按 `llm_api` 字符串路由到具体客户端类并批量创建 Ray Actor：
```python
SUPPORTED_APIS = ["openai", "anthropic", "litellm"]

if llm_api == "openai": OpenAIChatCompletionsClient
elif llm_api == "anthropic": AnthropicMessagesClient
elif llm_api == "sagemaker": SageMakerClient
elif llm_api == "vertexai": VertexAIClient
elif llm_api in SUPPORTED_APIS: LiteLLMClient   # 实际只有 "litellm" 会落到这支
else: raise ValueError(...)
```
**已知坑点**：`SUPPORTED_APIS` 常量只列了 3 个值，但 `construct_clients` 实际支持 5 种 `llm_api`（`sagemaker`、`vertexai` 未被收录）。这个常量同时被用作 CLI `--help` 文案（`token_benchmark_ray.py`/`llm_correctness.py` 里 `f"...{SUPPORTED_APIS}"`），因此**帮助文本会漏掉 sagemaker/vertexai 两个实际可用的选项**，容易误导用户。修改支持的 API 列表时，要同步检查这个常量与 `elif` 链是否一致。

### `requests_launcher.py::RequestsLauncher`
封装 `ray.util.ActorPool`，提供：
- `launch_requests(request_config)`：仅当池中有空闲 Actor 时才提交任务（`has_free()` 判断），否则本次调用是空操作——调用方需要自己重试。
- `get_next_ready(block=False)`：`block=False` 时只捞取"已经就绪"的结果（可能为空列表）；`block=True` 时会用**忙等 `while not has_next(): pass`** 阻塞直到至少有一个结果就绪，然后一次性捞干所有就绪结果。

**坑点（对应 git log 中的 #59 修复）**：`launch_requests` 的"仅在有空闲 Actor 时提交"设计，与压测循环里"先 launch 再 get_next_ready，不 block"的组合方式，如果调用节奏不对，容易出现"并发数塌陷"——即所有 Actor 都在处理慢请求时，无法提交新任务，直到有一个恰好完成。`token_benchmark_ray.py` 目前通过多线程（每个线程一个独立的 `RequestsLauncher` + 单 Actor）来规避这个问题（见下）。`src/llmperf/benchmark/` 的设计文档中专门用 worker-pool 模型再次分析并修正了这类"批处理语义"缺陷。

### `common_metrics.py`
纯字符串常量模块，定义所有指标的字典 key 名（如 `TTFT = "ttft_s"`、`INTER_TOKEN_LAT = "inter_token_latency_s"`）。所有客户端、聚合逻辑都从这里 import 常量而不是硬编码字符串（DRY）。

### `utils.py`
- `LLMPerfResults`：结果落盘的包装类，自动打时间戳、`flatten_dict()` 展平嵌套字典后转 JSON。
- `randomly_sample_sonnet_lines_prompt(...)`：按目标 token 数（用固定的 `LlamaTokenizerFast` 计数）从 `sonnet.txt`（莎士比亚十四行诗语料）随机拼接出一段 prompt，通过截断最后一行来精确控制 token 数。**采用统一分词器**是为了让不同厂商 API 在"相同 prompt token 数"这个前提下公平对比——即便厂商各自的分词器不同。
- `sample_random_positive_int(mean, stddev)`：从高斯分布重采样直到取到正数（因为高斯分布可能采出负数，而 token 数不能为负）。
- `flatten_dict`：递归展平嵌套字典，`a.b.c` 形式的 key。
- `upload_to_s3`：调用 `aws s3 sync` CLI 命令（子进程），非 boto3 API 调用。

## 关键依赖与配置

- `ray`：Actor 并发模型的基础设施。
- `pydantic<2.5`：`RequestConfig` 的数据校验（注意版本上限，pydantic 2.5+ 的行为变化可能不兼容）。
- `transformers`（`LlamaTokenizerFast`）：固定分词器，压测循环和 `utils.py` 都依赖它统计 token 数——**这是一个较重的依赖**，`src/llmperf/benchmark/` 正是为了摆脱它才引入 `litellm.token_counter` 方案。
- `pandas`：仅用于 `token_benchmark_ray.py::metrics_summary()` 计算分位数（`quantile([0.25,0.5,0.75,0.9,0.95,0.99])`）。
- 环境变量：`ray.init()` 前必须把当前进程的环境变量通过 `runtime_env={"env_vars": ...}` 传给 Ray，否则各客户端读不到 API Key。

## 数据模型

```
RequestConfig (pydantic)
  ├─ model: str
  ├─ prompt: (text: str, token_len: int)
  ├─ sampling_params: dict | None      # 会被各客户端 body.update()，如 max_tokens/temperature
  ├─ llm_api: str | None               # 路由用，同时被部分客户端用于校验/拼接 model 名
  └─ metadata: dict | None             # 仅用于日志/正确性校验的期望值携带

llm_request() 返回值:
  (metrics: Dict[str, Any], generated_text: str, request_config: RequestConfig)
  metrics 的 key 全部来自 common_metrics.py 的常量
```

数据流全貌（性能压测路径）：
```
token_benchmark_ray.py
  → randomly_sample_sonnet_lines_prompt() 预生成全部 prompt（压测开始前，避免采样开销计入延迟）
  → 每个并发线程持有一个 RequestsLauncher(单 Actor)
  → RequestConfig 组装（model/prompt/sampling_params 含 max_tokens/llm_api）
  → req_launcher.launch_requests() → Actor.llm_request.remote()
  → req_launcher.get_next_ready() 取回 (metrics, generated_text, request_config)
  → 用 LlamaTokenizerFast 对 generated_text 重新计数，覆盖客户端自己数的 tokens_received
    （只有 Anthropic/OpenAI/LiteLLM 客户端逐 delta 自增的计数会被这一步覆盖；
     SageMaker/VertexAI 客户端本身就已经用 Llama tokenizer 计数）
  → 重算 INTER_TOKEN_LAT / NUM_TOTAL_TOKENS / REQ_OUTPUT_THROUGHPUT
  → metrics_summary() 用 pandas 算分位数 + 错误率
  → LLMPerfResults 包装 + flatten_dict + json.dump 落盘
```

## 测试与质量

`src/llmperf` 本体（`RequestConfig`/`RequestsLauncher`/`common.py` 路由逻辑/`utils.py` 采样与展平函数）目前**没有直接针对它的单元测试**——`tests/` 下现有的测试文件分别覆盖 `ray_clients/anthropic_messages_client.py` 和 `benchmark/` 子模块（`test_benchmark_{prompts,metrics,runner,cli}.py`，共 26 个用例，`benchmark/` 已完工，见其独立文档）。`common.py::construct_clients()` 的路由分支和 `utils.py::flatten_dict`/`sample_random_positive_int` 仍是本模块最大的测试缺口，建议优先补充纯函数单测（不需要 Ray runtime）。

## 常见问题 (FAQ)

- **Q: 为什么 `mean_input_tokens < 40` 会打印警告但不报错？**
  A: `token_benchmark_ray.py` 中硬编码了一个提示（"最小发送 41 个 token"），因为 prompt 采样逻辑有一个固定的前缀模板本身就要占用若干 token，均值过小会导致重采样循环耗时变长，但代码并未真正阻止运行。
- **Q: 为什么 `RequestsLauncher.get_next_ready(block=True)` 用 `while not has_next(): pass` 忙等？**
  A: 这是一个 CPU 忙轮询而非事件驱动等待，短时间内不是问题，但如果长时间没有结果就绪会空转消耗 CPU。目前仅 `README.md` 的"Advanced Usage"示例代码里用到 `block=True`，主压测循环用的是非阻塞模式。
- **Q: `additional_sampling_params` 和 `sampling_params` 是什么关系？**
  A: 压测循环里先构造 `default_sampling_params = {"max_tokens": 采样值}`，再用 CLI 传入的 `--additional-sampling-params`（JSON 字符串）覆盖/追加，最终整体作为 `RequestConfig.sampling_params` 传给客户端，客户端再 `body.update(sampling_params)` 合并进请求体。这个"口袋参数"设计让厂商特有参数（如 Anthropic 的 `thinking`）无需在框架层加字段即可透传。

## 相关文件清单

- `src/llmperf/__init__.py`（空）
- `src/llmperf/models.py`
- `src/llmperf/common.py`
- `src/llmperf/common_metrics.py`
- `src/llmperf/ray_llm_client.py`
- `src/llmperf/requests_launcher.py`
- `src/llmperf/utils.py`
- `src/llmperf/sonnet.txt`（采样语料，供 `utils.py` 使用；`benchmark/sonnet.txt` 是它的副本）
- `token_benchmark_ray.py`（仓库根目录）
- `llm_correctness.py`（仓库根目录）

## 变更记录 (Changelog)

- 2026-07-05：首次生成本模块文档。
