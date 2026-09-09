# llmperf 新贡献者上手指南

> 面向「要改代码」的人：项目地图、核心契约、最常见的扩展任务（加一个厂商）、测试怎么写、编码红线。
> 实现级的逐模块细节见 `docs/code-summary.md`；本文只讲**从哪下手、怎么不踩雷**。生成：2026-07-24，核对：2026-07-25。

---

## 1. 五分钟项目地图

代码总量约 2.6k 行，两条并行路线共享一个核心框架：

```
llmperf/
├── token_benchmark_ray.py     # Ray 路线入口（性能压测，497 行）
├── llm_correctness.py         # Ray 路线入口（正确性校验，309 行）
├── src/llmperf/
│   ├── models.py              # RequestConfig 数据模型（统一入参）
│   ├── common_metrics.py      # 指标 key 常量（两路线共用）
│   ├── ray_llm_client.py      # ★ LLMClient 抽象基类（全仓最重要的契约）
│   ├── common.py              # ★ 客户端注册表 + 工厂（加厂商的落点）
│   ├── requests_launcher.py   # Ray ActorPool 调度器
│   ├── utils.py               # Ray 路线采样/落盘/分词器
│   ├── ray_clients/           # 5 个厂商客户端（各实现 llm_request）
│   │   ├── openai_chat_completions_client.py
│   │   ├── anthropic_messages_client.py   # 原生 /v1/messages，纯函数范例
│   │   ├── litellm_client.py              # 万能兜底 100+ 家
│   │   ├── sagemaker_client.py
│   │   └── vertexai_client.py
│   └── benchmark/             # 轻量 asyncio 路线（无 Ray）
│       ├── __main__.py        # CLI 入口
│       ├── runner.py          # asyncio worker-pool 并发引擎（核心，311 行）
│       ├── http_client.py     # HTTP 直连模式
│       ├── prompts.py         # prompt 采样
│       └── metrics.py         # 指标聚合（纯函数）
└── tests/                     # 全部纯函数级单测，不连网、不起 Ray
```

**两个 `★` 文件是你改任何东西前都该先读的。** 下面详述。

---

## 2. 唯一必须记住的契约

`src/llmperf/ray_llm_client.py` 定义了所有 Ray 客户端的统一接口：

```python
class LLMClient:
    @abc.abstractmethod
    def llm_request(
        self, request_config: RequestConfig
    ) -> Tuple[Dict[str, Any], str, RequestConfig]:
        # 返回 (指标 dict, 生成文本, 原始 request_config) —— 必须是这个三元组
        ...
```

**改任何客户端前先读它。** 调度层（`RequestsLauncher`）只认这个三元组，返回别的东西会在下游炸掉。

配套 `src/llmperf/common.py` 是「开放/封闭原则」的落地点——一个注册表把 API 名映射到类：

```python
_CLIENT_REGISTRY = {
    "openai":    OpenAIChatCompletionsClient,
    "anthropic": AnthropicMessagesClient,
    "sagemaker": SageMakerClient,
    "vertexai":  VertexAIClient,
    "litellm":   LiteLLMClient,
}
SUPPORTED_APIS = list(_CLIENT_REGISTRY)   # 支持列表由注册表派生，杜绝漂移
```

注册表是**单一真相源**：「支持哪些 API」和「怎么构造」出自同一处，历史上曾因两处不同步导致「报错声称支持某 API，实际没建」的 bug——现在不可能再发生。

---

## 3. 最常见任务：新增一个 LLM 厂商

标准流程只碰两个文件、零改动既有代码：

**Step 1** — 在 `src/llmperf/ray_clients/` 下新建 `foo_client.py`，实现 `LLMClient` 子类：

```python
import ray
from llmperf.ray_llm_client import LLMClient
from llmperf.models import RequestConfig

@ray.remote
class FooClient(LLMClient):
    def llm_request(self, request_config: RequestConfig):
        metrics = {}
        generated_text = ""
        # 1. 发流式请求
        # 2. 逐帧累计：首帧记 TTFT，之后每帧记 inter-token 间隔
        # 3. 任何异常都落到 metrics[ERROR_CODE]/[ERROR_MSG]，不要让它逃逸
        return metrics, generated_text, request_config
```

**Step 2** — 在 `src/llmperf/common.py::_CLIENT_REGISTRY` 加一行：

```python
"foo": FooClient,
```

完成。`--llm-api foo` 即可用，`SUPPORTED_APIS` 自动包含它。

### 写客户端的三条范式（照抄 `anthropic_messages_client.py`）

1. **把协议解析抽成纯函数**。SSE 分帧这类易错逻辑抽成不依赖 Ray/网络的纯函数（如 `parse_data_line` / `extract_delta_text`），这样能直接用 pytest 断言，不用起 Ray。**这是全仓贯穿的可测性设计。**
2. **异常绝不逃逸**。单请求任何环节出错就地转成失败记录（写 `ERROR_CODE`/`ERROR_MSG`），一条坏请求不能拖垮整个池。
3. **防御式边界**。空 `choices`、除零、空流都要有守卫——参考 `runner.py` 里的 `if not tok.choices: continue`。

---

## 4. 测试怎么写

- 框架 `pytest`，**全部是纯函数级单测**——不连真实网络、不起 Ray runtime，用 `monkeypatch` 替换外部依赖（如 `litellm.token_counter`）。
- 覆盖重心 = 最易出 bug 又难对 Actor 直接测的部分：**SSE 分帧、prompt 采样、指标聚合**。所以写新逻辑时，先想「能不能抽成纯函数」，能抽就能测。
- **没装 `pytest-asyncio`**：异步测试约定在普通同步 `def test_...()` 里用 `asyncio.run()` 驱动（零新增依赖），别 `import pytest_asyncio`。

跑测试：

```bash
python -m pytest tests/ -v
```

> ⚠️ 环境提示（2026-07-25）：当前 `.venv` 是 `pydantic 2.4.2 + litellm 1.73.6`，不兼容会让依赖 litellm 的 4 个 benchmark 测试文件 import 失败（收集期报错），只有 18 例核心框架测试能跑。修法：`pip install 'pydantic>=2.5,<3'` 或 `pip install 'litellm==1.61.13'`。修好后应全绿（91 个测试函数）。

现状分布：`tests/` 下 10 个文件——`test_benchmark_runner.py`(31) / `test_benchmark_metrics.py`(16) / `test_benchmark_cli.py`(12) / `test_benchmark_http_client.py`(10) / `test_anthropic_messages_client.py`(7) / `test_benchmark_prompts.py`(5) / `test_utils.py`(4) / `test_requests_launcher.py`(3) / `test_common.py`(2) / `test_token_benchmark_ray.py`(1)。

---

## 5. 编码红线

| 红线 | 原因 |
|---|---|
| **不准用 Python 3.10+ 语法**（`list[str]`、`X \| None`），一律 `typing.List/Dict/Tuple/Optional` | `pyproject.toml` 硬约束 `>=3.8,<3.11`，尤其 `benchmark/` 内 |
| **`benchmark/runner.py` 禁止 `asyncio.gather` 批处理**，必须 worker-pool | 历史上 Ray 版因批处理语义踩过上游 #59 bug（「一批全完成才能发下一批」），约束刻在注释里 |
| **提交前跑 `black`** | `bash pre-commit.sh`，无自定义配置走默认规则 |
| **改指标口径前先确认改的是哪条路线** | 两路线 ITL/token 口径已知不一致，混改会污染报告 |

---

## 6. 开发流程：TDD（先写复现测试）

仓库约定 **Red → Green → 格式化 → commit**：

1. **Red**：先写一个能复现 bug / 表达新需求的测试，确认它失败。
2. **Green**：写最小实现让它过。
3. **格式化**：`bash pre-commit.sh`。
4. **commit**。

报 bug 时也走这条路：**先写复现测试，再修**。`docs/superpowers/plans/` 里的实施计划都是这个模式。

---

## 7. 两条路线的关系（别改错地方）

| | Ray 路线 | benchmark 路线 |
|---|---|---|
| 并发原语 | Ray Actor + `ActorPool` + `threading.Thread` | 纯 asyncio worker-pool |
| 加厂商 | 上面 §3 的注册表流程 | 直接改 `runner.py` 的请求体构造（litellm/HTTP 两后端） |
| token 计数 | 固定 Llama 分词器 | `litellm.token_counter`（随模型变） |
| 依赖 | ray / pandas / transformers | 零新增（仅 litellm + requests） |

两条路线**零侵入并存**：共享指标常量、独立运行，改一条不动另一条。加冷门厂商走 Ray 路线的注册表；优化轻量/开环/直连能力走 benchmark 路线。

---

## 8. 已知待办（审计发现，已全部修复）

`docs/code-summary.md` §10–11 记了 Ray 老路线逐行审计的 5 处潜伏问题（发现 A-E），**均已按 TDD（先写复现测试再修）修复并入仓**，可作为"如何给这个仓库提 TDD 修复 PR"的参考范例：

- ~~**`llm_correctness.py:132`**：`error_rate = num_errored / num_completed_requests` 无零保护，超时且零完成时 `ZeroDivisionError`。~~ 已修（发现 C，加零保护守卫）。
- ~~Ray 路线 ITL 除数在主循环与收尾段口径不一致（`token_benchmark_ray.py` 正常/收尾两路除数不同）。~~ 已修（发现 A，抽取共享的 `finalize_request_metrics()`）。
- ~~`llm_correctness()` 默认 `llm_api="chat"` 不在客户端注册表里。~~ 已修（发现 D，默认值改 `"openai"`）。
- ~~结果横幅 `print(f"\Results ...")` 含非法转义 `\R`。~~ 已修（发现 E，改为 `\n`）。

新的练手起点：查 `docs/code-summary.md` §12 的上游 open issue 交叉比对表，挑一条本 fork 尚未覆盖的功能诉求（如 #92 自定义分词器路径）动手实现。

---

## 9. 相关文档

- **实现级逐模块导读**（每段代码做什么/为什么/坑在哪 + Ray 路线逐行审计 + 上游调研）：`docs/code-summary.md`
- **使用者速览**（能测什么、命令怎么敲、输出怎么读）：`docs/overview.md`
- **架构索引**（面向 AI）：根 `CLAUDE.md` 及各子模块 `CLAUDE.md`
- **benchmark 路线用户手册**：`docs/benchmark.md`
- **设计规格与实施计划**：`docs/superpowers/specs/` 与 `docs/superpowers/plans/`
