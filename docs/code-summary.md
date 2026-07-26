# llmperf 代码总结

> 面向开发者的代码级导读：逐模块讲清"每段代码做什么、为什么这么写、坑在哪"。
> 生成日期：2026-07-24，**最近核对：2026-07-26**。配套阅读：架构索引见根 `CLAUDE.md`，各子模块 `CLAUDE.md`；用户手册见 `README.md`、`docs/benchmark.md`。

> ✅ **环境状态（2026-07-26 复核，已恢复健康）**：当前 `.venv` 为 **pydantic 2.4.2 + litellm 1.61.13**（兼容组合），全仓 **113 例全绿**。此前一度出现的 pydantic/litellm 冲突（litellm 曾被升到 1.73.x，依赖 `pydantic.Discriminator` 而 pydantic 2.4.2 无此符号，导致 `import litellm` 失败、benchmark 测试无法收集）已通过两步修复：① `pyproject.toml` 给 litellm 补上界 `litellm>=0.1.738,<1.62`（因 litellm≥1.62 需 pydantic≥2.5）；② venv 回退 litellm 到 1.61.13。**依赖约束已锁进 `pyproject.toml`，此坑不会再复现。**
>
> ⚠️ **工作区当前存在未提交改动**：`src/llmperf/benchmark/runner.py` 的 `BenchmarkConfig.num_warmup_requests` 字段与 warmup 相调度逻辑仍在，但 `__main__.py` 的 `--num-warmup-requests` CLI 参数、`docs/benchmark.md` 的对应说明、`tests/test_benchmark_cli.py` 的 CLI 级用例均已在工作区中被撤销——即 warmup 目前只能通过直接构造 `BenchmarkConfig(num_warmup_requests=N)` 以库方式使用，CLI 用户不可见也无法设置。这看起来像一次未完成/未提交的功能回退，建议向仓库所有者确认是否有意为之。

---

## 1. 一句话定位

llmperf 是一把量化 LLM API **性能**（TTFT 首字延迟、ITL 逐字延迟、E2E 端到端延迟、吞吐量、错误率）与**正确性**（数字↔英文单词往返校验）的压测尺。核心设计是"一个插座配多种插头"：调度逻辑与厂商 API 差异严格隔离，新增厂商只加一个客户端、不改既有代码（开放/封闭原则）。

## 2. 两条并行技术路线

| | Ray 路线（成熟） | benchmark 路线（轻量，已完工+硬化） |
|---|---|---|
| 入口 | `token_benchmark_ray.py` / `llm_correctness.py`（仓库根） | `python -m llmperf.benchmark` |
| 并发原语 | Ray Actor + `ActorPool` + `threading.Thread` | 纯 `asyncio` worker-pool |
| 发压对象 | 5 个厂商专用 Ray 客户端 | `litellm.acompletion()` 或 HTTP 直连 |
| 依赖 | ray / pandas / transformers | 零新增依赖（仅 litellm + requests） |
| token 计数 | 固定 Llama 分词器（跨模型口径统一） | `litellm.token_counter` |

**两条路线指标口径不完全一致**（尤其 ITL 公式，见 §6），跨路线不要直接对比数字。

---

## 3. 核心框架 `src/llmperf/`

被两条路线共享的地基，全部是小而稳的纯逻辑。

### `models.py` — 请求数据模型
`RequestConfig(BaseModel)`：`model` / `prompt: Tuple[str,int]`（文本+token 数）/ 可选 `sampling_params` / `llm_api` / `metadata`。pydantic 模型，是所有客户端的统一入参。

### `common_metrics.py` — 指标常量表
一组字符串常量（`TTFT="ttft_s"`、`INTER_TOKEN_LAT`、`E2E_LAT`、`OUTPUT_THROUGHPUT`、`ERROR_CODE`、`SCHED_DELAY`、`ACHIEVED_REQUEST_RATE`…）。**两条路线共用同一套 key 命名**，保证落盘 JSON 字段一致。

### `ray_llm_client.py` — 统一接口契约
`LLMClient` 抽象基类，唯一抽象方法 `llm_request(request_config) -> (metrics, generated_text, request_config)` **三元组**。所有 Ray 客户端必须实现并返回这个三元组——这是全仓库最重要的契约，改任何客户端前先读它。

### `requests_launcher.py` — 调度器
`RequestsLauncher` 包一层 Ray `ActorPool`：
- `launch_requests()`：池有空闲才 `submit`。
- `get_next_ready(block)`：**关键修复**——`block=True` 但池中无在途请求时直接 `raise StopIteration`，而不是空转死等（注释解释 `has_next()` 报的是"有无在途"而非"有无就绪"，空池 blocking 会永久自旋）。

### `common.py` — 客户端工厂（开放/封闭的落地点）
`_CLIENT_REGISTRY` 字典把 API 名映射到客户端类，`SUPPORTED_APIS = list(_CLIENT_REGISTRY)` 由注册表派生。`construct_clients()` 查表失败即报错列出支持项。**用注册表统一了"支持列表"与"构造分支"两处真相**，杜绝了错误信息声称支持一个代码其实没建的 API（历史漂移 bug）。新增厂商=注册表加一行。

### `utils.py` — 工具函数（Ray 路线专用采样/落盘）
- `_get_default_tokenizer()`：**惰性加载**并缓存 Llama 分词器，避免 import 期就触发网络/副作用。
- `randomly_sample_sonnet_lines_prompt()`：按目标 token 数从莎士比亚十四行诗随机采样拼 prompt。
- `sample_random_positive_int()`：高斯采样直到得正整数；`mean<=0` 时**快速失败**（否则 `stddev=0` 必死循环、`stddev>0` 概率死循环）。
- `LLMPerfResults` / `flatten_dict` / `upload_to_s3`：结果封装、嵌套字典扁平化、S3 上传。

> **两处 clamp 修复同源**：`stddev=0` 时 `gauss(mean,0)` 是定值，"重采样直到超过基底前缀长度"的循环永远越不过去 → 死循环。`utils.py` 与 `benchmark/prompts.py` 都用 `max(采样值, 基底长度)` clamp 化解。

---

## 4. Ray 客户端 `src/llmperf/ray_clients/`

5 个 `@ray.remote` Actor，逐一实现 `llm_request` 三元组。以原生 Anthropic 客户端为范例说明共性：

### `anthropic_messages_client.py`（原生 `/v1/messages`，非借道 litellm）
- **纯函数抽离**：`parse_data_line`（从 SSE `data:` 行取 JSON，跳过 `event:`/`: ping`/空行）和 `extract_delta_text`（只认 `content_block_delta`，同时处理 `text_delta` 与推理模型的 `thinking_delta`，让首个 thinking token 计入 TTFT）。这两个函数不依赖 Ray/网络，可直接用 pytest 断言——**这是本仓库反复出现的可测性策略**。
- 逐帧累计：首个 delta 记 TTFT，之后每帧记 inter-token 间隔；`type=="error"` 帧主动 `raise`；异常统一落到 `ERROR_CODE/ERROR_MSG`。
- 环境变量：`ANTHROPIC_API_KEY`（→ `x-api-key`）、`ANTHROPIC_API_BASE`（默认官方）、`ANTHROPIC_VERSION`。`max_tokens` 是 Messages API 必填，缺省兜底 512。

其余四个客户端：`openai_chat_completions_client`（OpenAI 兼容 SSE）、`litellm_client`（万能兜底，覆盖 100+ 家）、`sagemaker_client`、`vertexai_client`（后两家不回传 token 数，用 Llama 分词器本地计数）。细节见 `ray_clients/CLAUDE.md`。

---

## 5. 轻量 benchmark 子模块 `src/llmperf/benchmark/`

去掉 Ray/threading/pandas/transformers，用 asyncio 直压。三种发射方式（闭环 worker-pool / 开环泊松 / HTTP 直连）+ 完整 CLI。

### `prompts.py` — prompt/输出长度采样
从 `utils.py` 移植，分词器换成 `litellm.token_counter`。`sample_prompt` 返回的 token 数对**最终文本重算**（口径自洽），而非采样目标值。同款 `stddev=0` clamp 防死循环。

### `runner.py` — 并发引擎（核心）
```
BenchmarkConfig  ← 所有旋钮：模型/输入输出分布/并发/超时/
                    request_rate(开环开关)/api_base(HTTP直连开关)
_build_body      ← 两后端共用请求体构造
_single_request  ← 发一条流式请求，采集 TTFT/E2E/output；A2 契约：异常绝不外逃
_arrival_offsets ← 泊松到达时刻表（间隔~Exp(rate)）
_dispatch_at     ← 开环单发射：睡到到达时刻→抢在途名额→重查预算→发射
run_benchmark    ← 调度入口，闭环/开环二选一
```
关键设计与踩坑：
- **禁止 `asyncio.gather` 批处理**：明确采用 worker-pool（`num_concurrent_requests` 个 `worker()` 常驻消费队列）。历史上 Ray 版本因批处理语义踩过 #59 bug，这条约束刻在注释里。
- **空 choices 守卫**（H3）：`include_usage` 末帧/心跳帧带空 `choices`，越过 `choices[0]` 会抛 `IndexError` 把已成功请求整条误判失败——`if not tok.choices: continue` 化解。
- **错误码真实提取**：修正原 litellm_client 把 error_code 写死常量的"吞噬 bug"，从异常的 `status_code` 真实取值。
- **空流判 empty_response**（A3 有意语义变更）：`generated_text==""` 记 `empty_response` 错误。
- **单请求超时=剩余预算**（B2 有意语义变更）：`max(0, timeout - elapsed)`，而非每请求都给满超时。
- **到点不 cancel 在途**：worker 到超时只是"停止取新任务"，不打断已发出的请求。
- **开环 SCHED_DELAY**：调度延迟=拿到名额时刻−计划到达时刻，对所有已发射请求（含失败）记录，因为客户端排队与请求成败无关。

### `http_client.py` — HTTP 直连模式（绕开 litellm SDK）
`--api-base` 设置后直连 OpenAI 兼容 `/chat/completions`。
- 纯函数 `parse_sse_line` / `extract_delta_text` 可单测；`single_request_sync` 是唯一含 I/O 入口，**自洽处理全部异常，绝不外抛**（与 runner 的 A2 契约一致）。
- 故意保持**完全同步**：在 runner 分配的专属 `ThreadPoolExecutor` 里跑（尺寸对齐 `num_concurrent_requests`），不用 asyncio 默认 executor（其容量 `min(32, cpu+4)` 高并发下会成隐性瓶颈）。
- 同样带空 choices 守卫和 empty_response 判定。

### `metrics.py` — 聚合（纯函数，无网络）
- `derive_request_metrics`：补单请求派生指标。**ITL 口径 = `(e2e-ttft)/(out-1)`**（spec §7 决策 2，与 Ray 版不同——这是两路线口径差异的根源）。
- `summarize`：仅对成功请求算分位数（p25/50/75/90/95/99，`statistics.quantiles` inclusive 法）/均值/极值/标准差；错误按 code 计频；整体吞吐/错误率/完成速率。
- 除零统一口径 `_safe_div`（分母≤0 返回 `None`）；n=0/1 的分位数/标准差都有安全分支。
- 开环记录才写 `SCHED_DELAY`/`ACHIEVED_REQUEST_RATE` 块，闭环 summary 键集合逐字节不变（向后兼容）。

### `__main__.py` — CLI
argparse → 组装 `BenchmarkConfig` → `asyncio.run(run_benchmark)` → `summarize` → stdout/落盘。
- **参数即防线**：`parse_json_dict` 拒绝非 JSON 对象的 `--additional-sampling-params`（否则下游 `body.update()` 抛 `TypeError` 崩池，退出码 2）；`parse_metadata` 拒绝缺 `=` 的项；非正数值参数一律 `parser.error` 拦截（防采样死循环）。
- 内建汇总键（model/backend/timestamp/litellm_version…）优先，`--metadata` 撞键则告警忽略（B3）。
- 落盘两文件：`*_summary.json` + `*_individual_responses.json`，文件名两步正则清洗。

---

## 6. 指标口径差异（务必牢记）

| 指标 | Ray 路线 | benchmark 路线 |
|---|---|---|
| ITL | 客户端回传 `sum(time_to_next_token)`（≈E2E），入口再 **`/= 输出token数`** 得逐字均值 | `(e2e - ttft) / (out - 1)` **有意修正** |
| token 计数 | 固定 Llama 分词器 | `litellm.token_counter`（随模型变） |
| 空流 | 不特判 | 判 `empty_response` 错误 |
| prompt 随机性 | `random.seed(11111)` **可复现** | 未播种，逐次不同 |

改任何"指标口径"前，先确认改的是哪条路线。**Ray 侧 ITL 不是"间隔之和"本身**：客户端 `openai_chat_completions_client.py:135` 存的是间隔求和（注释直言"约等于 E2E"），真正的逐字延迟是在入口 `token_benchmark_ray.py:121` 再除以输出 token 数得到的——这条除法是口径的关键一步，且两条代码路径的**除数并不一致**（见 §10 发现 A）。

---

## 7. 测试策略

- 框架 `pytest`；**全部是纯函数级单测**——不连真实网络、不起 Ray，用 `monkeypatch` 替换外部依赖（如 `litellm.token_counter`）。
- 覆盖重心=最易出 bug 又难对 Actor 直接测的部分：SSE 分帧、prompt 采样、指标聚合。**把易错逻辑抽成纯函数**是全仓贯穿的可测性设计。
- 未装 `pytest-asyncio`：异步测试约定在普通同步 `def test_...()` 里用 `asyncio.run()` 驱动（零新增依赖）。
- 现状（2026-07-26 实测）：全仓 **99 个测试函数 / pytest 113 例全绿**（`tests/` 下 11 个测试文件，其中 `test_benchmark_runner.py` 37 个、`test_benchmark_metrics.py` 16 个、`test_benchmark_cli.py` 12 个、`test_benchmark_http_client.py` 10 个、`test_anthropic_messages_client.py` 7 个、`test_benchmark_prompts.py` 5 个、`test_utils.py` 4 个、`test_requests_launcher.py` 3 个、`test_llm_correctness.py` 2 个、`test_common.py` 2 个、`test_token_benchmark_ray.py` 1 个）。其中 `test_llm_correctness.py` 是此前为**发现 C（除零）**新增的复现+守护测试；`test_benchmark_cli.py` 比历史峰值少 2 个函数，系工作区撤销了 warmup 的 CLI 级测试（见文首 ⚠️ 提示）。

---

## 8. 编码约束

- **Python `>=3.8,<3.11` 硬约束**：`benchmark/` 内禁用 3.10+ 语法（`list[str]`、`X|None`），一律 `typing.List/Dict/Tuple/Optional`。
- 格式化统一 `black`（默认规则），提交前跑 `pre-commit.sh`。
- 新增厂商标准流程：`ray_clients/` 下实现 `LLMClient` 子类 + `@ray.remote`，返回三元组，再在 `common.py::_CLIENT_REGISTRY` 加一行。
- 易错协议解析抽成纯函数（`parse_data_line`/`extract_delta_text` 是范例）。
- 开发遵循 TDD：Red（先写复现测试）→ Green → 格式化 → commit。

---

## 9. 贯穿全仓的工程经验

1. **纯函数化易错逻辑** → 可单测、可复用、脱离 Ray/网络（SSE 解析、采样、聚合都这么做）。
2. **单一真相源** → 注册表统一"支持列表+构造分支"，指标常量统一 key 命名，杜绝漂移。
3. **异常绝不逃逸并池** → 单请求任何环节出错都就地转失败记录（A2 契约），一条坏请求不拖垮整池。
4. **防御式边界** → 空 choices 守卫、除零返 None、采样死循环 clamp、非法参数退出码 2 拦截。
5. **零侵入并存** → 新 asyncio 路线与旧 Ray 路线共享常量、独立运行，互不改动。

---

## 10. 深度调研：Ray 路线内幕与潜伏问题（2026-07-24 逐行审计）

> 前 9 节把 benchmark 新路线讲透了，但 CLAUDE.md 里"唯一可用可跑通"的 **Ray 老路线**（`token_benchmark_ray.py` 497 行 / `llm_correctness.py` 309 行）此前几乎没被展开。本节是对这两个入口逐行审计后的发现——含 **5 处真实潜伏问题**（多在少走的分支里，平时跑不炸，但一旦触发就出错），以及几处容易误读的机制。**这些只是审计记录，尚未修复**；按仓库 TDD 约定，修复应先写复现测试再动手。

### 10.1 Ray 主循环怎么跑（`token_benchmark_ray.py`）

- **prompt 预生成**：`get_token_throughput_latencies` 先 `random.seed(11111)`，在发压前把 `max_num_completed_requests` 条 prompt 全部采样好放进 `prompts[]`（"发压循环外造数据，循环内只发请求"），保证压测热路径最短、且**跨次运行 prompt 完全可复现**。
- **并发=裸线程**：起 `num_concurrent_requests` 个 `threading.Thread`，每个线程 `construct_clients(num_clients=1)` 各建**独立的单 Actor 池**；线程按 `thread_index % max` 错位取 prompt、每完成一条 `+= num_concurrent_requests` 跳步，用**条带化下标**避免多线程抢同一条 prompt。共享计数 `num_completed_requests` 与结果列表用一把 `threading.Lock` 保护。
- **两段收尾**：主循环靠 `time - start < timeout AND 完成数 < 上限` 双条件退出；退出后还有一段"最后再排空一次 `get_next_ready()`"的收尾代码，捞在途残余结果。**发现 A、B 都藏在这段收尾里**。

### 10.2 逐字延迟（ITL）的真实算法链

这是全仓最易误读的一处，链路要跨三个文件看：
1. `openai_chat_completions_client.py:110-119`：每收到一个内容帧，push 一个"距上帧的间隔"进 `time_to_next_token[]`；首帧记为 TTFT。
2. 同文件 `:135` 落库：`INTER_TOKEN_LAT = sum(time_to_next_token)`——**存的是间隔之和**，注释自己都写"这应该约等于 E2E，先放这儿"。
3. `token_benchmark_ray.py:121` 入口二次加工：`request_metrics[INTER_TOKEN_LAT] /= request_metrics[NUM_OUTPUT_TOKENS]`——**再除以输出 token 数**，此刻分母还是客户端回传的 `tokens_received`（数流帧数）。

也就是说 Ray 侧最终报出的 ITL ≈ `(逐帧间隔之和) / 帧数` = **逐字平均延迟**，而不是原始的"间隔之和"。§6 表格已据此更正。

### 10.3 审计发现清单

| # | 位置 | 严重度 | 问题 |
|---|---|---|---|
| **A** | `token_benchmark_ray.py:121` vs `:161` | 中 | **ITL 除数在两条路径不一致**：主循环除以 `request_metrics[NUM_OUTPUT_TOKENS]`（客户端帧数，尚未被覆盖）；收尾段却除以 `num_output_tokens`（Llama 重算值）。同一指标在"正常完成"与"收尾捞取"两路下口径不同。 |
| **B** | `token_benchmark_ray.py:170` | 高 | ~~**对 dict 调 `extend` 污染结果**：收尾段 `completed_requests.extend(request_metrics)`——`request_metrics` 是 **dict**，`extend` 会迭代它的**键**，把 `"ttft_s"`、`"error_code"` 等字符串塞进结果列表，随后 `metrics_summary` 里 `pd.DataFrame` + `df[ERROR_CODE]` 抛 `KeyError`。~~ **已修复（2026-07-24，`extend`→`append`）**，复现+守护测试见 `tests/test_token_benchmark_ray.py`。 |
| **C** | `llm_correctness.py:132` | 中 | ~~**除零崩溃**：`error_rate = num_errored / num_completed_requests` 无零保护。若测试超时且一条都没完成（`num_completed_requests==0`），直接 `ZeroDivisionError`。~~ **已修复（2026-07-25）**：`error_rate`/`mismatch_rate` 均加 `if num_completed_requests else 0` 守卫；复现+守护测试见 `tests/test_llm_correctness.py`（`test_timeout_s=0` 命中零完成路径，不起 Ray）。与上游 open issue #87 同族（见 §12）。 |
| **D** | `llm_correctness.py:31` 及 docstring | 低 | **陈旧的 `llm_api` 默认值**：函数签名默认 `llm_api="chat"`、docstring 写"chat 或 litellm"，但 `_CLIENT_REGISTRY` 只有 `openai/anthropic/sagemaker/vertexai/litellm`，传 `"chat"` 会被 `construct_clients` 拒绝。因 CLI 默认覆盖为 `"openai"` 而被掩盖，但直接调库函数会踩坑。 |
| **E** | `token_benchmark_ray.py:172` | 低 | **无效转义**：`print(f"\Results ...")` 中 `\R` 非合法转义，实际打印出字面量反斜杠 `\Results`（Python 目前仅告警）。 |

补充机制备注（非缺陷，但易踩）：
- `RequestsLauncher.launch_requests`（`requests_launcher.py:21`）在池无空闲 Actor 时**静默丢弃**该请求（`if has_free()` 不满足就不 submit，无返回值提示）。Ray 路线每线程 1 Actor 的用法下不会触发，但复用该类做高并发单池时要警惕"请求悄悄没发出去"。
- OpenAI 客户端出错时把 `tokens_received` 兜底设为 1、`error_code` 置为整数（`-1` 或真实码），使下游吞吐除法不炸；这也意味着**Ray 侧 error_code 是 int 或 None**，与 benchmark 侧统一为 str 的口径不同，跨路线聚合错误码时注意类型差异。

### 10.4 正确性校验（`llm_correctness.py`）的判定逻辑

- 造题：随机整数 → `num2words` 转英文单词 → 让模型转回数字，`temperature=0` 求确定性。
- 判对：先用正则 `(\d+),(?=\d)` **去掉数字内千分位逗号**（把 `1,234` 归一成 `1234`），再 `findall(r"\d+")` 抽出所有数字串，断言期望值的字符串形式**在其中**。这套宽松匹配容忍模型多说话（"The number is 42 ..."），但也意味着只要模型输出里**任何位置**出现该数字就算过——是有意的宽松，不是精确等值。
- 排空节奏：每积累 `num_concurrent_requests` 次迭代才 `get_next_ready()` 批量收一次，降低轮询开销。

---

## 11. 待办：审计发现的落地建议（供后续迭代）

按严重度排序，均建议遵循"先写复现测试 → 再修 → 跑绿"的 TDD 流程：

1. ~~**发现 B（高）**：`token_benchmark_ray.py:170` 改为 `append`；补"收尾段有残余结果"的复现测试。~~ ✅ **已完成（2026-07-24）**：`extend(request_metrics)` → `append(request_metrics)`，复现测试 `tests/test_token_benchmark_ray.py::test_cleanup_residual_kept_as_dict_not_polluted`（`test_timeout_s=0` 命中收尾路径，注入假分词器/发射器，不起 Ray），全仓 105 例全绿。
2. ~~**发现 C（中）**：`llm_correctness.py:132` 用 `_safe_div` 式零保护（完成数为 0 时 `error_rate` 记 0 或 None）；复现测试模拟"超时零完成"。~~ ✅ **已完成（2026-07-25）**：`error_rate`/`mismatch_rate` 加 `if num_completed_requests else 0` 守卫（沿用仓库既有 `... if <denom> else 0` 惯用法），复现测试 `tests/test_llm_correctness.py`（2 例：不崩 + 两率为 0），Red→Green→black，全仓 116 例全绿。此即上游 open issue #87 同族缺陷（§12.3）。
3. **发现 A（中）**：统一 ITL 两处除数（都用最终采用的 token 计数口径），消除正常/收尾路径的数值分叉。
4. **发现 D、E（低）**：把 `llm_correctness.llm_correctness` 默认值改成 `"openai"`、同步 docstring；修正 `\Results` 转义为 `\nResults` 或原意的换行。

> 发现 **B、C 已落地修复**（均按 TDD：先复现测试再修，全仓 116 例全绿）；剩余 **A（ITL 除数分叉）、D（陈旧默认值）、E（无效转义）** 未改动，供后续迭代。是否落地取决于项目所有者对"老 Ray 路线维护优先级"的判断——若 Ray 路线正逐步让位于 benchmark 新路线，A/D/E 可低优先。

---

## 12. GitHub 上游调研（2026-07-25，`gh` 实地核查）

> 本仓库是 fork。为把"本地代码"放回"社区上下文"里看，本节用 `gh` 直接查了上游仓库与相关 issue/PR 的真实状态，并交叉比对本 fork 的代码。**所有数据均为实查，非推测。**

### 12.1 上游已归档，本 fork 在"接棒"

- **上游 `ray-project/llmperf` 已被官方归档**（`isArchived: true`），最后一次 push 停在 **2024-12-09**；1129 ★ / 203 fork / Apache-2.0。归档意味着上游**不再接受任何 PR/维护**，社区未解决的问题只能靠各 fork 自行消化。
- 本 fork `thinker007/llmperf` 自上游归档日之后已累积 **36 个提交**——即 benchmark asyncio 子模块、原生 Anthropic 客户端、客户端注册表、各项硬化与审计修复，**全部是上游从未有过的增量**。这解释了为什么 `README.md` 里的安装地址仍指向上游 `ray-project/llmperf`（继承自 fork 起点），而实际能力已显著超出上游。

### 12.2 关键：代码注释里的 "#59" 得到实证

`runner.py` 顶部"禁止 `asyncio.gather` 批处理、必须 worker-pool"的铁律，一直引用一个 **#59**。实查确认它是上游一个 **已 MERGED 的 PR #59**——标题 *"subsequent requests cannot be sent until 'num_concurrent_requests' requests have all finished in non-block mode"*，关联 issue #43 / #56。其修复手段正是"多线程 + 每线程各持一个只管单请求的 launcher"，以绕开"整批请求必须全部完成才能发下一批"的批处理语义缺陷。**本 fork 的 worker-pool 约束就是对这段历史的直接继承**——注释所述属实，不是都市传说。

### 12.3 上游 open issue 与本 fork 代码的交叉比对

上游当前有 **37 个 open issue**（永远不会被官方修了）。逐条比对后，有几条与本 fork 代码直接相关：

| 上游 issue | 状态 | 与本 fork 的关系 |
|---|---|---|
| **#87** *ZeroDivisionError: division by zero* | OPEN | traceback 落在 `token_benchmark_ray.py` 的 `REQ_OUTPUT_THROUGHPUT = num_output_tokens / <生成耗时>`（上游裸除）。**本 fork 已修复此处**：主循环 `:127-128` 与收尾 `:167-168` 均改为 `num_output_tokens / e2e_lat if e2e_lat else 0`，客户端层 `output_throughput` 也初始化为 0 兜底。→ **一个上游未决 bug，在本 fork 已经不存在。** 但注意：本仓库审计**发现 C**（`llm_correctness.py:132` 的 `error_rate` 除零）是**另一处**除零，#87 并不覆盖它，仍是真实待修项。 |
| **#89** *如何压测推理模型 / reason content 是否计入 output token* | OPEN | 本 fork 的 `anthropic_messages_client.py` 已在 `extract_delta_text` 中同时识别 `thinking_delta`，**让首个 thinking token 计入 TTFT**、推理内容计入产出——正是该社区问题在本 fork 中的一种落地答案。 |
| **#92** *支持 `--tokenizer-path` 自定义分词器* / **#75** *生成 prompt 长度不对* | OPEN | 呼应本文 §6 的"token 计数口径差异"：benchmark 路线用随模型变化的 `litellm.token_counter`，Ray 路线用固定 Llama 分词器。两种口径各有取舍，上游的分词器诉求在 benchmark 路线里已被"按目标模型计数"部分满足。 |
| #90 llama4 / #91 embedding / #88 多模态 correctness / #95 指标区间偏差 | OPEN | 纯功能诉求，本 fork 暂未覆盖；记录在此供后续路线图参考——它们代表"归档上游无人认领、但真实存在"的需求缺口。 |

### 12.4 调研带来的结论

1. **本 fork 的定位应从"fork"升级为"事实上的续维护分支"**：上游已死，本地增量（asyncio 路线 + 原生 Anthropic + 一系列硬化/审计）已是项目主要价值来源。文档、README 的自我指向宜逐步与上游脱钩。
2. **本 fork 已无意中修好了一个上游热门 open bug（#87 的吞吐除零）**——这佐证了本仓库"防御式边界（除零返 0/None）"工程习惯的实战价值。
3. **审计发现 C 与上游 #87 是"同族不同处"的两处除零**：#87 那处已修，C 那处（correctness 的 error_rate）仍裸露，且社区里就有人踩过同类崩溃——这把发现 C 的优先级从"内部审计项"抬高为"社区验证过的真实痛点"，**建议优先修复**。
4. 上游 37 个未决 issue 里，推理模型（#89）、自定义分词器（#92）等已被本 fork 的设计部分回应，可作为对外说明本 fork 相对上游优势的素材。

### 12.5 归档后社区仍在发声：两条值得关注的最新信号（`gh` 实查，2026-07-26）

上游归档不代表社区停止使用——`isDisabled:false`，仓库仍可 clone/issue，只是无人再合代码。查了归档后最新提交的两条 open issue：

- **#95（2025-07-02，最新一条 open issue）*"Wide range of metrics (rpm,e2el) compared to other solutions"***：作者用同一模型（Qwen2.5-72B-AWQ）、同一 sonnet 数据集、同样的输入/输出 token 分布，分别跑 llmperf / genai-perf / vllm-benchmarks 三个工具，测出的 RPM **相差近 2 倍**（llmperf ≈96、genai-perf ≈170、vllm ≈138）。这不是某个工具的 bug，而是**"每个压测工具对同一批请求的计时口径都不一样"**——直接印证了本文 §6 强调的"指标口径差异"不是本仓库内部两条路线独有的麻烦，而是**整个 LLM 压测工具生态的通病**：不同工具对"请求何时算开始/结束/算完成"的定义各不相同，跨工具比较数字前必须先弄清楚各自口径，不能只看一个 RPM 数字就下结论。
- **#82（2025-02-10）*"actual server received 40 requests, expected 20"***：作者用本仓库同款命令行（`token_benchmark_ray.py --num-concurrent-requests 20`）跑，发现后端服务器实际收到 **2 倍于预期**的请求量。**已核查：本 fork 当前代码里没有任何重试/重发逻辑**（`rg "retry|retries|resubmit"` 在 `token_benchmark_ray.py`/`requests_launcher.py`/`openai_chat_completions_client.py` 中均无命中），根因未被上游定位、issue 至今无人回复修复，**只有另一位用户留言"这个项目好像已经死了"**——一句朴素的社区评价，恰好印证了 §12.1 的"上游已归档、无人认领"判断。**这不代表本 fork 一定复现该问题**（可能是用户环境的服务端/代理重试所致），但提醒：若未来有人反馈"请求数对不上"，应优先怀疑客户端外部因素（反向代理重试、服务端自身排队机制），而不是想当然归咎于 `RequestsLauncher`——本 fork 该组件的核心逻辑（每线程一个单请求 launcher）已因 #59 被重构过，不太可能是同一根因。

> 本节同样未改动任何源码，仅为调研记录。落地建议仍遵循仓库 TDD 约定（先写复现测试再修）。
