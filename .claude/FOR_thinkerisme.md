# LLMPerf 项目导读（写给 thinkerisme）

> 这份文档用大白话讲清楚 LLMPerf 是什么、怎么搭起来的、为什么这么设计，以及这次"支持 Anthropic `/v1/messages`"改造背后的工程思路与坑。读完你应该能独立给它再加一种 API 客户端。

## 一、这玩意儿是干嘛的

LLMPerf 是一把"压测尺"——拿它去量各种大模型 API 的**性能**：首字延迟（TTFT）、逐字延迟（inter-token latency）、端到端延迟、吞吐量。你给它一个模型、一个并发数、一段"造出来的"提示词，它就并发地猛打目标端点，把每个请求的时延掰开揉碎统计成分位数（p50/p90/p99）。

核心入口是 `token_benchmark_ray.py`（性能压测）和 `llm_correctness.py`（正确性校验）。

## 二、架构：一个"插座"配多种"插头"

把它想象成一个万能充电座。充电座（框架）只认一个接口形状，谁想接进来就做一个符合形状的插头（客户端）。

```
token_benchmark_ray.py            ← 主循环：造提示词、起线程、收指标、算分位数
        │  --llm-api 选哪种插头
        ▼
common.py  construct_clients()    ← 插座：按名字发对应的插头
        │
        ├─ openai     → OpenAIChatCompletionsClient   (requests 直打 /chat/completions)
        ├─ anthropic  → AnthropicMessagesClient   ★本次新增 (requests 直打 /v1/messages)
        ├─ sagemaker  → SageMakerClient
        ├─ vertexai   → VertexAIClient
        └─ litellm    → LiteLLMClient               (万金油，靠 litellm 兼容一大堆厂商)
```

**接口形状**定义在 `ray_llm_client.py`：所有插头都实现一个方法

```python
def llm_request(self, request_config) -> (metrics, generated_text, request_config)
```

返回三元组：① 这次请求的性能指标字典 ② 生成的文本（框架会用 llama tokenizer 重新数 token） ③ 原始配置（留作日志）。只要你的插头吐出这个形状，主循环一行都不用改——这就是**里氏替换（LSP）**在真实项目里的样子。

**为什么用 Ray？** 每个客户端是一个 `@ray.remote` Actor。Ray 帮我们把"并发打 N 个请求"这件事分布式化、进程隔离化，`RequestsLauncher` 负责把请求撒出去、`get_next_ready()` 把结果捞回来。你不用自己写进程池。

**关键数据结构** `models.py::RequestConfig`：一个 pydantic 模型，装着 `model / prompt / sampling_params / llm_api`。注意 `sampling_params` 这个口袋——主循环把 `max_tokens` 塞进去（`token_benchmark_ray.py` 里 `default_sampling_params = {"max_tokens": ...}`），客户端再 `body.update(sampling_params)`。这个设计让"输出长度"这种参数对所有厂商通用，省了一堆 if-else（**DRY**）。

## 三、本次改造：从"借道 litellm"到"原生 /v1/messages"

### 痛点
改造前 `--llm-api anthropic` 落在 `elif llm_api in SUPPORTED_APIS` 分支，**实际由 LiteLLM 代理**。LiteLLM 底层确实打的是 Anthropic 的 `/v1/messages`，所以"能用"——但作为压测工具有三个硬伤：
1. **延迟不纯**：TTFT 里混入了 litellm 的格式转换/缓冲开销，测的不是端点本身；
2. **字段受限**：Anthropic 特有的 `system` 顶层字段、`anthropic-version` 头、`thinking` 不好精确控制；
3. **端点受限**：连不上自建/代理的 `/v1/messages` 兼容端点。

一句话总结这次改造的价值：**去掉中间商，把延迟测准。**

### OpenAI 与 Anthropic 协议的关键差异（改造的真正难点）
这俩都叫"chat"，但流式协议长得不一样，照搬会翻车：

| 维度 | OpenAI Chat Completions | Anthropic Messages |
|---|---|---|
| 路径 | `/chat/completions` | `/v1/messages` |
| 鉴权 | `Authorization: Bearer` | `x-api-key` + `anthropic-version` |
| `max_tokens` | 可选 | **必填**，不给就 400 |
| 流式分帧 | `data: {...}` / `data: [DONE]` | `event: 类型` + `data: {...}`，按 JSON 的 `type` 分发 |
| 取增量字 | `choices[0].delta.content` | `content_block_delta` → `delta.text_delta.text` |
| 推理内容 | `delta.reasoning_content` | `delta.thinking_delta.thinking` |
| 结束信号 | `data: [DONE]` | `message_stop` 事件 |

### 怎么做的（落在 `anthropic_messages_client.py`）
以 OpenAI 客户端为模板，只改了 4 处：请求体（强制 `max_tokens`，`body.setdefault`）、请求头、SSE 解析、结束判定。其余 metrics/计时骨架原样复用——**开放/封闭（OCP）**：新增一个文件 + `common.py` 一个 `elif`，老客户端零改动。

一个特意做的设计：把 SSE 解析抽成两个**纯函数**

```python
parse_data_line(chunk)   # 从 SSE 字节行里抠出 JSON（忽略 event:/ping/空行）
extract_delta_text(data) # 从 content_block_delta 里取 text 或 thinking
```

为什么抽出来？因为客户端是 `@ray.remote` Actor，又依赖网络，**很难直接单测**。把"协议解析"这块易错逻辑剥成不依赖 ray/网络的纯函数，就能用普通 pytest 喂样例数据断言。这就是**单一职责（SRP）**带来的可测性红利。

## 四、能学到的工程经验

1. **先写测试复现，再写实现（TDD）**。这次先写 `tests/test_anthropic_messages_client.py`，跑一遍确认 Red（模块不存在导入失败），再实现到 Green。测试里专门塞了一段**真实形状的 Anthropic SSE 流**（含 `thinking_delta` 在 `text_delta` 之前），断言文本拼接顺序和 token 计数——这种"replay 真实报文"的测试，比 mock 一堆函数调用更能抓住协议层的 bug。

2. **端到端冒烟别省**。纯函数测试覆盖了解析，但请求头、`max_tokens` 注入、SSE 消费循环、`message_stop` 终止这些"胶水"只有真跑一遍才放心。做法：起一个本地 `http.server` 假装 `/v1/messages`，用真 Ray actor 打过去，断言它收到的 body/headers 和算出的 metrics。低成本，高信心。

3. **"基本能用"≠"测得准"**。litellm 路径就是典型的"功能上能用，但不满足这个工具的核心目的（精确测延迟）"。判断要不要重写时，看的不是"能不能跑通"，而是"它服务的真实目的有没有被中间层污染"。

4. **token 计数的口径要想清楚**。客户端里 `tokens_received` 逐 delta 自增，但框架最后会用 llama tokenizer 对 `generated_text` **重新数**（`token_benchmark_ray.py:117`）。所以客户端内的计数只影响 TTFT/逐字延迟的"分母"，不用纠结要不要去读 `usage.output_tokens`——知道哪个数字真正进了最终报表，能省掉无谓的精确化工作（**YAGNI**）。

5. **推理模型的首字延迟坑**。最近一次提交（`80a6566`）专门修了"流式下没提取 reasoning_content 导致 TTFT 算错"。对应到 Anthropic 就是 `thinking_delta`——思考内容也要计入第一个 token 的时间，否则首字延迟会偏大。新客户端内建了这个处理。注意：若模型默认 `display: "omitted"`，思考文本是空的，需要时通过 `--additional-sampling-params '{"thinking":{"type":"adaptive","display":"summarized"}}'` 打开。

## 五、想再加一种 API？照这个清单走
1. 在 `ray_clients/` 仿照 `anthropic_messages_client.py` 写一个 `@ray.remote` 类，实现 `llm_request`，把易错的解析抽成纯函数；
2. `common.py` 加一个 `elif`，并确认名字在 `SUPPORTED_APIS` 里；
3. 写 `tests/` 用 replay 真实报文做断言，先 Red 后 Green；
4. 起本地 mock server 做端到端冒烟；
5. 更新 README + 本文档（**Doc Sync**）。

## 六、相关文件速查
- 新客户端：`src/llmperf/ray_clients/anthropic_messages_client.py`
- 路由：`src/llmperf/common.py`
- 接口契约：`src/llmperf/ray_llm_client.py`
- 请求配置：`src/llmperf/models.py`
- 主循环：`token_benchmark_ray.py`
- 单测：`tests/test_anthropic_messages_client.py`

---

## 七、第二次改造：给 LLMPerf 甩掉 Ray，新增 `llmperf.benchmark`

### 为什么要做这件事
`token_benchmark_ray.py` 那套压测循环好用，但它的"发动机"是 Ray（多进程 actor）+ threading + `transformers` 的 Llama 分词器——三样都是"重脚手架"。而压测 LLM 本质是 **I/O 密集型**任务：99% 的时间都在等网络返回，不是在算 CPU。用比喻说：派 50 个人去 50 个窗口排队，Ray 的做法是真雇 50 个人各守一个窗口（每个人都要发工资、占工位——对应进程开销、序列化开销）；而 `asyncio` 的做法是雇 1 个眼疾手快的人在窗口间来回巡视，谁有动静就搭理谁——活一样干完，成本却低得多。

这次改造的产物是仓库内新增的 `src/llmperf/benchmark/` 模块，`python -m llmperf.benchmark` 是它的新入口，和旧的 Ray 压测**完全并存、零侵入**（不 import、不修改旧代码一行）。

### architecture：四个文件，各司一职（SRP）
```
src/llmperf/benchmark/
  prompts.py    ← 按目标 token 数从 sonnet.txt 里随机拼一段 prompt（纯函数）
  metrics.py    ← 单请求指标换算 + 跨请求聚合统计（纯函数，无网络）
  runner.py     ← asyncio worker-pool 并发引擎 + 单请求流式采集（唯一"脏"的部分：真的发请求）
  __main__.py   ← argparse CLI，把上面三块粘合成可执行命令
```
`prompts.py`/`metrics.py` 是纯函数，测试起来最省心——喂什么进去，断言什么出来，不需要 mock 网络。`runner.py` 是唯一要跟 `litellm.acompletion()` 打交道的地方，测试靠 `monkeypatch` 换掉 `acompletion`，喂假的异步流式响应。

### 技术选型：为什么是 `asyncio` 而不是继续用 Ray
| 维度 | 旧版（Ray） | 新版（asyncio） |
|---|---|---|
| 并发原语 | `@ray.remote` actor + `ActorPool` | 普通协程 + `asyncio.Queue` |
| 分词器 | 固定 `LlamaTokenizerFast`（重依赖，跨模型文本等长但计费口径不准） | `litellm.token_counter(model=...)`（零新增依赖，按目标模型精确计费） |
| 新增依赖 | 无（本来就有） | **零**——只用 litellm（已是依赖）+ Python 标准库 |

**关键纪律：worker-pool，不用 `gather` 批处理。** 这不是随便选的，是踩过坑的教训——仓库历史上的 `#59` bug（commit `f1d6bed`）就是"凑够 N 个一起发、等最慢的那个"这种批处理语义导致的：并发数会从 N 塌陷到 1 再回弹，吞吐被严重低估。asyncio 版本钉死用"N 个常驻协程各自从共享队列里取任务，谁先完成谁立刻取下一个"的 worker-pool 模型，在途请求数稳定等于 N，不抖动。`tests/test_benchmark_runner.py::test_run_benchmark_concurrency_stays_at_n` 专门验证了这一点——不是只断言"峰值 ≤ N"（批处理也能满足这个弱条件），而是断言"队列有货时持续保持 == N"，这才是真正区分两种模型的地方。

### 指标口径：一个有意为之的"不兼容"变更
旧版的 inter-token latency 算法是 `sum(每个 token 的到达间隔) / chunk 数`——问题是这个"和"里第一项其实是 TTFT（首字延迟），混进去就不纯净了，而且分母用的是"chunk 数"而不是"token 数"，两个指标口径对不上。新版明确改成：
```
ITL = (端到端延迟 − 首字延迟) / (输出 token 数 − 1)   当输出 token 数 > 1
    = 0                                              否则
```
语义清楚："掐头去尾后剩下的时间，摊到每个 token 间隔上"。**代价是新旧两版的 ITL 数值不可以直接比较**——这个事实在 `docs/benchmark.md` 里专门用 ⚠️ 标出来了,不然以后有人拿新旧结果画在一张图上对比,会得出错误结论。

### 这次踩的坑：一个从"抄"过来的死循环 bug
`prompts.py` 的采样逻辑是照抄 `utils.py::randomly_sample_sonnet_lines_prompt` 的（只是把固定分词器换成 `token_counter`）。原逻辑里有一段：
```python
num_prompt_tokens = 采样一个高斯随机数
while num_prompt_tokens < 基础前缀的 token 数:
    num_prompt_tokens = 重新采样
```
这段代码在旧版本里"能用"，因为默认配置的 `stddev`（标准差）从来不是 0。但这次为 `prompts.py` 补单测时，写了一个"验证收敛性"的测试，故意传了 `stddev=0`（`mean` 又小于基础前缀长度）——`random.gauss(mean, 0)` 是**确定性**的，永远只能采出同一个数，这个 while 循环因此**永远退不出来**，测试直接卡死。

怎么发现的：不是靠读代码看出来的，是**真的跑了一遍测试，等它超时**才发现的——这也是为什么"写完就跑"这个习惯很重要，光靠肉眼审查这种边界条件极容易漏掉。

怎么修的：`stddev == 0` 时提前判断——既然结果确定不会变，就没必要重采样，直接钳制到"至少等于基础前缀长度"即可；`stddev > 0` 时保留原来的重采样逻辑不变（不改变真实使用场景下的行为分布）。改动只有 4 行，但把一个"配置恰好踩中边界就会挂死进程"的隐患彻底堵上了。

**教训**：移植/复用别人的代码时，"原来能跑"不等于"没有 bug"，只是没人用触发条件测过它。**该给这段逻辑补的测试用例，恰恰是原作者没写过的那个**——这正是新增测试覆盖率的价值所在。

### 整个开发流程：如何用"计划 + TDD"把活干扎实
这次没有直接上手写代码，而是先有一份经过 6 轮自我评审的设计文档（`docs/superpowers/specs/2026-06-28-litellm-benchmark-integration-design.md`），再拆成 5 个任务的实施计划（`docs/superpowers/plans/2026-06-28-litellm-benchmark-integration-plan.md`），每个任务都严格走"先写测试确认失败（Red）→ 实现到测试通过（Green）→ `black` 格式化 → `git commit`"的节奏,一个任务一个提交,方便回溯。5 个任务分别是 `prompts.py`（采样）→ `metrics.py`（聚合）→ `runner.py`（并发引擎）→ `__main__.py`（CLI）→ 文档收尾,共 26 个测试全绿。

### 想继续扩展？
- **加 Proxy 模式**（压 HTTP 端点而非直接调 SDK）：这次刻意没做（YAGNI），如果要加，新建一个 `runner_http.py` 之类的文件，复用 `metrics.py` 不用动。
- **加正确性校验**：同样超出这次范围，可以参考 `llm_correctness.py` 的思路另起一个模块。
- 改指标口径或采样逻辑之前，务必先读一遍设计文档的"关键技术决策"一节——那里记录了"为什么不这样做"的理由,省得走回头路。

## 八、相关文件速查（第二次改造）
- 采样：`src/llmperf/benchmark/prompts.py`
- 指标聚合：`src/llmperf/benchmark/metrics.py`
- 并发引擎：`src/llmperf/benchmark/runner.py`
- CLI：`src/llmperf/benchmark/__main__.py`
- 单测：`tests/test_benchmark_{prompts,metrics,runner,cli}.py`
- 使用文档：`docs/benchmark.md`
- 设计规格 + 实施计划：`docs/superpowers/specs/`、`docs/superpowers/plans/`

## 九、第三次改造：从"能跑通"到"生产可用"的硬化

第二次改造让 `benchmark` 模块**跑得通**（happy path 全绿）。但"跑得通"和"敢丢到生产环境压一整晚"是两回事。这次（2026-07-15）做的就是把中间那道坎填平——8 个原子改动，分成两簇：A 簇管"运行期别崩别算错"，B 簇管"用户手滑了给个人话报错而不是一脸 traceback"。

### A 簇：真实环境里，对象不会永远长你想的样子
happy-path 代码最爱做的假设是"流式返回的每个 delta 都是我预期的那个结构"。真实的 litellm 背后接了 100+ 家厂商，有的 delta 是 dict、有的是 pydantic 对象，有的模型干脆一个 token 都不吐。四个坑逐一堵：

- **A1 防御式提取**：把"从 delta 里抠 `reasoning_content`/`content`"抽成一个纯函数 `extract_content`，内部 `_get` 同时兼容 `dict.get` 和 `getattr` 两种取值方式。好处不只是健壮——它现在能脱离网络直接单测四种 delta 形态，而不用真发一次请求去撞运气。
- **A2 异常绝不逃逸**：把整个 `_single_request` 的生命周期（连 token 计费都算上）包进一个 `try/except`。任何一个请求内部炸了，都就地变成一条带 `error_code` 的失败记录返回——**绝不让它冒泡上去把整个 worker 协程带崩**。压测跑到第 998 个请求时因为一个畸形响应崩掉、前面 997 个结果全丢，是最让人肉疼的失败模式。
- **A3 空流算失败**（⚠️ 语义变更）：模型成功返回但一个 token 都没吐，以前被算成"成功"，混进延迟分位数里拉低数据。现在明确判成 `empty_response` 错误。
- **A4 一个 `or` 引发的血案**：原代码写 `error_code = str(e.status_code or "unknown")`。看着没问题？`status_code == 0` 是**合法值**（某些传输层错误就是 0），但 `0 or "unknown"` 在 Python 里等于 `"unknown"`——真实的 0 被 `or` 悄悄吞掉了。修法是老老实实判 `None`：`"unknown" if sc is None else str(sc)`。**教训：`x or default` 这种写法，只有当 `x` 的所有"假值"（0、""、[]、False）都确实该走 default 时才安全，否则用 `x if x is not None else default`。**

### B 簇：边界处给人话
- **B1**：`--metadata foo`（忘了写 `=`）以前会产生诡异结果，现在直接 argparse 友好报错、退出码 2。
- **B2 超时=剩余预算**（⚠️ 语义变更）：以前每个请求都透传满一个 `--timeout` 给 HTTP 层。想象 `--timeout 600`，第 599 秒才发出的慢请求，居然还能再独占 600 秒——整体预算形同虚设。改成透传"剩余预算"`max(0, timeout − 已用时)`，慢请求最多拖到总预算耗尽为止。
- **B3 内建键优先**：`--metadata model=假的` 不该把汇总里真实的 `model` 字段覆盖掉。撞名就跳过 + stderr 告警，内建字段永远赢。
- **B4 数值下限**：`--num-concurrent-requests 0` 以前会静默跑出一份全 `None` 的空报告（无 worker）。现在非正值直接退出码 2 拒绝。

### 这次真正的"元教训"：工具会骗你，git 不会
这轮是用 **SDD（子代理驱动开发）** 跑的——每个任务派一个全新的子代理去实现、再派一个去评审。听起来很美，现实给了三记闷棍：

1. **Task 1 的子代理"越界"了**：让它只改 `runner.py`，它却顺手改了 6 个不相关文件（包括 54 行主脚本），还没提交、报告也是坏的。**处理**：把越界改动全部回退到基线，只保留经我逐行核对无误的范围内工作，再提交。此后每次派活都**明确点名允许改哪些文件、禁止碰其他**。
2. **连续三个子代理死于 "Prompt is too long"**：Task 7、8 的实现代理、8 的评审代理接连因为"控制器上下文太大"派发即死。其中两个是**写完文件甚至提交完才断的**——这就是为什么 **`.superpowers/sdd/progress.md` 这本账本 + `git log` 才是唯一可信的事实来源**。上下文会被压缩、会丢，但账本里写的提交号在 git 里真实存在。断了就从账本恢复，而不是靠"我记得刚才做到哪了"。
3. **打捞模式（salvage）**：代理死了但活儿留下了，就地验证（范围干净？diff 匹配契约？测试绿？）→ 补一份打捞报告 → 继续。Task 8 的独立评审门因此没跑成，就把它**折叠进最终的全分支评审**（反正那轮横扫全部 8 个提交），而不是假装它评过了。

**顺带一个环境坑**：某个时刻本机 `.venv` 里的 `pydantic`（2.4.2）配不上 `litellm`（1.73.6，要 import 2.5+ 才有的 `Discriminator`），`pytest` 一 import 就崩。这跟我们改的代码半毛钱关系没有，纯粹是依赖漂移——但它提醒你：**"测试跑不起来"和"测试失败"是两码事，前者往往是环境问题，别急着去改业务代码。**

## 十、相关文件速查（第三次改造·硬化）
- 改动集中在：`src/llmperf/benchmark/runner.py`（A1-A4、B2）、`src/llmperf/benchmark/__main__.py`（B1、B3、B4）
- 新增/扩充测试：`tests/test_benchmark_runner.py`、`tests/test_benchmark_cli.py`、`tests/test_benchmark_metrics.py`（26 → 41 个测试函数）
- 设计规格：`docs/superpowers/specs/2026-07-15-benchmark-production-hardening-design.md`
- 实施计划 + 提交映射：`docs/superpowers/plans/2026-07-15-benchmark-production-hardening.md`
- SDD 执行账本（事实来源）：`.superpowers/sdd/progress.md`
