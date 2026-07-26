# llmperf 使用者速览

> 面向「拿来用」的人：llmperf 能测什么、两条压测路线怎么选、命令怎么敲、输出怎么读。
> 不讲内部实现（那部分见 `docs/code-summary.md`）。生成：2026-07-24，核对：2026-07-25。

---

## 1. 它是什么

一把量化 LLM API **性能**与**正确性**的压测尺。你给它一个模型和一组「输入/输出长度分布 + 并发」，它并发打请求，产出一份**分位数统计报告**（p50/p90/p95/p99…）。

它测五个核心指标：

| 指标 | 含义 | 落盘 key |
|---|---|---|
| **TTFT** | 首字延迟：发出请求到收到第一个 token | `ttft_s` |
| **ITL** | 逐字延迟：token 之间的平均间隔 | `inter_token_latency_s` |
| **E2E** | 端到端延迟：整条请求从发出到收完 | `end_to_end_latency_s` |
| **吞吐** | 每秒产出 token 数 | `request_output_throughput_token_per_s` |
| **错误率** | 失败请求占比 | `error_rate` |

外加一个**正确性校验**工具（`llm_correctness.py`）：把随机数字转成英文单词，让模型转回数字，核对是否一致——用来验证模型在长上下文/高并发下是否「说胡话」。

---

## 2. 两条路线，先选对入口

llmperf 有两个能跑的压测入口，**指标数字不能跨路线直接对比**（尤其 ITL 算法不同）。先按下表选：

| 你的情况 | 用哪条 | 命令入口 |
|---|---|---|
| 要压 **SageMaker / VertexAI**，或想要**跨模型统一 token 口径**（固定 Llama 分词器），或要**可复现**的 prompt | **Ray 路线** | `python token_benchmark_ray.py …` |
| 只压 **OpenAI 兼容 / Anthropic / litellm 支持的 100+ 家**，想要**轻量无 Ray**、想用**开环请求速率**或**HTTP 直连**模式 | **benchmark 路线** | `python -m llmperf.benchmark …` |

一句话：**要冷门厂商或可复现 → Ray；要轻量或开环/直连 → benchmark。**

---

## 3. Ray 路线：`token_benchmark_ray.py`

成熟、覆盖最广（5 个厂商），用 Ray 多线程并发，固定 Llama 分词器保证跨模型 token 计数口径一致。

### 快速开始（以 OpenAI 兼容端点为例）

```bash
export OPENAI_API_KEY=secret_abcdefg
export OPENAI_API_BASE="https://api.endpoints.anyscale.com/v1"

python token_benchmark_ray.py \
  --model "meta-llama/Llama-2-7b-chat-hf" \
  --mean-input-tokens 550 --stddev-input-tokens 150 \
  --mean-output-tokens 150 --stddev-output-tokens 10 \
  --num-concurrent-requests 1 \
  --max-num-completed-requests 2 \
  --timeout 600 \
  --llm-api openai \
  --results-dir "result_outputs" \
  --additional-sampling-params '{}'
```

### 参数速查

| 参数 | 默认 | 含义 |
|---|---|---|
| `--model` | 必填 | 模型名 |
| `--llm-api` | `openai` | `openai` / `anthropic` / `litellm` / `sagemaker` / `vertexai` |
| `--mean-input-tokens` / `--stddev-input-tokens` | 550 / 150 | 输入长度的高斯分布 |
| `--mean-output-tokens` / `--stddev-output-tokens` | 150 / 80 | 输出长度分布（`mean` 即 `max_tokens`，实际不保证返回这么多） |
| `--num-concurrent-requests` | 10 | 并发线程数（每线程一个独立 Actor 池） |
| `--max-num-completed-requests` | 10 | 完成多少条后停（也可能先超时停） |
| `--timeout` | 90 | 整个压测的时间上限（秒） |
| `--additional-sampling-params` | `{}` | 透传给 API 的额外采样参数（JSON 字符串） |
| `--results-dir` | 空 | 落盘目录，不填不落盘 |
| `--metadata` | 空 | 附加标签，如 `name=foo,bar=1` |

各 `--llm-api` 所需的环境变量（`ANTHROPIC_API_KEY`、SageMaker/VertexAI 凭证等）见根目录 `README.md`。

---

## 4. benchmark 路线：`python -m llmperf.benchmark`

轻量，纯 asyncio，无 Ray/pandas/transformers。除闭环外还多两个模式：**开环请求速率**、**HTTP 直连**。

### 4.1 闭环（默认，压满并发）

```bash
export OPENAI_API_KEY=secret_abcdefg

python -m llmperf.benchmark \
  --model "gpt-4o-mini" \
  --mean-input-tokens 550 --mean-output-tokens 150 \
  --num-concurrent-requests 10 \
  --max-num-completed-requests 100 \
  --timeout 90 \
  --results-dir result_outputs
```

闭环 = 始终保持 `--num-concurrent-requests` 条请求在飞，一条完成立刻补一条。测「系统在固定并发下的表现」。

### 4.2 开环（模拟真实到达速率）

加 `--request-rate`（每秒请求数，泊松到达）即切开环。此时 `--num-concurrent-requests` 语义变为**最大在途上限**：

```bash
python -m llmperf.benchmark \
  --model "gpt-4o-mini" \
  --request-rate 5 \
  --num-concurrent-requests 50 \
  --max-num-completed-requests 200 \
  --timeout 120
```

测「每秒来 5 个请求、最多允许 50 个同时在途」时的延迟——更贴近线上流量。开环报告额外含**调度延迟**（`sched_delay_s`，请求排队等名额的时间）和**实际达成速率**。

### 4.3 HTTP 直连（绕开 litellm SDK）

加 `--api-base`，直连 OpenAI 兼容的 `/chat/completions`，去掉 litellm 中间层以获得更纯净的延迟：

```bash
python -m llmperf.benchmark \
  --model "meta-llama/Llama-3-8b" \
  --api-base "https://your-endpoint/v1" \
  --api-key-env OPENAI_API_KEY \
  --num-concurrent-requests 8 \
  --max-num-completed-requests 50
```

`--api-key-env` 指定从哪个环境变量取 key（默认 `OPENAI_API_KEY`）。

### 参数速查（benchmark 独有）

| 参数 | 默认 | 含义 |
|---|---|---|
| `--request-rate` | 无 | 传了即**开环**，每秒到达请求数（泊松） |
| `--api-base` | 无 | 传了即 **HTTP 直连**该端点 |
| `--api-key-env` | `OPENAI_API_KEY` | HTTP 直连时从哪个环境变量取 key |
| `--stddev-output-tokens` | 80 | （其余 `--model`/`--mean-*`/`--num-concurrent`/`--max-num`/`--timeout`/`--results-dir`/`--metadata`/`--additional-sampling-params` 同 Ray 路线语义） |

> **参数即防线**：非正数值（如 `--mean-input-tokens 0`）、缺 `=` 的 metadata、非 JSON 对象的 sampling params 都会以**退出码 2** 被拦下并友好报错，不会让压测跑到一半崩池。

---

## 5. 怎么读输出

两条路线都打印一份 JSON summary（配 `--results-dir` 时另落盘两文件）。关键字段：

```jsonc
{
  "end_to_end_latency_s": { "p50": 2.31, "p90": 4.10, "p99": 6.7, "mean": 2.6, ... },
  "ttft_s":               { "p50": 0.28, "p90": 0.55, ... },
  "inter_token_latency_s":{ "p50": 0.012, ... },
  "request_output_throughput_token_per_s": { "p50": 42.1, ... },
  "num_completed_requests": 100,
  "num_errored_requests": 0,
  "error_rate": 0.0,
  "error_code_frequency": {},          // 有失败时按错误码计频
  "overall_output_throughput": 380.5,  // 全局：总产出 token / 总墙钟时间
  "model": "gpt-4o-mini",
  "backend": "litellm"                 // 或 "http"
}
```

看性能主要看三组分位数（**E2E / TTFT / ITL**）；`p50` 看典型体验，`p90/p99` 看尾部抖动。`overall_output_throughput` 是系统整体产能。有错误时看 `error_code_frequency` 定位是哪类失败。

落盘目录（`--results-dir`）会生成两个文件：
- `<model>_<in>_<out>_summary.json`：上面这份汇总
- `<model>_<in>_<out>_individual_responses.json`：每条请求的原始明细（用来自己画图/深挖）

---

## 6. 三个必须知道的坑

1. **别跨路线对比数字**。Ray 与 benchmark 的 ITL 算法、token 计数口径、prompt 随机性都不同（细节见 `docs/code-summary.md` §6）。同一维度只在同一路线内比。
2. **`--mean-output-tokens` 不是「保证输出这么多」**，它是 `max_tokens` 上限，模型可能提前停。
3. **环境依赖有坑**（2026-07-25 复核）：当前 `.venv` 是 `pydantic 2.4.2 + litellm 1.73.6`，二者不兼容会导致 `import litellm` 失败、benchmark 路线跑不起来。修法二选一：`pip install 'pydantic>=2.5,<3'` 或 `pip install 'litellm==1.61.13'`。Ray 路线的原生 Anthropic/OpenAI 客户端不吃这条依赖。

---

## 7. 相关文档

- **代码级实现导读**（每段代码做什么/为什么/坑在哪）：`docs/code-summary.md`
- **新贡献者上手 / 如何加一个厂商**：`docs/dev-guide.md`
- **架构索引**（面向 AI）：根 `CLAUDE.md` 及各子模块 `CLAUDE.md`
- **各厂商环境变量与安装**：根 `README.md`
- **benchmark 路线详细用户手册**：`docs/benchmark.md`
