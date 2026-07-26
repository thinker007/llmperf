# Async SDK Benchmark (`llmperf.benchmark`)

轻量 asyncio 压测入口，直接压 `litellm.acompletion()`，无需 Ray。

## 用法

```bash
python -m llmperf.benchmark \
  --model gpt-4o \
  --mean-input-tokens 550 --stddev-input-tokens 150 \
  --mean-output-tokens 150 --stddev-output-tokens 80 \
  --num-concurrent-requests 10 \
  --max-num-completed-requests 100 \
  --timeout 600 \
  --results-dir ./results \
  --metadata "env=prod,version=1.0"
```

不传 `--results-dir` 时只把 summary 打印到 stdout；传了则额外落盘
`<model>_<in>_<out>_summary.json` 和 `..._individual_responses.json` 两个文件。

## 开环速率模式（`--request-rate`，2026-07-20 新增）

默认（不传 `--request-rate`）是**闭环**压测：固定 N 个并发槽，上一个请求返回才发下一个，回答"固定并发下延迟多少"。加上 `--request-rate` 后切换为**开环**压测：请求按泊松过程（指数分布间隔）在计划时刻到达并发出，不等前序请求返回，回答"流量以 X QPS 到达时服务会不会排队恶化"——这是 vLLM `benchmark_serving` 等主流工具同款的测量口径。

```bash
python -m llmperf.benchmark \
  --model gpt-4o \
  --request-rate 5 \
  --num-concurrent-requests 50 \
  --max-num-completed-requests 200 \
  --timeout 600
```

- `--request-rate` 必须 > 0（每秒到达请求数）。
- 开环下 `--num-concurrent-requests` 语义变为**最大在途请求数上限**：服务恶化时新到达会先排队等名额，防止客户端无限堆积。
- 计划到达时刻已超过 `--timeout` 的请求不会生成；拿到名额时预算已尽的请求不发射、不产生记录（与闭环"到点停发"口径一致）。
- summary 新增三个键：`scheduling_delay_s`（统计块：实际发出时刻 − 计划到达时刻，反映客户端因在途上限排队的程度；对**所有已发射请求**统计，含失败的）、`achieved_requests_per_s`（实际达成速率，与 `--request-rate` 目标差距大说明在途上限或客户端在限流）、`request_rate`（目标值回显）。individual 记录相应多一个 `scheduling_delay_s` 字段。闭环运行的输出结构完全不变。
- **语义提示**：开环下错误率天然可能高于闭环——请求不等前序返回，服务过载时会真实暴露排队与超时，这正是该模式的测量目的。

## HTTP 直连模式（`--api-base`，2026-07-22 新增）

默认压测经由 `litellm.acompletion()` 发请求。加上 `--api-base` 后**绕开 litellm SDK**，改用 `requests` 直接 POST 目标地址的 OpenAI 兼容 `/chat/completions` 流式端点，手工解析 SSE——用于测自建/网关端点的"裸延迟"（不含 SDK 层开销），或 litellm 尚未识别的 model 名称前缀。仓库 Ray 路线上 `--llm-api anthropic` 从借道 litellm 改为原生客户端就是同一个理由：去掉中间商，把延迟测准。

```bash
python -m llmperf.benchmark \
  --model my-served-model \
  --api-base http://localhost:8000/v1 \
  --api-key-env MY_API_KEY \
  --num-concurrent-requests 20 \
  --max-num-completed-requests 100
```

- `--api-base`：目标端点的 base URL，本模块会拼上 `/chat/completions`。不传 = 现状不变，继续走 litellm。
- `--api-key-env`：从该环境变量读 Bearer token（默认 `OPENAI_API_KEY`）。变量未设置时**不会报错退出**——只在 stderr 打一条告警后继续、不带 `Authorization` 头，因为本模式的典型场景（本地自建/内网端点）常常压根没有鉴权，比 Ray 路线的 openai 客户端更宽松。
- **协议假设**：仅支持标准 OpenAI 兼容流式 `/chat/completions`（vLLM/TGI/LM Studio/绝大多数网关的默认行为）。非 OpenAI 形状的协议（如原生 Anthropic SSE）不在本模式范围内，请走 Ray 路线的 `--llm-api anthropic`。
- **并发实现**：`requests` 是同步库，本模式通过一个**专属线程池**（大小 = `--num-concurrent-requests`）桥接进 asyncio 调度，不占用/受限于 asyncio 全局默认 executor（容量固定 `min(32, cpu+4)`）。因此并发数越大，实际会开等量的操作系统线程，请按需设置。
- summary 新增 `backend` 键（`"http"` 或 `"litellm"`，始终写入）；设置了 `--api-base` 时额外写入 `api_base` 键回显。
- 闭环、开环（`--request-rate`）两种调度模式都可以叠加 `--api-base` 使用，管线其余部分（指标聚合、CLI 参数、落盘）零改动。

## 与原 Ray 版（`token_benchmark_ray.py`）的差异

- **并发引擎**：asyncio worker-pool 替代 Ray + threading，零重依赖。
- **分词器**：`litellm.token_counter`（按 model 精确计）替代固定 Llama 分词器。
- **⚠️ ITL 口径不同**：本模块 `ITL = (e2e − ttft) / (output_tokens − 1)`，语义纯净、与 output token 口径一致；原 Ray 版是 `sum(time_to_next_token) / chunk数`（首项含 TTFT、分母用 chunk 数）。**两者数值不可直接对比。**
- **`--timeout` 语义**：整体测试时长上限，到点停止派发新请求，不 cancel 在途请求；已发出的请求等其自然完成后计入统计。单个请求透传给底层 HTTP 的超时是**当前剩余预算**（`--timeout` 减去已耗时），而非每个请求都重新拿满一个 `--timeout`——这样临近整体预算末尾发出的慢请求不会再独占一整个超时窗口。

## 健壮性行为（2026-07-15 硬化）

- **空响应计为失败**：请求成功返回但模型未吐出任何 token 时，该请求记 `error_code = "empty_response"`、计入错误率，而不是当成功污染延迟分位数。
- **单请求错误隔离**：任何单个请求内部抛出的异常都会被就地转成一条带 `error_code`/`error_msg` 的失败记录，不会中断并发池里的其他请求。错误码取自异常的 `status_code`（保留合法的 `0`，仅在缺失时才记 `"unknown"`）。
- **`--metadata` 校验与优先级**：格式必须是 `key=value`（逗号分隔多项）；某项缺 `=` 会直接报错退出（退出码 2）。用户 metadata 键若与内建汇总字段（`model` / `num_concurrent_requests` / `timestamp` / `litellm_version`）撞名，会被忽略并在 stderr 打印告警——内建字段永远优先。
- **数值参数下限**：`--num-concurrent-requests` 与 `--max-num-completed-requests` 必须 ≥ 1，`--timeout` 必须 > 0；否则报错退出（退出码 2），不再静默跑出一份空报告。
- **`--additional-sampling-params` 必须是 JSON 对象**：非法 JSON 或合法但非对象（如 `'[1,2]'`、`'5'`）都会报错退出（退出码 2），而不是把非法值透传到底层导致压测中途崩溃。
