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
