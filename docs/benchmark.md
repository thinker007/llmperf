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
- **`--timeout` 语义**：整体测试时长上限，到点停止派发新请求，不 cancel 在途请求；已发出的请求等其自然完成后计入统计。
