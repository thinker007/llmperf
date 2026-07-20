[根目录](../CLAUDE.md) > **tests**

## 模块职责

`tests/` 是 `llmperf` 的 pytest 单元测试套件。**全部为纯函数级测试**：不连真实网络、不启动 Ray runtime，用 `monkeypatch` 替换外部依赖（如 `litellm.acompletion` / `litellm.token_counter` / `LlamaTokenizerFast`）。测试聚焦"协议解析 / 采样逻辑 / 指标聚合 / CLI 参数校验"这类容易出 bug、又难以对 Ray Actor 直接单测的部分——把易错逻辑抽成纯函数、再用普通 `assert` 覆盖，是本仓库反复出现的测试策略。

## 运行

```bash
python -m pytest tests/ -v
```

当前全绿：**6 个文件 / 53 个测试函数 / pytest 展开 58 例**。

## 文件清单

| 文件 | 覆盖对象 | 用例数 | 要点 |
|---|---|---|---|
| `test_anthropic_messages_client.py` | `ray_clients/anthropic_messages_client.py` 的纯函数 `parse_data_line` / `extract_delta_text` | 7 | SSE 分帧：`startswith` 守卫、`event:`/心跳行返回 None、`thinking_delta` 拼接 |
| `test_benchmark_prompts.py` | `benchmark/prompts.py` 采样逻辑 | 4 | `sample_random_positive_int` 恒正、`sample_prompt` 口径自洽、mean 远小于基底前缀时不死循环 |
| `test_benchmark_metrics.py` | `benchmark/metrics.py` 聚合 | 14 | 分位数、`_safe_div`/`_safe_stdev` 边界、全失败、`wall_clock=0`、错误码 JSON 安全 |
| `test_benchmark_runner.py` | `benchmark/runner.py` worker-pool 与单请求采集 | 18 | TTFT、reasoning_content 拼接、错误码从异常真实提取、`status_code==0` 不吞、`empty_response`、非 dict 采样参数不逃逸、在途峰值 == N（防批处理塌陷）、剩余预算超时、**H3 usage-only 末帧空 choices 不误判失败** |
| `test_benchmark_cli.py` | `benchmark/__main__.py` | 13 | `parse_metadata`（缺 `=` 友好报错）、`clean_filename`、数值参数非正值退出码 2、内建汇总键优先于 `--metadata` |
| `test_utils.py` | `src/llmperf/utils.py` | 2 | **H2 tokenizer 不在 import 期加载**（默认参数为 None）、注入 tokenizer 时口径自洽 |

## 异步测试约定

仓库**未安装** `pytest-asyncio`，也未配置 `asyncio_mode`。异步代码（`benchmark/runner.py`）的测试一律用 `asyncio.run(...)` 在普通同步 `def test_...()` 内驱动（零新增依赖）。见 `test_benchmark_runner.py` 顶部的 `_make_acompletion` / `_chunk` 假流构造器。

## 关键测试模式（供扩展参考）

- **假异步流**：用 `async def _gen(): yield ...` 构造 `async for` 可迭代对象，`monkeypatch.setattr(runner, "acompletion", ...)` 注入。
- **确定性 token 计数**：`monkeypatch.setattr(mod, "token_counter", lambda model, text: len(text.split()))`，1 词 = 1 token，使断言可精确。
- **时钟推进**：`monkeypatch.setattr(runner.time, "monotonic", _fake_monotonic)` 让每次调用 +1s，从而断言"剩余预算"随时间收缩。
- **Red → Green**：新报告的 bug 先写复现测试（确认 Red），再改实现（确认 Green），最后 `black` 格式化。

## 变更记录 (Changelog)

- 2026-07-19（高危修复补测）：新增 `test_utils.py`（2 例，覆盖 H2 import 期 tokenizer 惰性加载）；`test_benchmark_runner.py` 新增 1 例覆盖 H3（include_usage 末帧空 `choices` 不得把成功请求误判失败）。计数由 5 文件 55 例 → 6 文件 58 例。
- 2026-07-15（硬化补测）：`test_benchmark_runner.py`/`test_benchmark_cli.py` 随 A/B 两簇硬化扩充，benchmark 子模块测试达 43 函数 / 48 例。
