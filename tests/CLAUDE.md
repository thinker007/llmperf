[根目录](../CLAUDE.md) > **tests**

## 模块职责

`tests/` 是 `llmperf` 的 pytest 单元测试套件。**全部为纯函数级测试**：不连真实网络、不启动 Ray runtime，用 `monkeypatch` 替换外部依赖（如 `litellm.acompletion` / `litellm.token_counter` / `LlamaTokenizerFast`）。测试聚焦"协议解析 / 采样逻辑 / 指标聚合 / CLI 参数校验"这类容易出 bug、又难以对 Ray Actor 直接单测的部分——把易错逻辑抽成纯函数、再用普通 `assert` 覆盖，是本仓库反复出现的测试策略。

## 运行

```bash
python -m pytest tests/ -v
```

当前全绿：**11 个文件 / 114 个测试函数 / pytest 展开 128 例**（2026-11-21 实测核对）。

## 文件清单

| 文件 | 覆盖对象 | 函数数 | 要点 |
|---|---|---|---|
| `test_anthropic_messages_client.py` | `ray_clients/anthropic_messages_client.py` 的纯函数 `parse_data_line` / `extract_delta_text` | 7 | SSE 分帧：`startswith` 守卫、`event:`/心跳行返回 None、`thinking_delta` 拼接 |
| `test_benchmark_prompts.py` | `benchmark/prompts.py` 采样逻辑 | 5 | `sample_random_positive_int` 恒正、`sample_prompt` 口径自洽、mean 远小于基底前缀时不死循环、**mean&lt;=0 快速失败（M2，参数化）** |
| `test_benchmark_metrics.py` | `benchmark/metrics.py` 聚合 | 16 | 分位数、`_safe_div`/`_safe_stdev` 边界、全失败、`wall_clock=0`、错误码 JSON 安全、**开环 `scheduling_delay_s`/`achieved_requests_per_s` 条件扩展** |
| `test_benchmark_runner.py` | `benchmark/runner.py` worker-pool、开环调度、HTTP 直连分发、warmup 相 | 37 | TTFT、reasoning_content 拼接、错误码从异常真实提取、`status_code==0` 不吞、`empty_response`、非 dict 采样参数不逃逸、在途峰值 == N（防批处理塌陷）、剩余预算超时、**H3 usage-only 末帧空 choices 不误判失败**、**开环泊松调度 R1-R7**、**HTTP 直连分发/端到端/专属线程池尺寸**、**warmup 相请求计数/丢弃/计时分离（`BenchmarkConfig.num_warmup_requests`，库层可用但当前无 CLI 入口，见下方 ⚠️）** |
| `test_benchmark_cli.py` | `benchmark/__main__.py` | 12 | `parse_metadata`（缺 `=` 友好报错）、`clean_filename`、数值参数非正值退出码 2（参数化 ×9）、内建汇总键优先于 `--metadata`、`--request-rate`/`--api-base` 接线与回显 |
| `test_benchmark_http_client.py` | `benchmark/http_client.py`（HTTP 直连模式） | 10 | SSE 分帧、delta 提取（空 `choices` 安全返回 None）、`single_request_sync` 全路径（happy path/非 200/网络异常/空流/缺失鉴权环境变量） |
| `test_utils.py` | `src/llmperf/utils.py` | 4 | **H2 tokenizer 不在 import 期加载**（默认参数为 None）、注入 tokenizer 时口径自洽、**M2 死循环快速失败（参数化 + stddev=0 钳制）** |
| `test_common.py` | `src/llmperf/common.py` 客户端注册表 | 2 | **M4：`SUPPORTED_APIS` 与实际注册表同源**、未知 `llm_api` 报错 |
| `test_requests_launcher.py` | `src/llmperf/requests_launcher.py` 阻塞取结果 | 3 | **M2：空池阻塞快速失败（`StopIteration`）而非热自旋**、正常排空、非阻塞取结果 |
| `test_llm_correctness.py` | `llm_correctness.py` 除零防护 + 默认参数契约 | 4 | 超时零完成场景不崩溃、`error_rate`/`mismatch_rate` 归 0（对应上游 open issue #87 同族缺陷）；**默认 `llm_api` ∈ `SUPPORTED_APIS`（发现 D）**、docstring 不再提未注册的 `"chat"` |
| `test_token_benchmark_ray.py` | `token_benchmark_ray.py` 收尾段结果污染 + ITL 除数一致性 + 横幅转义 | 4 | 收尾段 `dict` 结果用 `append` 而非 `extend`，不被污染成键字符串列表；**主循环 ITL 用重算 token 数而非陈旧客户端计数（发现 A）**、主循环与收尾段 ITL 数值一致；**结果横幅不再含非法转义 `\R`（发现 E）** |

> ⚠️ **warmup CLI 接线当前处于未提交的回退状态**：`test_benchmark_runner.py` 里的 warmup 相测试（库层 `BenchmarkConfig.num_warmup_requests`）仍在且全绿，但 `test_benchmark_cli.py` 中原有的 `--num-warmup-requests` CLI 级用例已被工作区撤销——即 warmup 功能目前只能通过直接构造 `BenchmarkConfig` 使用，不接 CLI。详见根 `docs/code-summary.md` 文首提示。

## 异步测试约定

仓库**未安装** `pytest-asyncio`，也未配置 `asyncio_mode`。异步代码（`benchmark/runner.py`）的测试一律用 `asyncio.run(...)` 在普通同步 `def test_...()` 内驱动（零新增依赖）。见 `test_benchmark_runner.py` 顶部的 `_make_acompletion` / `_chunk` 假流构造器。

## 关键测试模式（供扩展参考）

- **假异步流**：用 `async def _gen(): yield ...` 构造 `async for` 可迭代对象，`monkeypatch.setattr(runner, "acompletion", ...)` 注入。
- **确定性 token 计数**：`monkeypatch.setattr(mod, "token_counter", lambda model, text: len(text.split()))`，1 词 = 1 token，使断言可精确。
- **时钟推进**：`monkeypatch.setattr(runner.time, "monotonic", _fake_monotonic)` 让每次调用 +1s，从而断言"剩余预算"随时间收缩。
- **Red → Green**：新报告的 bug 先写复现测试（确认 Red），再改实现（确认 Green），最后 `black` 格式化。

## 变更记录 (Changelog)

- 2026-11-21（`llm_correctness.py` 按密钥自动选协议）：`--llm-api` 默认改 `None`，复用 `common.resolve_llm_api(args.llm_api, os.environ)`；help 文案抽成 `common.LLM_API_HELP` 由两个脚本共用（DRY）。`test_llm_correctness.py` 新增 3 例（默认 `None`、接线、help 写明 openai/anthropic）。全仓 114 函数 / 128 例全绿；实测仅 `ANTHROPIC_*` → anthropic、仅 `OPENAI_*` → openai。注意：该脚本固定发 `temperature: 0.0`，只接受 `temperature=1` 的模型会 400，需 `--additional-sampling-params '{"temperature": 1}'`。
- 2026-11-21（Anthropic 自动路由）：`test_common.py` 新增 4 例（`resolve_llm_api` 显式优先 / 仅 `ANTHROPIC_API_KEY` → `anthropic` / 仅 `OPENAI_API_KEY` 或都没配 → `openai` / 两者都配保持 `openai`）；`test_anthropic_messages_client.py` 新增 3 例（`resolve_endpoint` 的 `ANTHROPIC_*` 优先、`OPENAI_*` 回退并去尾部 `/v1`、默认 base 与缺 key 报错）。计数 104→111 函数、118→125 例。
- 2026-09-10（审计发现 A/D/E 重做，逐条 TDD）：三处独立提交，每处 Red→实现→black→Green。**发现 A**：`token_benchmark_ray.py` 主循环用客户端回传的陈旧 `NUM_OUTPUT_TOKENS` 做 ITL 除数，收尾段用分词器重算值，两路口径不一致；抽取 `finalize_request_metrics()` 统一两条调用路径，`test_token_benchmark_ray.py` 新增 2 例（主循环用重算值、主循环与收尾段结果一致）。**发现 E**：结果横幅 `print(f"\Results ...")` 里 `\R` 是非法转义序列，改为 `\n`；新增 1 例断言 stdout 不含字面 `\Results` 且含真实换行。**发现 D**：`llm_correctness()` 默认 `llm_api="chat"` 不在 `SUPPORTED_APIS` 注册表里，直接调库函数不传参会被 `construct_clients` 拒绝；默认值改 `"openai"`，docstring 同步；`test_llm_correctness.py` 新增 2 例（断言"默认值 ∈ 注册表"契约本身 + docstring 不再提 `"chat"`）。计数 99→104 函数、113→118 例。
- 2026-07-26（核对+补录）：实测全量核对 `python -m pytest tests/ -v`，确认为 **11 文件 / 99 函数 / 113 例**全绿；本表此前遗漏 `test_llm_correctness.py`（2）与 `test_token_benchmark_ray.py`（1）两个既已存在的文件，一并补录；`test_benchmark_runner.py` 函数数按实测更正为 37（含 warmup 相测试）。同时记录一处工作区未提交状态：warmup 的 CLI 级测试已被撤销，见上方 ⚠️ 提示。
- 2026-07-22（HTTP 直连模式）：新增 `test_benchmark_http_client.py`（10 例，SSE 分帧 + `single_request_sync` 全路径）；`test_benchmark_runner.py` 新增 5 例（`_build_body` 同形回归、分发隔离、闭环+开环端到端假 HTTP 后端、专属线程池尺寸）；`test_benchmark_cli.py` 新增 2 例（`--api-base`/`--api-key-env` 接线与回显）。计数由 8 文件 73 函数/87 例 → 9 文件 90 函数/104 例。
- 2026-07-20（开环速率模式 + Ray 路线遗留收口）：`test_benchmark_metrics.py`/`test_benchmark_runner.py`/`test_benchmark_cli.py` 随开环请求速率模式扩充；新增 `test_common.py`（M4 注册表）、`test_requests_launcher.py`（M2 空池快速失败）收口上一会话遗留的 Red 测试。
- 2026-07-19（高危修复补测）：新增 `test_utils.py`（H2 import 期 tokenizer 惰性加载 + M2 死循环快速失败）；`test_benchmark_runner.py` 新增覆盖 H3（include_usage 末帧空 `choices` 不得把成功请求误判失败）。
- 2026-07-15（硬化补测）：`test_benchmark_runner.py`/`test_benchmark_cli.py` 随 A/B 两簇硬化扩充，benchmark 子模块测试达 43 函数 / 48 例。
