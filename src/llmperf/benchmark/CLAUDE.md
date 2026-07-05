[根目录](../../../CLAUDE.md) > [src](../../) > [llmperf](../) > **benchmark**

## 模块职责

**本模块已完工（计划 Task 1-5 全部完成，26 个测试全绿）。** `python -m llmperf.benchmark` 是可用的 CLI 入口。

在不引入 Ray/threading/pandas/transformers 的前提下，用 `asyncio` worker-pool 直接压测 `litellm.acompletion()`，保留 `token_benchmark_ray.py` 的度量灵魂（TTFT/ITL/E2E/吞吐/错误率），与旧的 Ray 压测路线**并存、零侵入**（不 import、不修改旧代码）。核心动机是"压测 LLM 是 I/O 密集型任务，Ray 的多进程并行毫无必要，asyncio 能更省更快地完成同一调度职责"。

设计与实施计划的完整文档：
- `docs/superpowers/specs/2026-06-28-litellm-benchmark-integration-design.md`（设计规格，Draft v5，含并发模型、指标换算公式、CLI 参数表、风险清单）
- `docs/superpowers/plans/2026-06-28-litellm-benchmark-integration-plan.md`（5 个 Task 的 TDD 实施计划，checkbox 已回填为完成状态）
- `docs/benchmark.md`（面向用户的使用文档，含与 Ray 版的口径差异说明）

## 当前实现状态

| 文件 | 状态 |
|---|---|
| `src/llmperf/benchmark/__init__.py` | **已完成** — 导出 `run_benchmark`/`summarize`/`BenchmarkConfig` |
| `src/llmperf/benchmark/sonnet.txt` | **已完成** — 从 `../sonnet.txt` 复制的采样语料 |
| `src/llmperf/benchmark/prompts.py` | **已完成**（Task 1） |
| `src/llmperf/benchmark/metrics.py` | **已完成**（Task 2） |
| `src/llmperf/benchmark/runner.py` | **已完成**（Task 3） |
| `src/llmperf/benchmark/__main__.py` | **已完成**（Task 4） |
| `tests/test_benchmark_prompts.py` | **已完成** — 4 用例 |
| `tests/test_benchmark_metrics.py` | **已完成** — 13 用例 |
| `tests/test_benchmark_runner.py` | **已完成** — 5 用例 |
| `tests/test_benchmark_cli.py` | **已完成** — 4 用例 |
| `docs/benchmark.md` | **已完成**（Task 5） |

## 入口与启动

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

不传 `--results-dir` 时只把 summary 打到 stdout；传了则额外落盘 `<model>_<in>_<out>_summary.json` 与 `..._individual_responses.json`。也可以直接 `from llmperf.benchmark import run_benchmark, summarize, BenchmarkConfig` 作为库使用。

## 对外接口

### `prompts.py`
```python
def sample_random_positive_int(mean: int, stddev: int) -> int
def sample_output_len(mean_output_tokens: int, stddev_output_tokens: int) -> int
def sample_prompt(model: str, mean_input_tokens: int, stddev_input_tokens: int, expect_output_tokens: int) -> Tuple[str, int]
```
- 逻辑移植自 `../utils.py::randomly_sample_sonnet_lines_prompt`（同样是"高斯采样目标 token 数 → 从打乱的 sonnet 行里拼接 → 超出部分截断最后一行"的算法），但把固定的 `LlamaTokenizerFast` 分词器换成了 `litellm.token_counter(model=model, text=text)`——按目标模型精确计费 token，而不是用统一分词器保证跨模型文本等长。
- **口径差异（有意为之，非 bug）**：`sample_prompt()` 返回的 token 数是对**最终拼接完成的文本**重新用 `token_counter` 算一遍的结果，而不是采样时的目标值。
- **`stddev_input_tokens == 0` 的收敛保护**：`random.gauss(mean, 0)` 是确定性的，如果这个确定性采样值小于 prompt 基底前缀本身的 token 数，原版 `utils.py` 那种"不断重采样直到超过阈值"的 while 循环会**死循环**（这是从原始 llmperf 代码原样移植过来的既有 bug，本次实现测试时才被 `test_sample_prompt_converges_with_small_mean` 触发暴露）。代码对 `stddev == 0` 做了 `max(num_prompt_tokens, get_len(prompt))` 钳制而非进入 while 循环；`stddev > 0` 时行为与原版一致。改动这段逻辑时务必保留这个分支，否则会复现挂死。

### `metrics.py`
```python
def derive_request_metrics(raw: Dict[str, Any]) -> Dict[str, Any]
def summarize(results: List[Dict[str, Any]], wall_clock_s: float) -> Dict[str, Any]
# 内部工具（下划线前缀，仍可直接测）：
def _safe_div(numerator: float, denominator: float) -> Optional[float]
def _safe_stdev(data: List[float]) -> Optional[float]
def _safe_quantiles(data: List[float]) -> Dict[str, Optional[float]]
```
- `derive_request_metrics(raw)`：纯函数，补齐三个派生字段：`INTER_TOKEN_LAT`（口径与旧 Ray 路线不同，见下）、`REQ_OUTPUT_THROUGHPUT = n_out/e2e`（`e2e<=0` 时为 0）、`NUM_TOTAL_TOKENS = n_in+n_out`。
- `summarize(results, wall_clock_s)`：过滤出成功请求，算 6 个 key 的 `mean`/`min`/`max`/`stddev`/`p25~p99`，再算整体 `error_rate`/`error_code_frequency`/吞吐/每分钟完成数，全部做了除零保护。
- `_safe_quantiles`/`_safe_stdev`/`_safe_div` 把 `statistics` 在样本数 `<2` 时抛异常的边界统一收敛掉（DRY）。
- 所有 key 名从 `../common_metrics.py` import，不重新定义字符串字面量。

### `runner.py`
```python
@dataclass class BenchmarkConfig  # model, mean/stddev_input_tokens, mean/stddev_output_tokens,
                                    # num_concurrent_requests=10, max_num_completed_requests=10,
                                    # timeout=90, additional_sampling_params={}
async def run_benchmark(config: BenchmarkConfig) -> Tuple[List[Dict[str, Any]], float]
async def _single_request(config, prompt_text, num_input_tokens, sampled_max_tokens) -> Dict[str, Any]
```
- **并发模型：worker-pool，钉死不用 `gather` 批处理**。`run_benchmark` 预生成恰好 `max_num_completed_requests` 条 prompt 放进有界 `asyncio.Queue`，`num_concurrent_requests` 个常驻协程各自死循环 `queue.get_nowait()` 取任务，任一请求完成立刻取下一个。这直接避免了旧 Ray 路线历史上真实踩过的 `#59` bug（commit `f1d6bed`）——"凑够一批再一起等"的语义下，在途请求数会从 N 塌陷到 1 再回弹，拖慢吞吐。`tests/test_benchmark_runner.py::test_run_benchmark_concurrency_stays_at_n` 专门验证"队列有货时持续保持 == N"（不只是"峰值 ≤ N"这种弱断言）。
- `--timeout` 到点后 worker 停止取新任务，但不 cancel 在途请求，等其自然完成后计入统计；同时给 `acompletion` 透传 HTTP 层 `timeout=config.timeout` 兜底，防止单个永久 hang 的请求让 `gather` 永不返回。
- `_single_request` 的流式采集骨架照搬 `ray_clients/litellm_client.py:61-103`（只把同步 `for` 换成 `async for`），但**修正**了原代码的错误吞噬 bug：`except Exception as e` 里从 `e` 真实提取 `error_msg`/`error_code`（`str(getattr(e, "status_code", None) or "unknown")`），而不是原代码写死的常量。
- `reasoning_content`（推理模型的思维链）与 `content` 都计入 `generated_text` 和 TTFT 判定——对应仓库历史上 `80a6566` 修的"流式下没提取 reasoning_content 导致 TTFT 算错"那个坑，这里从一开始就做对。

### `__main__.py` / `__init__.py`
```python
def parse_metadata(raw: str) -> Dict[str, str]      # "a=1,b=x=y" → {"a":"1","b":"x=y"}；split("=",1)，重复 key 后覆盖
def clean_filename(model: str, mean_input: int, mean_output: int) -> str  # 照搬 token_benchmark_ray.py 的两步正则清洗
def main(argv: Optional[List[str]] = None) -> None
```
- CLI 参数与旧 Ray 版对齐（降低迁移成本），`--additional-sampling-params` 用 `type=json.loads`，非法 JSON 由 argparse 直接报错退出（`SystemExit`），不需要手写校验。
- `__init__.py` 导出 `run_benchmark`/`summarize`/`BenchmarkConfig`，可作为库直接 `from llmperf.benchmark import ...` 使用而不经过 CLI。

## 关键依赖与配置

**零新增依赖**：只用 `litellm`（已是仓库现有依赖，提供 `acompletion`/`token_counter`）+ Python 标准库（`asyncio`/`statistics`/`math`/`pathlib`/`random`）。明确**不**在本模块内 import：`ray`、`threading`、`transformers`、`pandas`、`boto3`。

Python 版本约束（继承自仓库根 `pyproject.toml`）：`>=3.8, <3.11`，因此类型注解**必须**使用 `typing.List/Dict/Tuple/Optional`，**禁止**使用 3.10+ 语法（`list[str]`、`X | None`）——全部四个文件均已遵守。

## 数据模型

```
sample_prompt(...) -> (prompt_text: str, num_input_tokens: int)

单请求 raw dict（_single_request 产出，derive_request_metrics 的入参）：
  {ttft_s, end_to_end_latency_s, number_input_tokens, number_output_tokens,
   error_code, error_msg}
derive_request_metrics 输出：raw 浅拷贝 + {inter_token_latency_s, request_output_throughput_token_per_s, number_total_tokens}
summarize 输出：
  6 个 quantile key 各自 {quantiles:{p25..p99}, mean, min, max, stddev}
  + num_requests_started / number_errors / error_rate / error_code_frequency(Dict[str,int])
  + mean_output_throughput_token_per_s / num_completed_requests / num_completed_requests_per_min

BenchmarkConfig: model, mean_input_tokens=550, stddev_input_tokens=150,
                 mean_output_tokens=150, stddev_output_tokens=80,
                 num_concurrent_requests=10, max_num_completed_requests=10,
                 timeout=90, additional_sampling_params={}
run_benchmark(config) -> (results: List[Dict], wall_clock_s: float)
```

**与旧 Ray 路线的关键指标口径差异（已生效，非纸面设计）**：

- **Inter-Token Latency**：旧版 `token_benchmark_ray.py` 用 `sum(time_to_next_token) / chunk数`（首项混入 TTFT，分母是 chunk 数而非 token 数，语义不纯净）；本模块用 `(E2E_LAT - TTFT) / (NUM_OUTPUT_TOKENS - 1)`（`NUM_OUTPUT_TOKENS <= 1` 时为 0）。**两条路线的 ITL 数值不可直接比较**，`docs/benchmark.md` 用 ⚠️ 标出。修改这个公式前先读设计规格 §7 决策 2。
- **并发模型**：旧版"派发一批 N 个再一起等"（历史上因此产生过 #59 并发数塌陷 bug）；本模块钉死用 worker-pool，在途请求数稳定等于 N。

## 测试与质量

| 测试文件 | 用例数 | 覆盖点 |
|---|---|---|
| `tests/test_benchmark_prompts.py` | 4 | 高斯重采样必为正数、`sample_output_len` 恒正、`sample_prompt` 口径自洽、小均值场景下的收敛性（含 `stddev=0` 边界，曾在实现时触发死循环 bug） |
| `tests/test_benchmark_metrics.py` | 13 | 换算正确性、`_safe_div`/`_safe_stdev`/`_safe_quantiles` 全部除零与 n<2 边界、`summarize` 成功/失败分流、全失败场景、`wall_clock_s==0`、`error_code` 类型归一为 `str` |
| `tests/test_benchmark_runner.py` | 5 | TTFT/文本采集、`reasoning_content` 计入、错误从异常真实提取、有界队列软上限、**worker-pool 并发稳定性**（峰值==N 且持续保持==N，不塌陷） |
| `tests/test_benchmark_cli.py` | 4 | `--metadata` 解析（`split("=",1)`、重复 key 覆盖）、文件名清洗、非法 JSON 报错退出 |

全部 26 个用例无需网络（`prompts`/`metrics` 是纯函数；`runner`/`cli` 用 `monkeypatch` 注入假的 `acompletion`/`token_counter`）。运行：
```bash
python -m pytest tests/test_benchmark_prompts.py tests/test_benchmark_metrics.py tests/test_benchmark_runner.py tests/test_benchmark_cli.py -v
```
异步测试用 `asyncio.run()` 在普通同步 `def test_...()` 函数内驱动，**不引入 `pytest-asyncio`**（仓库未装，零新增依赖）。

## 常见问题 (FAQ)

- **Q: 为什么并发模型明确禁止用 `asyncio.gather` 一次性批处理 N 个请求？**
  A: 旧 Ray 路线历史上真实踩过这个坑（git log `#59`）——"凑够一批再等全部完成"的语义下，只要批里有一个慢请求，在途请求数就会从 N 塌陷到 1、拖慢总吞吐。worker-pool 模型让 N 个常驻协程各自从共享队列取任务，任一完成立刻取下一个，在途数稳定等于 N。
- **Q: `metrics.py` 里 ITL 的公式和 `ray_clients/` 目录里客户端算出来的 ITL 是同一个东西吗？**
  A: 不是，字段名相同（都叫 `inter_token_latency_s`）但计算口径不同，两者数值**不可以**跨路线直接比较。
- **Q: `sonnet.txt` 在 `benchmark/` 目录下有一份副本，而不是复用 `../sonnet.txt`，会不会漂移？**
  A: 设计上刻意让本模块自包含，保证"零侵入、可独立演进"。代价是两份语料需要手动保持同步，没有自动化校验。
- **Q: `sample_prompt` 那个 `stddev==0` 的钳制是必要的吗，能不能删掉简化代码？**
  A: 不能删。删掉后 `mean` 小于 prompt 基底前缀 token 数、且 `stddev==0` 时会死循环（`random.gauss(mean, 0)` 确定性地返回同一个值，永远达不到阈值）。这是从原始 `utils.py` 移植时发现的既有 bug，测试 `test_sample_prompt_converges_with_small_mean` 专门守护这个边界。

## 相关文件清单

- `src/llmperf/benchmark/__init__.py`
- `src/llmperf/benchmark/prompts.py`
- `src/llmperf/benchmark/metrics.py`
- `src/llmperf/benchmark/runner.py`
- `src/llmperf/benchmark/__main__.py`
- `src/llmperf/benchmark/sonnet.txt`
- `tests/test_benchmark_prompts.py`
- `tests/test_benchmark_metrics.py`
- `tests/test_benchmark_runner.py`
- `tests/test_benchmark_cli.py`
- `docs/benchmark.md`（用户文档）
- `docs/superpowers/specs/2026-06-28-litellm-benchmark-integration-design.md`（设计规格）
- `docs/superpowers/plans/2026-06-28-litellm-benchmark-integration-plan.md`（实施计划，已全部完成）

## 变更记录 (Changelog)

- 2026-07-05：Task 3（`runner.py`）、Task 4（`__main__.py`/`__init__.py`）、Task 5（`docs/benchmark.md`）全部完成，26 个测试全绿，模块正式完工。记录 `prompts.py` 的 `stddev==0` 死循环 bug 修复。
- 2026-07-05（第二次扫描）：核实 `metrics.py` 与 `tests/test_benchmark_metrics.py` 已完成。
- 2026-07-05（首次扫描）：首次生成本模块文档，标注当时为半成品状态（仅 Task 1 完成）。
