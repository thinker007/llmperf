# Spec: `benchmark` 生产可用性硬化迭代（A + B）

- 日期：2026-07-15
- 范围：`src/llmperf/benchmark/`（runner / prompts / metrics / \_\_main\_\_）
- 类型：纯增量硬化，**不改架构、不改 summary/individual JSON 输出结构**
- 前置：`benchmark` 模块已完工（26 测试全绿），本轮在其上叠加运行时鲁棒性与健壮性边界

## 1. 目标与非目标

### 目标
把 `benchmark` 从"happy-path 能跑通"推进到"生产可用"：真实 litellm 环境下不因对象结构差异全量报错、不因单点异常崩溃丢结果、不把污染数据混入统计；CLI 边界给出友好报错而非裸 traceback。

本轮覆盖两簇（源自 2026-07-15 只读勘查报告）：

- **A 簇 — 运行时正确性**（高优先，可能线上每请求皆错 / 崩溃丢结果）
- **B 簇 — 健壮性边界**（中优先，用户可见的粗糙）

### 非目标（本轮明确不做）
C 簇纯优化，留待下一轮，避免本轮 diff 掺入无关重构：

- `prompts.py` 语料每请求重复读盘 + 洗牌 → 模块级缓存
- `prompts.py` 逐行 `token_counter` 的 O(行数) 分词开销
- `metrics.py::_safe_stdev` 返回类型三态（`None`/`0`/`float`）统一
- `prompts.py:37` E731（lambda 赋值给变量）改内嵌 `def`
- `prompts.py` 中 `expect_output_tokens` 用均值、`max_tokens` 用采样值的口径不一致

## 2. 开发方式

全程 TDD（对齐仓库既有约定与 CLAUDE.md）：每项先写 Red 复现测试 → 实现到 Green → `black` 格式化 → commit。异步路径用 `asyncio.run()` 在同步 `def test_...()` 内驱动（仓库无 `pytest-asyncio`，零新增依赖）。新解析/提取逻辑优先抽成纯函数直接单测（对齐 `anthropic_messages_client.py::parse_data_line` 范例）。

类型注解遵守 `<3.11` 约束：`typing.List/Dict/Tuple/Optional`，禁止 `list[str]` / `X | None`。

## 3. A 簇 — 运行时正确性

### A1｜delta 防御式提取（根治被 mock 掩盖的最高优先隐患）
**问题**：`runner.py:63-71` 靠 `tok.choices[0].delta.get("content", None)` 取增量。测试用 dict 子类 `_Delta` 所以 `.get` 可用；真实 litellm 较新版本里 `delta` 是 pydantic 对象，`.get` 可能抛 `AttributeError` → 落入 `except` → **每条请求被记成错误**。

**方案**：新增模块级纯函数

```python
def extract_content(delta: Any) -> Optional[str]:
    """从 litellm 流式 delta 取增量文本，兼容 dict 与 pydantic 对象。
    优先 reasoning_content，其次 content（保留 runner 原语义）。"""
```

- 取值逻辑：`dict`（含子类）走 `delta.get(key)`，否则 `getattr(delta, key, None)`；对 `reasoning_content` 与 `content` 两个 key 各取一次，返回 `reasoning_content or content`（保留 `runner.py:68-71` 的优先级）。
- `_single_request` 的采集循环改调 `extract_content(delta)`，TTFT 触发与文本累加都基于其返回值，消除对 `.get` 的直接依赖。

**测试**：`extract_content` 对以下形态各断言一次 —
- dict：`{"content": "x"}` → `"x"`
- dict：`{"reasoning_content": "r", "content": "x"}` → `"r"`（优先级）
- 对象（pydantic-like，仅 `getattr` 可取，无 `.get`）：`content="x"` → `"x"`
- 空/无字段：`{}` 或空对象 → `None`

### A2｜`_single_request` 绝不外抛（根治崩溃 + 丢结果 + worker 无隔离）
**问题**：`runner.py:77` 的 `num_output_tokens = token_counter(...)` 在 `try/except`（59-75）**之外**。若 `token_counter` 抛异常（非法 model、分词器加载失败等），异常冒出 `worker` → `asyncio.gather`（122）抛出 → `run_benchmark` 整体崩溃，**已完成的 results 全部丢失**。

**方案**：保证 `_single_request` 在任何情况下都返回结果 dict、绝不向外抛异常。将 `token_counter` 计算纳入统一兜底（放进 `try`，或对其单独兜底并使 `num_output_tokens` 退化为 `0` 并置 `error_code`）。实现取"整体不外抛"语义：任何异常 → 返回带 `error_code` 的结果 dict，`worker`/`gather` 永不见异常。这一条同时消除勘查 §4 "worker 无请求级隔离"的隐患。

**测试**：`token_counter` 被 monkeypatch 成抛异常 → `run_benchmark`（或直接 `_single_request`）不崩、该请求结果 `error_code` 非 `None`、且同批其余正常请求结果**仍被保留**（断言 results 长度与成功计数）。

### A3｜空流判失败（避免污染分位统计）
**问题**：若 `acompletion` 正常返回但流为空 / 无 content，无异常抛出 → `error_code` 保持 `None` → `metrics.py:71-73` 按 `error_code is None` 把它计入"成功"，带 `ttft=0`、`num_output_tokens=0` 进入 TTFT / 吞吐分位数，污染统计。

**方案**：流正常结束后，若 `generated_text == ""` 且 `error_code is None`，显式置 `error_code = "empty_response"`。该请求随即被 `summarize` 归为错误、计入 `error_code_freq`，不再进入成功分位。

**决策记录**：这是一次**有意的语义收紧**——"返回但零 token"从成功改判为失败。已获确认。

**测试**：mock `acompletion` 返回一个产出零 content 的空流 → 结果 `error_code == "empty_response"`；再经 `summarize` 断言其不在成功分位、且计入 `num_errors` / `error_code_freq["empty_response"]`。

### A4｜`status_code` 顺手修（`or` 吞掉合法 0）
**问题**：`runner.py:75` `str(getattr(e, "status_code", None) or "unknown")`，当 `status_code == 0`（falsy）被误判为 `"unknown"`。

**方案**：改为显式判 `None`：

```python
sc = getattr(e, "status_code", None)
error_code = "unknown" if sc is None else str(sc)
```

**测试**：构造带 `status_code = 0` 的异常对象 → `error_code == "0"`（非 `"unknown"`）。可对 `_single_request` 直接驱动断言。

## 4. B 簇 — 健壮性边界

### B1｜`parse_metadata` 友好报错
**问题**：`__main__.py:23` `key, value = item.split("=", 1)`，若某项不含 `=`（如 `--metadata "foo"`）解包抛 `ValueError`，用户见裸 traceback。且 `parse_metadata` 在 argparse 之后手工调用（57-58），不享受 argparse 的友好报错通道。

**方案**：
- `parse_metadata` 内检测无 `=` 的项，抛 `argparse.ArgumentTypeError`，消息形如 `metadata item 'foo' must be key=value`。
- CLI 改为 `p.add_argument("--metadata", type=parse_metadata, default={})`，交由 argparse 统一捕获并以退出码 2 + 友好提示呈现（对齐 `--additional-sampling-params` 的非法 JSON 体验）。
- `main()` 中 `args.metadata` 自此已是 `Dict`，删去手工 `parse_metadata(args.metadata)` 调用。

**测试**：
- `parse_metadata("foo")` → 抛 `argparse.ArgumentTypeError`
- `parse_metadata("a=1,b=x=y")` → `{"a": "1", "b": "x=y"}`（回归，保留首个 `=` 分割语义）
- `parse_metadata("")` → `{}`（回归）

### B2｜单请求 timeout 内部派生（消除 wall clock 2×）
**问题**：`runner.py:48-49` 透传给 `acompletion` 的 HTTP `timeout` 直接用 `config.timeout`（= 整场 wall 预算）。叠加 worker "到点不 cancel 在途请求"（112-113），最坏 wall clock ≈ 2×timeout，且单请求无独立兜底。

**方案**（用户选定"内部派生、不加 CLI 参数、无魔法数"）：`acompletion` 的 `timeout` 取**当前剩余全局预算**：

- 给 `_single_request` 增加入参 `request_timeout: float`。
- `worker` 在调用前用其持有的 `start` 计算 `remaining = max(0.0, config.timeout - (time.monotonic() - start))`，传入。
- `body["timeout"] = request_timeout`。

任何单请求都不会越过全局 deadline，最坏 wall clock 压回约 1×timeout；不引入任何新常量或新 CLI 参数（符合 KISS）。

**测试**：monkeypatch `acompletion` 记录收到的 `timeout` kwarg；构造 `config.timeout` 较小的一次运行，断言透传的 `timeout` ≤ `config.timeout` 且随 elapsed 递减（或在人为推进时间下 ≤ 剩余预算）。用 monkeypatch `time.monotonic` 制造确定性 elapsed。

### B3｜metadata 冲突：内建键优先 + warning
**问题**：`__main__.py:78` `summary.update(user_metadata)` 在 summary 组装完成后合并，用户 `--metadata "model=x"` 会**静默覆盖**内建 `model`/`timestamp`/`num_concurrent_requests`/`litellm_version`，报告里记录的 model 可能非真正压测对象，事后无从分辨。

**方案**（守住"报告里的 model/timestamp 一定为真"这条底线，且不改 JSON 结构）：把无条件 `update` 换成逐键合并——

```python
for k, v in user_metadata.items():
    if k in summary:
        print(f"warning: metadata key '{k}' conflicts with a built-in "
              f"field and was ignored", file=sys.stderr)
        continue
    summary[k] = v
```

内建键永不被覆盖；用户仍可自由添加**不冲突**的自定义键。

**测试**：以 `--metadata "model=fake,extra=ok"` 跑 `main`（mock 掉 `run_benchmark`）→ 断言 `summary["model"]` 为真实 `--model` 值（未被覆盖）、`summary["extra"] == "ok"`、且 stderr 含冲突 warning（用 `capsys` 捕获）。

### B4｜非法数值 sanity 校验
**问题**：`num_concurrent_requests` / `max_num_completed_requests` 传 0 或负数不报错 → 队列预填 0 条或 worker 数为 0 → 空结果 → `summarize` 全 `None`，静默产出空报告而非提示。

**方案**：`main()` 在组装 config 前校验，违规走 `parser.error(msg)`（退出码 2 + 友好信息）：

- `num_concurrent_requests >= 1`
- `max_num_completed_requests >= 1`
- `timeout > 0`

**测试**：`main(["--model", "m", "--num-concurrent-requests", "0"])` → `SystemExit`（code 2）；`--max-num-completed-requests -1`、`--timeout 0` 各一条同类断言。

## 5. 测试矩阵总览

| 编号 | 测试 | 守护 | 文件（建议） |
|---|---|---|---|
| A1 | `extract_content` dict / reasoning 优先 / 对象 / 空 四形态 | 防御式提取 | `test_benchmark_runner.py` |
| A2 | `token_counter` 抛异常 → 记 error、不崩、余者保留 | 不外抛 | `test_benchmark_runner.py` |
| A3 | 空流 → `error_code=="empty_response"`、不入成功分位 | 空流判失败 | `test_benchmark_runner.py` + `test_benchmark_metrics.py` |
| A4 | `status_code==0` → `"0"` 非 `"unknown"` | 0 不被吞 | `test_benchmark_runner.py` |
| B1 | `parse_metadata` 无 `=` 抛 `ArgumentTypeError` + 两条回归 | 友好报错 | `test_benchmark_cli.py` |
| B2 | 透传 `timeout` ≤ 剩余预算 | timeout 派生 | `test_benchmark_runner.py` |
| B3 | `--metadata model=x` 不覆盖内建 + warning | 内建键优先 | `test_benchmark_cli.py` |
| B4 | `num_concurrent=0` / `max_num=-1` / `timeout=0` → `SystemExit(2)` | sanity 校验 | `test_benchmark_cli.py` |

预计测试数从 26 增至约 34+。

## 6. 影响面与兼容性

- **输出结构**：summary / individual JSON 的键集合与形状**不变**（B3 只是不再允许覆盖，B4 只是提前拒绝非法输入）。下游读取脚本零改动。
- **CLI 表面**：无新增/删除参数；`--metadata` 由手工解析改为 argparse `type`，非法输入行为从"崩溃"变为"友好退出码 2"。
- **行为语义变化（需知悉）**：
  1. 空流请求从"成功"改判为"失败"（A3）——统计口径收紧，历史对比需注意。
  2. 单请求 HTTP timeout 从"全局 timeout"改为"剩余预算"（B2）——极长单请求会更早被 HTTP 层中断。
- **依赖**：零新增（`sys` 为标准库；测试用 `capsys`/`monkeypatch`，均 pytest 内置）。

## 7. 提交切分（建议）

按 A1→A2→A3→A4→B1→B2→B3→B4 逐项 Red→Green→`black`→commit，每项一次原子提交，便于回溯与 review。完成后同步更新 `src/llmperf/benchmark/CLAUDE.md`（测试数、语义变化）与 `docs/benchmark.md`（空流判失败、timeout 语义），并回填本 spec 状态。
