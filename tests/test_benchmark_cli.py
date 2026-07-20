"""CLI 解析单测：metadata/JSON 边界 + 文件名清洗。无网络。"""

import argparse
import json

import pytest

from llmperf.benchmark import __main__ as cli


def test_parse_metadata_splits_on_first_equals():
    # val 可含 '='；重复 key 后者覆盖（spec I3）。
    assert cli.parse_metadata("a=1,b=x=y,a=2") == {"a": "2", "b": "x=y"}


def test_parse_metadata_empty():
    assert cli.parse_metadata("") == {}


def test_parse_metadata_missing_equals_raises():
    with pytest.raises(argparse.ArgumentTypeError):
        cli.parse_metadata("foo")


def test_clean_filename_replaces_special_chars():
    # 照搬 token_benchmark_ray.py:339-341 两步正则。
    assert cli.clean_filename("openai/gpt-4o", 550, 150) == "openai-gpt-4o_550_150"


def test_additional_sampling_params_invalid_json_exits():
    # type=json.loads 让 argparse 对非法 JSON 直接报错退出（spec I4）。
    with pytest.raises(SystemExit):
        cli.main(["--model", "gpt-4o", "--additional-sampling-params", "{not json}"])


@pytest.mark.parametrize("bad", ["[1,2]", "5", '"x"', "true"])
def test_additional_sampling_params_non_object_exits(bad):
    # 合法 JSON 但非对象（list/int/str/bool）必须友好报错退出（退出码 2），
    # 而不是让下游 dict.update 抛 TypeError 崩掉整个压测池（评审 finding #1）。
    with pytest.raises(SystemExit) as exc:
        cli.main(["--model", "gpt-4o", "--additional-sampling-params", bad])
    assert exc.value.code == 2


def test_metadata_does_not_override_builtin_keys(monkeypatch, capsys):
    async def _fake_run_benchmark(config):
        return ([], 0.0)

    monkeypatch.setattr(cli, "run_benchmark", _fake_run_benchmark)

    cli.main(
        [
            "--model",
            "real-model",
            "--metadata",
            "model=fake,extra=ok",
        ]
    )
    out, err = capsys.readouterr()
    summary = json.loads(out)
    assert summary["model"] == "real-model"  # 内建键未被覆盖
    assert summary["extra"] == "ok"  # 非冲突键写入
    assert "model" in err and "ignored" in err  # stderr 有冲突 warning


@pytest.mark.parametrize(
    "bad_args",
    [
        ["--num-concurrent-requests", "0"],
        ["--max-num-completed-requests", "-1"],
        ["--timeout", "0"],
        # M2：mean<=0 会让 sample_random_positive_int 死循环（stddev=0 时必然），
        # 必须在 CLI 层拦截；stddev 为负同样没有意义。
        ["--mean-input-tokens", "0"],
        ["--mean-output-tokens", "-5"],
        ["--stddev-input-tokens", "-1"],
        ["--stddev-output-tokens", "-1"],
        # C1（spec 2026-07-20 §5）：开环速率必须为正。
        ["--request-rate", "0"],
        ["--request-rate", "-0.5"],
    ],
)
def test_main_rejects_non_positive_numeric_args(bad_args):
    with pytest.raises(SystemExit) as exc:
        cli.main(["--model", "m"] + bad_args)
    assert exc.value.code == 2  # parser.error 退出码


# ---- 开环速率模式（spec 2026-07-20 §5/§6 C2-C3） ----


def test_request_rate_wired_into_config_and_summary(monkeypatch, capsys):
    # C2：--request-rate 流入 BenchmarkConfig 且 summary 回显。
    captured = {}

    async def _fake_run_benchmark(config):
        captured["config"] = config
        return ([], 0.0)

    monkeypatch.setattr(cli, "run_benchmark", _fake_run_benchmark)
    cli.main(["--model", "m", "--request-rate", "2.5"])
    assert captured["config"].request_rate == 2.5
    summary = json.loads(capsys.readouterr().out)
    assert summary["request_rate"] == 2.5


def test_request_rate_default_none_and_absent_from_summary(monkeypatch, capsys):
    # C3：不传 → config 为 None，summary 无 request_rate 键（闭环零侵入）。
    captured = {}

    async def _fake_run_benchmark(config):
        captured["config"] = config
        return ([], 0.0)

    monkeypatch.setattr(cli, "run_benchmark", _fake_run_benchmark)
    cli.main(["--model", "m"])
    assert captured["config"].request_rate is None
    summary = json.loads(capsys.readouterr().out)
    assert "request_rate" not in summary
