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
