"""http_client.py 单测：纯函数 SSE 解析 + 同步请求函数（monkeypatch requests.post，无网络）。"""

import requests

from llmperf import common_metrics as cm
from llmperf.benchmark import http_client

# ---- T1: parse_sse_line ----


def test_parse_sse_line_extracts_json_payload():
    chunk = b'data: {"choices":[{"delta":{"content":"Hi"}}]}'
    assert http_client.parse_sse_line(chunk) == {
        "choices": [{"delta": {"content": "Hi"}}]
    }


def test_parse_sse_line_ignores_done_blank_and_non_data_lines():
    assert http_client.parse_sse_line(b"data: [DONE]") is None
    assert http_client.parse_sse_line(b"") is None
    assert http_client.parse_sse_line(b": ping") is None
    assert http_client.parse_sse_line(b"event: message") is None


# ---- T2: extract_delta_text ----


def test_extract_delta_text_prefers_reasoning_content():
    data = {"choices": [{"delta": {"reasoning_content": "think", "content": "answer"}}]}
    assert http_client.extract_delta_text(data) == "think"


def test_extract_delta_text_falls_back_to_content():
    data = {"choices": [{"delta": {"content": "hello"}}]}
    assert http_client.extract_delta_text(data) == "hello"


def test_extract_delta_text_empty_choices_returns_none():
    # H3 同款守卫：include_usage 末帧 choices=[]，不得 [0] 下标崩溃。
    assert http_client.extract_delta_text({"choices": []}) is None
    assert http_client.extract_delta_text({"usage": {"total_tokens": 10}}) is None


# ---- T4-T7: single_request_sync ----


class _FakeResponse:
    def __init__(self, status_code=200, lines=None, text=""):
        self.status_code = status_code
        self._lines = lines or []
        self.text = text

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def iter_lines(self, chunk_size=None):
        return iter(self._lines)

    def raise_for_status(self):
        if self.status_code != 200:
            raise requests.exceptions.HTTPError("%s error" % self.status_code)


def _sse(*pieces):
    return [
        ('data: {"choices":[{"delta":{"content":"%s"}}]}' % p).encode() for p in pieces
    ] + [b"data: [DONE]"]


def test_single_request_sync_happy_path(monkeypatch):
    captured = {}

    def _fake_post(url, **kwargs):
        captured.update(url=url, **kwargs)
        return _FakeResponse(lines=_sse("Hello", " world"))

    monkeypatch.setattr(http_client.requests, "post", _fake_post)
    monkeypatch.setattr(
        http_client, "token_counter", lambda model, text: len(text.split())
    )

    result = http_client.single_request_sync(
        model="my-model",
        api_base="http://localhost:8000/v1",
        api_key_env="MY_API_KEY",
        num_input_tokens=5,
        request_timeout=30.0,
        body={"model": "my-model", "messages": [], "stream": True},
    )

    assert result[cm.ERROR_CODE] is None
    assert result[cm.TTFT] > 0
    assert result[cm.NUM_OUTPUT_TOKENS] == 2  # "Hello world" -> 2 words
    assert result[cm.NUM_INPUT_TOKENS] == 5
    assert captured["url"] == "http://localhost:8000/v1/chat/completions"


def test_single_request_sync_non_200_captures_status_and_body(monkeypatch):
    monkeypatch.setattr(
        http_client.requests,
        "post",
        lambda url, **kw: _FakeResponse(status_code=500, text="internal error"),
    )
    monkeypatch.setattr(http_client, "token_counter", lambda model, text: 0)

    result = http_client.single_request_sync(
        model="my-model",
        api_base="http://localhost:8000/v1",
        api_key_env="MY_API_KEY",
        num_input_tokens=1,
        request_timeout=30.0,
        body={},
    )
    assert result[cm.ERROR_CODE] == "500"
    assert result[cm.ERROR_MSG] == "internal error"


def test_single_request_sync_network_exception_no_escape(monkeypatch):
    def _boom(url, **kw):
        raise requests.exceptions.ConnectionError("connection refused")

    monkeypatch.setattr(http_client.requests, "post", _boom)
    monkeypatch.setattr(http_client, "token_counter", lambda model, text: 0)

    result = http_client.single_request_sync(
        model="my-model",
        api_base="http://localhost:8000/v1",
        api_key_env="MY_API_KEY",
        num_input_tokens=1,
        request_timeout=30.0,
        body={},
    )
    assert result[cm.ERROR_CODE] == "unknown"
    assert "connection refused" in result[cm.ERROR_MSG]


def test_single_request_sync_empty_stream_marked_failed(monkeypatch):
    monkeypatch.setattr(
        http_client.requests,
        "post",
        lambda url, **kw: _FakeResponse(lines=[b"data: [DONE]"]),
    )
    monkeypatch.setattr(http_client, "token_counter", lambda model, text: 0)

    result = http_client.single_request_sync(
        model="my-model",
        api_base="http://localhost:8000/v1",
        api_key_env="MY_API_KEY",
        num_input_tokens=1,
        request_timeout=30.0,
        body={},
    )
    assert result[cm.ERROR_CODE] == "empty_response"
    assert result[cm.NUM_OUTPUT_TOKENS] == 0


def test_single_request_sync_missing_api_key_env_omits_auth_header(monkeypatch, capsys):
    monkeypatch.delenv("NO_SUCH_KEY_VAR", raising=False)
    captured = {}

    def _fake_post(url, **kwargs):
        captured.update(kwargs)
        return _FakeResponse(lines=_sse("hi"))

    monkeypatch.setattr(http_client.requests, "post", _fake_post)
    monkeypatch.setattr(http_client, "token_counter", lambda model, text: 1)

    http_client.single_request_sync(
        model="my-model",
        api_base="http://localhost:8000/v1",
        api_key_env="NO_SUCH_KEY_VAR",
        num_input_tokens=1,
        request_timeout=30.0,
        body={},
    )
    assert captured["headers"] == {}
    assert "NO_SUCH_KEY_VAR" in capsys.readouterr().err
