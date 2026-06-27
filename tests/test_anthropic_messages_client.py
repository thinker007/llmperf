"""Unit tests for the Anthropic Messages (/v1/messages) SSE parsing helpers.

These cover the pure parsing logic so we can validate streaming behaviour
(text deltas, thinking deltas, errors, control frames) without a live endpoint
or Ray runtime.
"""

from llmperf.ray_clients.anthropic_messages_client import (
    extract_delta_text,
    parse_data_line,
)


def test_parse_data_line_extracts_json_payload():
    chunk = b'data: {"type":"content_block_delta","delta":{"type":"text_delta","text":"Hi"}}'
    assert parse_data_line(chunk) == {
        "type": "content_block_delta",
        "delta": {"type": "text_delta", "text": "Hi"},
    }


def test_parse_data_line_ignores_event_ping_and_blank_lines():
    # Anthropic SSE interleaves `event:` lines, `ping` heartbeats and blanks.
    assert parse_data_line(b"event: content_block_delta") is None
    assert parse_data_line(b"") is None
    assert parse_data_line(b": ping") is None


def test_extract_delta_text_from_text_delta():
    data = {
        "type": "content_block_delta",
        "delta": {"type": "text_delta", "text": "Hello"},
    }
    assert extract_delta_text(data) == "Hello"


def test_extract_delta_text_from_thinking_delta():
    # Reasoning models stream their chain-of-thought as thinking_delta blocks;
    # we count these toward TTFT just like reasoning_content in the OpenAI client.
    data = {
        "type": "content_block_delta",
        "delta": {"type": "thinking_delta", "thinking": "let me think"},
    }
    assert extract_delta_text(data) == "let me think"


def test_extract_delta_text_ignores_control_frames():
    for data in (
        {"type": "message_start", "message": {"usage": {"input_tokens": 25}}},
        {"type": "content_block_start", "index": 0, "content_block": {"type": "text"}},
        {"type": "content_block_stop", "index": 0},
        {"type": "message_delta", "delta": {"stop_reason": "end_turn"}},
        {"type": "message_stop"},
    ):
        assert extract_delta_text(data) is None


def test_full_stream_accumulates_text_and_counts_tokens():
    """Replay a representative Anthropic SSE stream and assert the parsed result."""
    raw_stream = [
        b"event: message_start",
        b'data: {"type":"message_start","message":{"usage":{"input_tokens":25}}}',
        b"",
        b"event: content_block_delta",
        b'data: {"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"hmm "}}',
        b"",
        b"event: content_block_delta",
        b'data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hello"}}',
        b"",
        b"event: content_block_delta",
        b'data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":", world"}}',
        b"",
        b"event: message_delta",
        b'data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":15}}',
        b"",
        b"event: message_stop",
        b'data: {"type":"message_stop"}',
    ]

    generated_text = ""
    tokens_received = 0
    for chunk in raw_stream:
        data = parse_data_line(chunk.strip())
        if data is None:
            continue
        delta_text = extract_delta_text(data)
        if delta_text:
            generated_text += delta_text
            tokens_received += 1

    assert generated_text == "hmm Hello, world"
    assert tokens_received == 3


def test_full_stream_surfaces_error_frame():
    error_chunk = b'data: {"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}'
    data = parse_data_line(error_chunk)
    assert data["type"] == "error"
    assert data["error"]["type"] == "overloaded_error"
    assert data["error"]["message"] == "Overloaded"
