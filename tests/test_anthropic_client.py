from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from campaign_pipeline.config import CampaignConfig
from campaign_pipeline.llm import create_llm
from list_processing.llm.anthropic_client import AnthropicClient


def test_create_llm_claude_requires_api_key(tmp_path, monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    cfg = CampaignConfig(campaign_dir=tmp_path, base_name="t", backend="claude")
    with pytest.raises(RuntimeError, match="ANTHROPIC_API_KEY"):
        create_llm(cfg)


def _fake_anthropic_sdk(fake_messages):
    fake_sdk = MagicMock()
    fake_sdk.Anthropic.return_value = SimpleNamespace(messages=fake_messages)
    return fake_sdk


def test_anthropic_client_chat_and_json_nudge(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
    monkeypatch.delenv("ANTHROPIC_EFFORT", raising=False)

    fake_response = SimpleNamespace(
        content=[SimpleNamespace(type="text", text='{"match_score": 4}')],
        usage=SimpleNamespace(
            input_tokens=10,
            output_tokens=5,
            cache_read_input_tokens=0,
            cache_creation_input_tokens=0,
        ),
    )
    fake_messages = MagicMock()
    fake_messages.create.return_value = fake_response

    with patch.dict("sys.modules", {"anthropic": _fake_anthropic_sdk(fake_messages)}):
        client = AnthropicClient(api_key="test-key", model="claude-sonnet-4-5")
        out = client.chat(
            system_prompt="You score domains.",
            user_prompt="Domain: example.com",
            response_format={"type": "json_object"},
            temperature=0,
        )

    assert out == '{"match_score": 4}'
    assert client.total_prompt_tokens == 10
    assert client.total_completion_tokens == 5
    kwargs = fake_messages.create.call_args.kwargs
    assert kwargs["model"] == "claude-sonnet-4-5"
    assert kwargs["temperature"] == 0
    assert "output_config" not in kwargs
    assert "JSON object" in kwargs["system"]
    assert kwargs["messages"] == [{"role": "user", "content": "Domain: example.com"}]


def test_anthropic_client_effort_from_env(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
    monkeypatch.setenv("ANTHROPIC_MODEL", "claude-sonnet-5-5")
    monkeypatch.setenv("ANTHROPIC_EFFORT", "low")

    fake_response = SimpleNamespace(
        content=[SimpleNamespace(type="text", text="{}")],
        usage=SimpleNamespace(
            input_tokens=1,
            output_tokens=1,
            cache_read_input_tokens=0,
            cache_creation_input_tokens=0,
        ),
    )
    fake_messages = MagicMock()
    fake_messages.create.return_value = fake_response

    with patch.dict("sys.modules", {"anthropic": _fake_anthropic_sdk(fake_messages)}):
        client = AnthropicClient()
        assert client.model_name == "claude-sonnet-5-5"
        assert client.effort == "low"
        client.chat(system_prompt="sys", user_prompt="user", temperature=0)

    kwargs = fake_messages.create.call_args.kwargs
    assert kwargs["output_config"] == {"effort": "low"}
    assert "temperature" not in kwargs


def test_anthropic_client_rejects_bad_effort(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
    with patch.dict("sys.modules", {"anthropic": _fake_anthropic_sdk(MagicMock())}):
        with pytest.raises(ValueError, match="ANTHROPIC_EFFORT"):
            AnthropicClient(effort="turbo")
