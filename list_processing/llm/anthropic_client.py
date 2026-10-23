from __future__ import annotations

import os
from typing import Any, Dict, Optional

from .base import LLMClient

_DEFAULT_MODEL = "claude-sonnet-4-5"
_DEFAULT_MAX_TOKENS = 4096
_VALID_EFFORTS = frozenset({"low", "medium", "high"})


def _resolve_effort(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    effort = value.strip().lower()
    if not effort:
        return None
    if effort not in _VALID_EFFORTS:
        raise ValueError(
            f"Invalid ANTHROPIC_EFFORT={value!r}; expected one of: low, medium, high"
        )
    return effort


class AnthropicClient(LLMClient):
    """
    LLMClient implementation that talks to the Anthropic (Claude) API.

    Configuration is read from:

    - ANTHROPIC_API_KEY (required)
    - ANTHROPIC_MODEL (optional, default claude-sonnet-4-5)
    - ANTHROPIC_EFFORT (optional: low | medium | high → output_config.effort)
    - ANTHROPIC_WORKSPACE_ID (optional; sent as anthropic-workspace-id header)
    """

    def __init__(
        self,
        *,
        api_key: Optional[str] = None,
        model: Optional[str] = None,
        effort: Optional[str] = None,
        max_tokens: int = _DEFAULT_MAX_TOKENS,
        max_concurrent_requests: int = 5,
    ) -> None:
        super().__init__(max_concurrent_requests=max_concurrent_requests)

        api_key = api_key or os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            raise RuntimeError("ANTHROPIC_API_KEY is not set")

        try:
            import anthropic
        except ImportError as exc:
            raise RuntimeError(
                "anthropic package is not installed; pip install anthropic"
            ) from exc

        workspace = os.environ.get("ANTHROPIC_WORKSPACE_ID")
        headers = {"anthropic-workspace-id": workspace} if workspace else None
        self._client = anthropic.Anthropic(api_key=api_key, default_headers=headers)
        self._model = model or os.environ.get("ANTHROPIC_MODEL", _DEFAULT_MODEL)
        raw_effort = effort if effort is not None else os.environ.get("ANTHROPIC_EFFORT")
        self._effort = _resolve_effort(raw_effort)
        self._max_tokens = max_tokens

    @property
    def model_name(self) -> str:
        return self._model

    @property
    def effort(self) -> Optional[str]:
        return self._effort

    def _raw_chat(
        self,
        system_prompt: str,
        user_prompt: str,
        response_format: Optional[Dict[str, Any]],
        temperature: float,
    ) -> str:
        system = system_prompt or ""
        # Anthropic has no OpenAI-style response_format=json_object; nudge via system text.
        if response_format and response_format.get("type") == "json_object":
            system = (
                f"{system}\n\n"
                "Respond with a single valid JSON object only. "
                "No markdown fences or extra commentary."
            ).strip()

        kwargs: Dict[str, Any] = {
            "model": self._model,
            "max_tokens": self._max_tokens,
            "messages": [{"role": "user", "content": user_prompt}],
        }
        if system:
            kwargs["system"] = system
        if self._effort:
            # Adaptive thinking / effort; omit temperature (API often rejects both).
            kwargs["output_config"] = {"effort": self._effort}
        else:
            kwargs["temperature"] = temperature

        response = self._client.messages.create(**kwargs)

        usage = getattr(response, "usage", None)
        if usage is not None:
            prompt_tokens = getattr(usage, "input_tokens", None)
            completion_tokens = getattr(usage, "output_tokens", None)
            cache_read = getattr(usage, "cache_read_input_tokens", None) or 0
            cache_create = getattr(usage, "cache_creation_input_tokens", None) or 0
            if isinstance(prompt_tokens, int):
                prompt_tokens = prompt_tokens + cache_read + cache_create
            self._record_usage(
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
            )

        parts: list[str] = []
        for block in response.content or []:
            if getattr(block, "type", None) == "text":
                parts.append(getattr(block, "text", "") or "")
        return "".join(parts)
