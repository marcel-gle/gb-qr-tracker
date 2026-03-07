from __future__ import annotations

import os
from typing import Optional, Dict, Any

from openai import OpenAI

from .base import LLMClient


class OpenAIClient(LLMClient):
    """
    LLMClient implementation that talks to the OpenAI API (or compatible
    hosted endpoints).

    Configuration is read from:

    - OPENAI_API_KEY
    - OPENAI_MODEL (optional, otherwise a sensible default is used)
    """

    def __init__(
        self,
        *,
        api_key: Optional[str] = None,
        model: Optional[str] = None,
        max_concurrent_requests: int = 5,
    ) -> None:
        super().__init__(max_concurrent_requests=max_concurrent_requests)

        api_key = api_key or os.environ.get("OPENAI_API_KEY")
        if not api_key:
            raise RuntimeError("OPENAI_API_KEY is not set")

        model = model or os.environ.get("OPENAI_MODEL", "gpt-4.1-mini")

        self._client = OpenAI(api_key=api_key)
        self._model = model

    @property
    def model_name(self) -> str:
        return self._model

    def _raw_chat(
        self,
        system_prompt: str,
        user_prompt: str,
        response_format: Optional[Dict[str, Any]],
        temperature: float,
    ) -> str:
        kwargs: Dict[str, Any] = {
            "model": self._model,
            "temperature": temperature,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        }
        if response_format is not None:
            kwargs["response_format"] = response_format

        response = self._client.chat.completions.create(**kwargs)

        # Best-effort extraction of token usage information.
        usage = getattr(response, "usage", None)
        if usage is not None:
            # The OpenAI client exposes usage as attributes; fall back to dict-style
            # access if needed for OpenAI-compatible providers.
            prompt_tokens = getattr(usage, "prompt_tokens", None)
            completion_tokens = getattr(usage, "completion_tokens", None)
            if prompt_tokens is None and isinstance(usage, dict):
                prompt_tokens = usage.get("prompt_tokens")
                completion_tokens = usage.get("completion_tokens")
            self._record_usage(
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
            )

        return response.choices[0].message.content or ""

