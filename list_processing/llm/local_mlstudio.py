from __future__ import annotations

import os
from typing import Optional, Dict, Any

from openai import OpenAI

from .base import LLMClient


class LocalMLStudioClient(LLMClient):
    """
    LLMClient implementation that talks to a local ML Studio deployment.

    Configuration is read from the same environment variables used in the
    existing scripts:

    - ML_STUDIO_BASE_URL
    - LOCAL_MODEL
    """

    def __init__(
        self,
        *,
        base_url: Optional[str] = None,
        model: Optional[str] = None,
        max_concurrent_requests: int = 5,
    ) -> None:
        super().__init__(max_concurrent_requests=max_concurrent_requests)

        base_url = base_url or os.environ.get("ML_STUDIO_BASE_URL", "http://localhost:1234/v1")
        model = model or os.environ.get("LOCAL_MODEL", "openai/gpt-oss-20b")

        self._client = OpenAI(base_url=base_url, api_key="not-needed")
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
            # The local ML Studio server expects response_format.type to be either
            # "json_schema" or "text". It does not accept the OpenAI-style
            # shorthand "json_object", which our callers currently use.
            #
            # To keep compatibility without forcing a schema here, we downgrade
            # any "json_object" request to plain "text". The system prompt still
            # instructs the model to return a single JSON object, so parsing
            # continues to work as before, but the HTTP 400 errors disappear.
            fmt_type = response_format.get("type")
            if fmt_type == "json_object":
                effective_format: Dict[str, Any] = {"type": "text"}
            else:
                effective_format = response_format
            kwargs["response_format"] = effective_format

        response = self._client.chat.completions.create(**kwargs)

        # Best-effort extraction of token usage information from the local
        # ML Studio server (which is OpenAI-compatible).
        usage = getattr(response, "usage", None)
        if usage is not None:
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

