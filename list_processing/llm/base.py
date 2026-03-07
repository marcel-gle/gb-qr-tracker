from __future__ import annotations

import abc
import threading
from typing import Optional, Dict, Any


class LLMClient(abc.ABC):
    """
    Minimal chat-style interface used by the list_processing pipeline.

    Concrete implementations wrap either a local ML Studio deployment or an
    online provider such as OpenAI. Concurrency limiting is handled inside
    the client via an internal semaphore so that callers can safely use
    ThreadPoolExecutor without worrying about rate limits.
    """

    def __init__(self, max_concurrent_requests: int = 5) -> None:
        self._semaphore = threading.Semaphore(max_concurrent_requests)
        # Aggregate usage metrics across all calls made through this client.
        self._usage_lock = threading.Lock()
        self._total_prompt_tokens: int = 0
        self._total_completion_tokens: int = 0
        self._total_calls: int = 0

    @abc.abstractmethod
    def _raw_chat(
        self,
        system_prompt: str,
        user_prompt: str,
        response_format: Optional[Dict[str, Any]],
        temperature: float,
    ) -> str:
        """
        Perform the actual chat completion call and return the content string.
        Implementations must raise exceptions on hard failures.
        """

    # ------------------------------------------------------------------
    # Usage accounting helpers
    # ------------------------------------------------------------------

    def _record_usage(
        self,
        *,
        prompt_tokens: Optional[int],
        completion_tokens: Optional[int],
    ) -> None:
        """
        Record token usage for a single call in a threadsafe way.

        Subclasses should call this after receiving a response object that
        includes token usage information (if available).
        """
        with self._usage_lock:
            self._total_calls += 1
            if isinstance(prompt_tokens, int) and prompt_tokens >= 0:
                self._total_prompt_tokens += prompt_tokens
            if isinstance(completion_tokens, int) and completion_tokens >= 0:
                self._total_completion_tokens += completion_tokens

    @property
    def total_prompt_tokens(self) -> int:
        return self._total_prompt_tokens

    @property
    def total_completion_tokens(self) -> int:
        return self._total_completion_tokens

    @property
    def total_tokens(self) -> int:
        return self._total_prompt_tokens + self._total_completion_tokens

    @property
    def total_calls(self) -> int:
        return self._total_calls

    def chat(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        response_format: Optional[Dict[str, Any]] = None,
        temperature: float = 0.3,
    ) -> str:
        """
        High-level entrypoint for all LLM calls.

        This wraps the concrete `_raw_chat` implementation with a semaphore to
        enforce a maximum number of concurrent requests per client instance.
        """
        with self._semaphore:
            return self._raw_chat(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                response_format=response_format,
                temperature=temperature,
            )

