"""LLM backends behind one tiny interface: complete(system, user) -> text.

  LLM_PROVIDER=ollama  (default) free, runs locally: `ollama pull qwen2.5-coder:7b`
  LLM_PROVIDER=openai  any OpenAI-compatible API (OpenAI, Groq, Together, vLLM...):
                       OPENAI_API_KEY, OPENAI_BASE_URL (optional), OPENAI_MODEL
Tests use FakeLLM, so no key or model is needed to run them.
"""
from __future__ import annotations

import os
from typing import Protocol

import httpx


class LLM(Protocol):
    name: str

    def complete(self, system: str, user: str) -> str: ...


class OllamaLLM:
    def __init__(self, model: str | None = None, host: str | None = None):
        self.model = model or os.environ.get("OLLAMA_MODEL", "qwen2.5-coder:7b")
        self.host = host or os.environ.get("OLLAMA_HOST", "http://localhost:11434")
        self.name = f"ollama:{self.model}"

    def complete(self, system: str, user: str) -> str:
        r = httpx.post(f"{self.host}/api/chat", timeout=120, json={
            "model": self.model, "stream": False, "options": {"temperature": 0},
            "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}]})
        r.raise_for_status()
        return r.json()["message"]["content"]


class OpenAICompatLLM:
    def __init__(self, model: str | None = None):
        self.key = os.environ["OPENAI_API_KEY"]
        self.base = os.environ.get("OPENAI_BASE_URL", "https://api.openai.com/v1").rstrip("/")
        self.model = model or os.environ.get("OPENAI_MODEL", "gpt-4o-mini")
        self.name = f"openai-compatible:{self.model}"

    def complete(self, system: str, user: str) -> str:
        r = httpx.post(f"{self.base}/chat/completions", timeout=120,
                       headers={"Authorization": f"Bearer {self.key}"},
                       json={"model": self.model, "temperature": 0,
                             "messages": [{"role": "system", "content": system},
                                          {"role": "user", "content": user}]})
        r.raise_for_status()
        return r.json()["choices"][0]["message"]["content"]


class FakeLLM:
    """Deterministic stand-in for tests: returns queued responses in order."""
    name = "fake"

    def __init__(self, responses: list[str]):
        self.responses = list(responses)
        self.calls: list[tuple[str, str]] = []

    def complete(self, system: str, user: str) -> str:
        self.calls.append((system, user))
        return self.responses.pop(0)


def get_llm() -> LLM:
    provider = os.environ.get("LLM_PROVIDER", "ollama")
    return OpenAICompatLLM() if provider == "openai" else OllamaLLM()
