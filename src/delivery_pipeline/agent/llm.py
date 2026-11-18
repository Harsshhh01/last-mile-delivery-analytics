"""LLM providers. Amazon Bedrock is the default on AWS; the Anthropic API works anywhere.

Configure with environment variables:
    LLM_PROVIDER=bedrock    BEDROCK_MODEL_ID=<model id enabled in your Bedrock console>
    LLM_PROVIDER=anthropic  ANTHROPIC_API_KEY=...  ANTHROPIC_MODEL=claude-haiku-5-5
"""

from __future__ import annotations

import os
from collections.abc import Iterable
from typing import Protocol


class LLM(Protocol):
    def complete(self, system: str, prompt: str) -> str: ...


class BedrockLLM:
    """Amazon Bedrock via the Converse API (boto3 bedrock-runtime)."""

    def __init__(self, model_id: str | None = None, region: str | None = None, max_tokens: int = 1024):
        import boto3

        self.model_id = model_id or os.environ.get("BEDROCK_MODEL_ID")
        if not self.model_id:
            raise ValueError("set BEDROCK_MODEL_ID to a model enabled in your Amazon Bedrock account")
        self.client = boto3.client("bedrock-runtime", region_name=region or os.environ.get("AWS_REGION"))
        self.max_tokens = max_tokens

    def complete(self, system: str, prompt: str) -> str:
        response = self.client.converse(
            modelId=self.model_id,
            system=[{"text": system}],
            messages=[{"role": "user", "content": [{"text": prompt}]}],
            inferenceConfig={"maxTokens": self.max_tokens, "temperature": 0.0},
        )
        return "".join(block.get("text", "") for block in response["output"]["message"]["content"])


class AnthropicLLM:
    """Anthropic Messages API (pip install anthropic)."""

    def __init__(self, model: str | None = None, max_tokens: int = 1024):
        import anthropic

        self.client = anthropic.Anthropic()
        self.model = model or os.environ.get("ANTHROPIC_MODEL", "claude-haiku-5-5")
        self.max_tokens = max_tokens

    def complete(self, system: str, prompt: str) -> str:
        response = self.client.messages.create(
            model=self.model,
            max_tokens=self.max_tokens,
            temperature=0.0,
            system=system,
            messages=[{"role": "user", "content": prompt}],
        )
        return "".join(getattr(block, "text", "") for block in response.content)


class ScriptedLLM:
    """Deterministic stand-in for tests and offline demos: returns responses in order."""

    def __init__(self, responses: Iterable[str]):
        self.responses = list(responses)
        self.prompts: list[tuple[str, str]] = []

    def complete(self, system: str, prompt: str) -> str:
        self.prompts.append((system, prompt))
        if not self.responses:
            raise RuntimeError("ScriptedLLM ran out of responses")
        return self.responses.pop(0)


def get_llm(provider: str | None = None) -> LLM:
    provider = (provider or os.environ.get("LLM_PROVIDER", "bedrock")).lower()
    if provider == "bedrock":
        return BedrockLLM()
    if provider == "anthropic":
        return AnthropicLLM()
    raise ValueError(f"unknown LLM_PROVIDER {provider!r}; use 'bedrock' or 'anthropic'")
