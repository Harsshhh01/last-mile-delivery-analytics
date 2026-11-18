"""Ask the warehouse a business question in plain English.

    python -m delivery_pipeline.agent.cli "Which DSP had the lowest on-time rate last week?"

Needs LLM_PROVIDER=bedrock (with BEDROCK_MODEL_ID and AWS credentials) or
LLM_PROVIDER=anthropic (with ANTHROPIC_API_KEY).
"""

from __future__ import annotations

import argparse

from .analytics_agent import AnalyticsAgent
from .llm import get_llm


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("question")
    p.add_argument("--db", default="warehouse/delivery.duckdb")
    p.add_argument("--provider", default=None, help="bedrock or anthropic (default: $LLM_PROVIDER)")
    a = p.parse_args(argv)

    result = AnalyticsAgent(get_llm(a.provider), a.db).ask(a.question)
    print(f"\nSQL ({result.attempts} attempt(s)):\n{result.sql}\n")
    print(" | ".join(result.columns))
    for row in result.rows[:20]:
        print(" | ".join(str(v) for v in row))
    print(f"\nAnswer: {result.answer}")
    for w in result.warnings:
        print(f"WARNING: {w}")


if __name__ == "__main__":
    main()
