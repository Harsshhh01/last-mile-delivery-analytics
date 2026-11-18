"""Text-to-SQL analytics agent over the delivery star schema.

Flow for each question:
  1. Prompt the LLM with the live warehouse schema and a business glossary.
  2. Validate the generated SQL with guardrails (read-only, approved tables, row cap).
  3. Execute on a read-only connection; on an error, feed it back and retry once.
  4. Sanity-check the result (empty result, impossible percentages).
  5. Ask the LLM to answer *only* from the result rows, then verify every number it
     cites appears in the data, flagging anything ungrounded instead of trusting it.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

import duckdb

from ..warehouse import STAR_TABLES
from .llm import LLM
from .sql_guard import UnsafeSQLError, validate_sql

GLOSSARY = """\
Business glossary:
- DSP = Delivery Service Partner (a delivery company). Station = a delivery station (hub).
- fact_delivery_attempt has one row per delivery attempt; a package can have up to 3 attempts.
- status is 'DELIVERED' or 'FAILED'. failure_reason is set only for FAILED attempts.
- is_on_time = delivered at or before promised_by. On-time rate = on-time deliveries / deliveries.
- is_first_attempt_delivery = delivered on attempt 1. First-attempt success rate =
  first-attempt deliveries / first attempts.
- Join facts to dimensions on dsp_key, station_key, driver_key and date_key.
- Drivers are pseudonymised (driver_hash); never try to identify a person.
"""

SQL_SYSTEM = """You are a senior data analyst for a last-mile delivery program.
Write ONE read-only SQL query (ANSI SQL that runs on DuckDB and Amazon Redshift) that answers
the user's question using only the tables below. Prefer explicit JOINs, round percentages to
2 decimals, and return at most 50 rows. Reply with the SQL only, inside a ```sql code block.

{schema}

{glossary}"""

ANSWER_SYSTEM = """You explain query results to operations managers.
Answer the question in 2-4 sentences using ONLY numbers that appear in the result table.
If the result cannot answer the question, say so. Do not speculate."""

MAX_RESULT_ROWS_IN_PROMPT = 50


@dataclass
class AgentAnswer:
    question: str
    sql: str
    columns: list[str]
    rows: list[tuple]
    answer: str
    attempts: int
    warnings: list[str] = field(default_factory=list)


def extract_sql(text: str) -> str:
    match = re.search(r"```(?:sql)?\s*(.*?)```", text, flags=re.DOTALL | re.IGNORECASE)
    return (match.group(1) if match else text).strip().rstrip(";").strip()


def _number_forms(value) -> set[str]:
    forms = set()
    if isinstance(value, bool) or value is None:
        return forms
    if isinstance(value, (int, float)):
        v = float(value)
        for digits in (0, 1, 2):
            forms.add(f"{round(v, digits):.{digits}f}")
        if v.is_integer():
            forms.add(str(int(v)))
    else:
        forms.update(re.findall(r"\d+(?:\.\d+)?", str(value)))
    return forms


def ungrounded_numbers(answer: str, rows: list[tuple], question: str = "") -> list[str]:
    """Numbers in the answer that appear neither in the result rows nor in the question."""
    known: set[str] = set(re.findall(r"\d+(?:\.\d+)?", question))
    for row in rows:
        for value in row:
            known |= _number_forms(value)
    cited = re.findall(r"\d[\d,]*(?:\.\d+)?", answer)
    missing = []
    for raw in cited:
        n = raw.replace(",", "")
        candidates = {n}
        if "." in n:
            candidates |= {f"{float(n):.{d}f}" for d in (0, 1, 2)}
        if float(n) <= 10 and float(n).is_integer():
            continue  # small counts like "top 3" or "7-day" are not data claims
        if not candidates & known:
            missing.append(raw)
    return missing


class AnalyticsAgent:
    def __init__(self, llm: LLM, db_path: str, max_rows: int = 200, max_attempts: int = 2):
        self.llm = llm
        self.db_path = db_path
        self.max_rows = max_rows
        self.max_attempts = max_attempts
        self.allowed_tables = set(STAR_TABLES)

    def schema_description(self) -> str:
        with duckdb.connect(self.db_path, read_only=True) as con:
            rows = con.execute(
                "SELECT table_name, column_name, data_type FROM information_schema.columns "
                "WHERE table_name IN (" + ",".join(f"'{t}'" for t in sorted(self.allowed_tables)) + ") "
                "ORDER BY table_name, ordinal_position"
            ).fetchall()
        tables: dict[str, list[str]] = {}
        for table, column, dtype in rows:
            tables.setdefault(table, []).append(f"{column} {dtype}")
        return "Tables:\n" + "\n".join(f"- {t}({', '.join(cols)})" for t, cols in tables.items())

    def _execute(self, sql: str) -> tuple[list[str], list[tuple]]:
        with duckdb.connect(self.db_path, read_only=True) as con:
            cur = con.execute(sql)
            return [d[0] for d in cur.description], cur.fetchall()

    def ask(self, question: str) -> AgentAnswer:
        system = SQL_SYSTEM.format(schema=self.schema_description(), glossary=GLOSSARY)
        prompt = f"Question: {question}"
        last_error = ""
        for attempt in range(1, self.max_attempts + 1):  # noqa: B007 - reported after the loop
            raw_sql = extract_sql(self.llm.complete(system, prompt))
            try:
                sql = validate_sql(raw_sql, self.allowed_tables, self.max_rows)
                columns, rows = self._execute(sql)
                break
            except (UnsafeSQLError, duckdb.Error) as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                prompt = (f"Question: {question}\n\nYour previous SQL was rejected.\nSQL:\n{raw_sql}\n"
                          f"Error: {last_error}\nWrite a corrected query.")
        else:
            return AgentAnswer(question, raw_sql, [], [], f"Could not answer safely: {last_error}",
                               self.max_attempts, [f"query rejected or failed: {last_error}"])

        warnings = self._check_result(columns, rows)
        table = "\n".join([" | ".join(columns)] + [
            " | ".join("" if v is None else str(v) for v in row) for row in rows[:MAX_RESULT_ROWS_IN_PROMPT]
        ])
        answer = self.llm.complete(ANSWER_SYSTEM, f"Question: {question}\n\nSQL:\n{sql}\n\nResult:\n{table}")
        ungrounded = ungrounded_numbers(answer, rows[:MAX_RESULT_ROWS_IN_PROMPT], question)
        if ungrounded:
            warnings.append(f"answer cites numbers not found in the query result: {ungrounded}")
        return AgentAnswer(question, sql, columns, rows, answer.strip(), attempt, warnings)

    @staticmethod
    def _check_result(columns: list[str], rows: list[tuple]) -> list[str]:
        warnings = []
        if not rows:
            warnings.append("query returned no rows")
        for i, col in enumerate(columns):
            name = col.lower()
            if "pct" in name or "percent" in name:
                bad = [r[i] for r in rows if isinstance(r[i], (int, float)) and not 0 <= r[i] <= 100]
                if bad:
                    warnings.append(f"column {col} has values outside 0-100: {bad[:3]}")
        return warnings
