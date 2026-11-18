"""Guardrails for LLM-generated SQL: one read-only SELECT over approved tables, row-capped.

The query is parsed into an AST (sqlglot) and rejected unless it is safe, so a wrong
or malicious generation can never modify data or read files on the host. Execution also
uses a read-only database connection as a second layer of defence.
"""

from __future__ import annotations

import sqlglot
from sqlglot import exp

QUERY_KEYS = {"select", "union", "intersect", "except"}
FORBIDDEN_NODE_NAMES = (
    "Insert", "Update", "Delete", "Drop", "Create", "Alter", "AlterTable", "Merge",
    "Command", "Copy", "TruncateTable", "Grant", "Set", "Pragma", "Use", "LoadData",
)
FORBIDDEN_NODES = tuple(getattr(exp, n) for n in FORBIDDEN_NODE_NAMES if hasattr(exp, n))
FORBIDDEN_FUNCTION_PREFIXES = ("read_", "glob", "copy", "attach", "install", "load", "pg_", "system")


class UnsafeSQLError(ValueError):
    """Generated SQL failed a guardrail and was not executed."""


def _function_name(fn: exp.Func) -> str:
    if isinstance(fn, exp.Anonymous):
        return str(fn.name).lower()
    return fn.sql_name().lower()


def validate_sql(sql: str, allowed_tables: set[str], max_rows: int = 200, dialect: str = "duckdb") -> str:
    """Return a safe, LIMIT-capped version of `sql` or raise UnsafeSQLError."""
    try:
        statements = [s for s in sqlglot.parse(sql, read=dialect) if s is not None]
    except sqlglot.errors.ParseError as exc:
        raise UnsafeSQLError(f"SQL does not parse: {str(exc).splitlines()[0]}") from exc
    if len(statements) != 1:
        raise UnsafeSQLError(f"expected exactly one statement, got {len(statements)}")
    stmt = statements[0]
    if stmt.key not in QUERY_KEYS:
        raise UnsafeSQLError(f"only SELECT queries are allowed, got {stmt.key.upper()}")

    for node in stmt.walk():
        if isinstance(node, FORBIDDEN_NODES):
            raise UnsafeSQLError(f"forbidden operation: {type(node).__name__}")
        if isinstance(node, exp.Func) and _function_name(node).startswith(FORBIDDEN_FUNCTION_PREFIXES):
            raise UnsafeSQLError(f"forbidden function: {_function_name(node)}")

    cte_names = {cte.alias_or_name.lower() for cte in stmt.find_all(exp.CTE)}
    tables = {t.name.lower() for t in stmt.find_all(exp.Table)} - cte_names
    unknown = tables - {t.lower() for t in allowed_tables}
    if unknown:
        raise UnsafeSQLError(f"query references tables outside the warehouse: {sorted(unknown)}")

    limit = stmt.args.get("limit")
    current = None
    if limit is not None:
        try:
            current = int(limit.expression.name)
        except (AttributeError, TypeError, ValueError):
            current = None
    if current is None or current > max_rows:
        stmt = stmt.limit(max_rows, copy=True)
    return stmt.sql(dialect=dialect)
