import pytest

from delivery_pipeline.agent.sql_guard import UnsafeSQLError, validate_sql

TABLES = {"fact_delivery_attempt", "dim_dsp", "dim_date", "dim_station", "dim_driver"}


def test_select_gets_a_row_cap():
    sql = validate_sql("SELECT dsp_code FROM dim_dsp", TABLES, max_rows=50)
    assert sql.upper().endswith("LIMIT 50")


def test_existing_small_limit_is_kept_and_large_limit_is_clamped():
    assert validate_sql("SELECT * FROM dim_dsp LIMIT 5", TABLES).upper().endswith("LIMIT 5")
    clamped = validate_sql("SELECT * FROM dim_dsp LIMIT 100000", TABLES, max_rows=200)
    assert clamped.upper().endswith("LIMIT 200")


def test_ctes_and_joins_are_allowed():
    sql = """WITH t AS (SELECT dsp_key, COUNT(*) AS n FROM fact_delivery_attempt GROUP BY 1)
             SELECT d.dsp_code, t.n FROM t JOIN dim_dsp d ON d.dsp_key = t.dsp_key"""
    assert "LIMIT" in validate_sql(sql, TABLES).upper()


@pytest.mark.parametrize(
    "sql, reason",
    [
        ("DROP TABLE dim_dsp", "only SELECT"),
        ("DELETE FROM fact_delivery_attempt", "only SELECT"),
        ("INSERT INTO dim_dsp VALUES (1)", "only SELECT"),
        ("UPDATE dim_dsp SET dsp_name = 'x'", "only SELECT"),
        ("SELECT 1; DROP TABLE dim_dsp", "exactly one statement"),
        ("SELECT * FROM information_schema.tables", "outside the warehouse"),
        ("SELECT * FROM users", "outside the warehouse"),
        ("SELECT * FROM read_csv('/etc/passwd')", "forbidden function"),
        ("SELECT * FROM read_parquet('s3://other/*.parquet')", "forbidden function"),
        ("SELECT * FROM 'secrets.csv'", "outside the warehouse"),
        ("SELEC dsp FROM", "does not parse"),
    ],
)
def test_unsafe_sql_is_rejected(sql, reason):
    with pytest.raises(UnsafeSQLError, match=reason):
        validate_sql(sql, TABLES)
