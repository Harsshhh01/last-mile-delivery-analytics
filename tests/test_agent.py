import duckdb

from delivery_pipeline.agent.analytics_agent import AnalyticsAgent, extract_sql, ungrounded_numbers
from delivery_pipeline.agent.llm import ScriptedLLM

ON_TIME_SQL = """```sql
SELECT d.dsp_code,
       ROUND(100.0 * SUM(CASE WHEN f.is_on_time THEN 1 ELSE 0 END)
             / SUM(CASE WHEN f.status = 'DELIVERED' THEN 1 ELSE 0 END), 2) AS on_time_pct
FROM fact_delivery_attempt f JOIN dim_dsp d ON f.dsp_key = d.dsp_key
GROUP BY d.dsp_code ORDER BY on_time_pct ASC LIMIT 1
```"""


def _lowest_dsp(db):
    with duckdb.connect(db, read_only=True) as con:
        return con.execute(extract_sql(ON_TIME_SQL)).fetchone()


def test_answers_from_query_results(warehouse):
    dsp, pct = _lowest_dsp(warehouse["db"])
    llm = ScriptedLLM([ON_TIME_SQL, f"{dsp} has the lowest on-time rate at {pct}%."])
    result = AnalyticsAgent(llm, warehouse["db"]).ask("Which DSP has the lowest on-time rate?")
    assert result.rows == [(dsp, pct)]
    assert result.attempts == 1
    assert result.warnings == []
    assert "fact_delivery_attempt(" in llm.prompts[0][0]  # live schema is in the prompt


def test_unsafe_sql_is_never_executed_and_retried(warehouse):
    dsp, pct = _lowest_dsp(warehouse["db"])
    llm = ScriptedLLM(["DROP TABLE dim_dsp", ON_TIME_SQL, f"{dsp}: {pct}%"])
    result = AnalyticsAgent(llm, warehouse["db"]).ask("Lowest on-time DSP?")
    assert result.attempts == 2
    assert "rejected" in llm.prompts[1][1]
    with duckdb.connect(warehouse["db"], read_only=True) as con:
        assert con.execute("SELECT COUNT(*) FROM dim_dsp").fetchone()[0] == 10


def test_sql_errors_are_fed_back(warehouse):
    llm = ScriptedLLM(
        ["SELECT no_such_column FROM dim_dsp", "SELECT COUNT(*) AS n FROM dim_dsp", "There are 10."]
    )
    result = AnalyticsAgent(llm, warehouse["db"]).ask("How many DSPs?")
    assert result.attempts == 2
    assert "no_such_column" in llm.prompts[1][1]
    assert result.rows == [(10,)]


def test_gives_up_safely(warehouse):
    llm = ScriptedLLM(["DELETE FROM dim_dsp", "DROP TABLE dim_date"])
    result = AnalyticsAgent(llm, warehouse["db"], max_attempts=2).ask("Break things")
    assert result.rows == [] and result.answer.startswith("Could not answer safely")


def test_hallucinated_numbers_are_flagged(warehouse):
    dsp, _ = _lowest_dsp(warehouse["db"])
    llm = ScriptedLLM([ON_TIME_SQL, f"{dsp} is lowest at 12.34%, down 87.5% from last month."])
    result = AnalyticsAgent(llm, warehouse["db"]).ask("Which DSP has the lowest on-time rate?")
    assert any("12.34" in w and "87.5" in w for w in result.warnings)


def test_grounding_check_tolerates_rounding_and_small_counts():
    rows = [("DSP-ARROW", 91.237, 1200)]
    assert ungrounded_numbers("Arrow leads the top 3 with 91.24% on 1,200 deliveries.", rows) == []
    assert ungrounded_numbers("Arrow is at 95.1%.", rows) == ["95.1"]


def test_extract_sql():
    assert extract_sql("Here you go:\n```sql\nSELECT 1;\n```") == "SELECT 1"
    assert extract_sql("SELECT 2") == "SELECT 2"
