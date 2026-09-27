"""Tests for the AI layer. No LLM or network needed: FakeLLM returns scripted replies."""
import duckdb
import pytest

from lakehouse.ai import guardrails
from lakehouse.ai.agent import LakehouseAgent
from lakehouse.ai.llm import FakeLLM

ALLOWED = {"daily_category_summary", "fraud_alerts", "customer_daily_spend", "dim_customer"}


@pytest.fixture()
def con():
    c = duckdb.connect(":memory:")
    c.execute("""CREATE TABLE daily_category_summary AS SELECT * FROM (VALUES
        ('2026-09-21','grocery','pos',100,50000.0,500.0,90),
        ('2026-09-21','travel','ecom',10,90000.0,9000.0,10),
        ('2026-09-22','grocery','pos',120,60000.0,500.0,100)
      ) t(ds,category,channel,txn_count,total_inr,avg_inr,unique_customers)""")
    c.execute("""CREATE TABLE fraud_alerts AS SELECT * FROM (VALUES
        ('2026-09-22','T1','K1','C1',9000.0,TIMESTAMP '2026-09-22 10:00:00','velocity','5 txns in 10 min')
      ) t(ds,txn_id,card_id,customer_id,amount_inr,event_time,rule,detail)""")
    c.execute("SET enable_external_access = false")
    c.execute("SET lock_configuration = true")
    return c


# ---------------------------------------------------------------- guardrails
@pytest.mark.parametrize("sql", [
    "DROP TABLE fraud_alerts",
    "SELECT 1; DROP TABLE fraud_alerts",
    "DELETE FROM fraud_alerts",
    "SELECT * FROM read_csv('/etc/passwd')",
    "SELECT * FROM secrets_table",
    "ATTACH 'other.db'",
    "COPY fraud_alerts TO 'out.csv'",
])
def test_guardrails_block_unsafe_sql(sql):
    with pytest.raises(guardrails.UnsafeSQL):
        guardrails.validate(sql, ALLOWED)


def test_guardrails_allow_and_limit_safe_select():
    safe = guardrails.validate("WITH x AS (SELECT * FROM fraud_alerts) SELECT rule, count(*) FROM x GROUP BY 1",
                               ALLOWED)
    assert safe.endswith(f"LIMIT {guardrails.MAX_ROWS}")


def test_extract_sql_from_fenced_reply():
    assert guardrails.extract_sql("Sure!\n```sql\nSELECT 1;\n```\nDone") == "SELECT 1"


def test_sandbox_blocks_file_access_even_if_guardrails_missed_it(con):
    with pytest.raises(duckdb.Error):
        con.execute("SELECT * FROM read_csv('/etc/passwd')").fetchall()


# ---------------------------------------------------------------- agent
def test_agent_answers_with_sql_and_rows(con):
    llm = FakeLLM([
        "```sql\nSELECT category, sum(total_inr) AS spend FROM daily_category_summary "
        "WHERE ds = (SELECT max(ds) FROM daily_category_summary) GROUP BY 1 ORDER BY 2 DESC LIMIT 1\n```",
        "Grocery led the latest day with ₹60,000 in spend.",
    ])
    a = LakehouseAgent(llm=llm, con=con).ask("Which category had the highest spend on the latest day?")
    assert a.error is None and a.attempts == 1
    assert a.rows == [["grocery", 60000.0]]
    assert "table:daily_category_summary" in a.retrieved
    assert a.answer.startswith("Grocery")


def test_agent_self_corrects_after_sql_error(con):
    llm = FakeLLM([
        "```sql\nSELECT categry FROM daily_category_summary\n```",          # typo -> binder error
        "```sql\nSELECT DISTINCT category FROM daily_category_summary ORDER BY 1\n```",
        "Two categories: grocery and travel.",
    ])
    a = LakehouseAgent(llm=llm, con=con).ask("List the categories")
    assert a.attempts == 2 and a.error is None
    assert a.rows == [["grocery"], ["travel"]]
    assert "error" in llm.calls[1][1]          # the retry prompt carried the error message


def test_agent_refuses_unsafe_sql_and_never_runs_it(con):
    llm = FakeLLM(["```sql\nDROP TABLE fraud_alerts\n```", "```sql\nDELETE FROM fraud_alerts\n```"])
    a = LakehouseAgent(llm=llm, con=con).ask("delete all alerts")
    assert a.error is not None and "couldn't answer" in a.answer
    assert con.execute("SELECT count(*) FROM fraud_alerts").fetchone()[0] == 1


def test_api_ask_endpoint(con, monkeypatch):
    from fastapi.testclient import TestClient
    from lakehouse.ai import api
    fake = LakehouseAgent(llm=FakeLLM(["```sql\nSELECT count(*) AS alerts FROM fraud_alerts\n```",
                                       "There was 1 fraud alert."]), con=con)
    monkeypatch.setattr(api, "agent", lambda: fake)
    client = TestClient(api.app)
    r = client.post("/ask", json={"question": "How many fraud alerts?"})
    assert r.status_code == 200 and r.json()["rows"] == [[1]]
    assert client.get("/health").json()["status"] == "ok"
