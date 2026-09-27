"""The question -> answer loop.

  1. retrieve the catalog docs relevant to the question (RAG)
  2. LLM writes one DuckDB SELECT using only those schemas
  3. guardrails validate it; the sandboxed warehouse runs it
  4. on an error, the LLM gets ONE retry with the error message (self-correction)
  5. LLM turns the result rows into a short answer; SQL + rows are returned for transparency
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import asdict, dataclass, field

import duckdb

from lakehouse.ai import guardrails
from lakehouse.ai.catalog import build_docs
from lakehouse.ai.llm import LLM, get_llm
from lakehouse.ai.retrieval import Retriever
from lakehouse.ai.warehouse import available_tables, connect

log = logging.getLogger(__name__)

SQL_SYSTEM = """You are a careful analytics engineer writing DuckDB SQL for a card-transactions lakehouse.
Rules:
- Use ONLY the tables and columns described in the context. Never invent columns.
- Write exactly one read-only SELECT (CTEs allowed). No DDL/DML, no file functions.
- ds is a string 'YYYY-MM-DD'. "Latest day" means the max(ds) of the table you query.
- Money is in INR. Round money to 2 decimals. Give readable column aliases.
- Reply with the SQL only, inside a ```sql code block."""

ANSWER_SYSTEM = """You explain query results to a business user in 1-3 short sentences.
Use only the numbers in the result. Format INR with commas. If the result is empty, say so plainly."""


@dataclass
class Answer:
    question: str
    answer: str
    sql: str | None
    columns: list[str] = field(default_factory=list)
    rows: list[list] = field(default_factory=list)
    retrieved: list[str] = field(default_factory=list)
    attempts: int = 0
    error: str | None = None
    llm: str = ""
    latency_s: float = 0.0

    def to_dict(self) -> dict:
        return asdict(self)


class LakehouseAgent:
    def __init__(self, llm: LLM | None = None, con: duckdb.DuckDBPyConnection | None = None, k: int = 4):
        self.llm = llm or get_llm()
        self.con = con or connect()
        self.tables = set(available_tables(self.con))
        self.docs = build_docs(self.con)
        self.retriever = Retriever(self.docs)
        self.k = k

    def generate_sql(self, question: str, context: str, feedback: str | None = None) -> str:
        user = f"Context:\n{context}\n\nQuestion: {question}"
        if feedback:
            user += f"\n\nYour previous query failed:\n{feedback}\nFix it and return corrected SQL."
        return guardrails.extract_sql(self.llm.complete(SQL_SYSTEM, user))

    def ask(self, question: str, max_attempts: int = 2) -> Answer:
        t0 = time.time()
        hits = self.retriever.search_expanded(question, k=self.k)
        context = "\n\n".join(d.text for d in hits)
        ans = Answer(question=question, answer="", sql=None, retrieved=[d.id for d in hits], llm=self.llm.name)
        feedback = None
        for attempt in range(1, max_attempts + 1):
            ans.attempts = attempt
            sql = self.generate_sql(question, context, feedback)
            ans.sql = sql
            try:
                safe = guardrails.validate(sql, self.tables)
                cur = self.con.execute(safe)
                ans.columns = [c[0] for c in cur.description]
                ans.rows = [list(r) for r in cur.fetchall()]
                ans.error = None
                break
            except (guardrails.UnsafeSQL, duckdb.Error) as e:
                ans.error = f"{type(e).__name__}: {e}"
                feedback = f"{sql}\n-- error: {ans.error}"
                log.warning("attempt %s failed: %s", attempt, ans.error)
        if ans.error:
            ans.answer = "I couldn't answer that safely from the lakehouse data."
        else:
            preview = json.dumps({"columns": ans.columns, "rows": ans.rows[:20]}, default=str)
            ans.answer = self.llm.complete(ANSWER_SYSTEM, f"Question: {question}\nSQL: {ans.sql}\nResult: {preview}").strip()
        ans.latency_s = round(time.time() - t0, 2)
        return ans
