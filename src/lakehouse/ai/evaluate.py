"""Offline evaluation of the AI layer against a golden set of questions.

  retrieval recall@k : did retrieval surface every table the question needs? (no LLM required)
  execution accuracy : does the LLM's SQL return the same result as the reference SQL?
                       (order-insensitive, floats rounded) - needs a working LLM backend
"""
from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

from lakehouse.ai.agent import LakehouseAgent
from lakehouse.ai.catalog import build_docs
from lakehouse.ai.retrieval import Retriever
from lakehouse.ai.warehouse import connect

GOLDEN = json.loads((Path(__file__).parent / "golden.json").read_text())


def _norm(rows: list) -> list:
    def cell(v):
        if isinstance(v, (float, Decimal)):
            return round(float(v), 2)
        return v
    return sorted((tuple(cell(v) for v in r) for r in rows), key=str)


def retrieval_eval(k: int = 4) -> dict:
    con = connect()
    retriever = Retriever(build_docs(con))
    per_q, hits = [], 0
    for g in GOLDEN:
        got = {d.table for d in retriever.search_expanded(g["q"], k=k) if d.table}
        ok = set(g["tables"]) <= got
        hits += ok
        per_q.append({"q": g["q"], "expected": g["tables"], "retrieved_tables": sorted(got), "hit": ok})
    return {"backend": retriever.backend, "k": k, "recall_at_k": round(hits / len(GOLDEN), 3), "questions": per_q}


def execution_eval(agent: LakehouseAgent | None = None) -> dict:
    agent = agent or LakehouseAgent()
    per_q, correct = [], 0
    for g in GOLDEN:
        expected = _norm(agent.con.execute(g["sql"]).fetchall())
        a = agent.ask(g["q"])
        ok = a.error is None and _norm(a.rows) == expected
        correct += ok
        per_q.append({"q": g["q"], "correct": ok, "attempts": a.attempts, "sql": a.sql, "error": a.error})
    return {"llm": agent.llm.name, "execution_accuracy": round(correct / len(GOLDEN), 3), "questions": per_q}
