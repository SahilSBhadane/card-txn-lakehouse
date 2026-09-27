"""FastAPI service:  uvicorn lakehouse.ai.api:app --port 8000   then open http://localhost:8000/docs"""
from __future__ import annotations

from functools import lru_cache

from fastapi import FastAPI
from pydantic import BaseModel, Field

from lakehouse.ai.agent import LakehouseAgent

app = FastAPI(title="Ask your lakehouse", version="0.1.0")


class Question(BaseModel):
    question: str = Field(..., min_length=3, max_length=500,
                          examples=["Which category had the highest spend on the latest day?"])


@lru_cache(maxsize=1)
def agent() -> LakehouseAgent:
    return LakehouseAgent()


@app.get("/health")
def health() -> dict:
    a = agent()
    return {"status": "ok", "tables": sorted(a.tables), "llm": a.llm.name, "retrieval": a.retriever.backend}


@app.get("/catalog")
def catalog() -> list[dict]:
    return [{"id": d.id, "text": d.text} for d in agent().docs]


@app.post("/ask")
def ask(q: Question) -> dict:
    return agent().ask(q.question).to_dict()


@app.post("/reload")
def reload() -> dict:
    """Pick up a new day's gold data (Airflow calls this after build_gold)."""
    agent.cache_clear()
    return health()
