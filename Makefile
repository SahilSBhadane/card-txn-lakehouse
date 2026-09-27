.PHONY: up down test run-day backfill model api ask eval eval-retrieval clean

up:              ## build + start Airflow (8080), Ollama (11434), API (8000)
	docker compose up --build -d

model:           ## download the local LLM once (~4.7 GB; use qwen2.5-coder:3b on low-RAM laptops)
	docker compose exec ollama ollama pull qwen2.5-coder:7b

down:
	docker compose down

test:            ## all unit tests, no LLM or network needed
	python -m pytest -q tests

run-day:         ## every pipeline step for one day, no Airflow: make run-day DS=2026-09-20
	PYTHONPATH=src python -m lakehouse.cli all --ds $(DS)

backfill:        ## make backfill FROM=2026-09-01 TO=2026-09-07
	docker compose exec airflow airflow backfill create --dag-id card_txn_lakehouse \
		--from-date $(FROM) --to-date $(TO) --max-active-runs 1

api:             ## run the API locally (needs Ollama running): http://localhost:8000/docs
	PYTHONPATH=src uvicorn lakehouse.ai.api:app --port 8000

ask:             ## make ask Q="Which category had the highest spend on the latest day?"
	PYTHONPATH=src python -m lakehouse.ai.cli ask "$(Q)"

eval-retrieval:  ## retrieval recall@4 on the golden questions (no LLM needed)
	PYTHONPATH=src python -m lakehouse.ai.cli eval-retrieval

eval:            ## execution accuracy of LLM-written SQL on the golden questions (needs the LLM)
	PYTHONPATH=src python -m lakehouse.ai.cli eval

clean:
	rm -rf data/*
