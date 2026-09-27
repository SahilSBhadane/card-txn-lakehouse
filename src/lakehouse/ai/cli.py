"""python -m lakehouse.ai.cli ask "question"   |   eval-retrieval   |   eval   |   catalog"""
import argparse
import json
import logging

logging.basicConfig(level=logging.WARNING)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("cmd", choices=["ask", "eval-retrieval", "eval", "catalog"])
    p.add_argument("question", nargs="?")
    p.add_argument("--min-recall", type=float, default=None, help="eval-retrieval: exit 1 if recall@k is below this")
    a = p.parse_args()
    if a.cmd == "ask":
        from lakehouse.ai.agent import LakehouseAgent
        out = LakehouseAgent().ask(a.question).to_dict()
    elif a.cmd == "eval-retrieval":
        from lakehouse.ai.evaluate import retrieval_eval
        out = retrieval_eval()
        if a.min_recall is not None and out["recall_at_k"] < a.min_recall:
            print(json.dumps(out, indent=2))
            raise SystemExit(f"retrieval recall {out['recall_at_k']} < {a.min_recall}")
    elif a.cmd == "eval":
        from lakehouse.ai.evaluate import execution_eval
        out = execution_eval()
    else:
        from lakehouse.ai.catalog import build_docs
        from lakehouse.ai.warehouse import connect
        out = [d.text for d in build_docs(connect())]
    print(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
