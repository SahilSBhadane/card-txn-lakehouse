"""Retrieval over catalog docs.

Default: sentence-transformers embeddings (all-MiniLM-L6-v2) + cosine similarity.
Fallback: TF-IDF, used automatically when the model isn't installed/downloadable, so the
project runs offline and in CI. The vector store is a small numpy matrix: the catalog has
~10 docs, so a vector database would be overkill (swap in pgvector/Chroma when it grows).
"""
from __future__ import annotations

import logging
import os

import numpy as np

from lakehouse.ai.catalog import Doc

log = logging.getLogger(__name__)


class Retriever:
    def __init__(self, docs: list[Doc], backend: str | None = None):
        self.docs = docs
        self.backend = backend or os.environ.get("EMBEDDINGS", "auto")
        self._model = None
        if self.backend in ("auto", "sentence-transformers"):
            try:
                from sentence_transformers import SentenceTransformer
                self._model = SentenceTransformer(os.environ.get("EMBED_MODEL", "all-MiniLM-L6-v2"))
                self.backend = "sentence-transformers"
            except Exception as e:  # not installed, or model download blocked
                if self.backend == "sentence-transformers":
                    raise
                log.warning("sentence-transformers unavailable (%s); falling back to TF-IDF", type(e).__name__)
                self.backend = "tfidf"
        if self.backend == "tfidf":
            from sklearn.feature_extraction.text import TfidfVectorizer
            self._tfidf = TfidfVectorizer(ngram_range=(1, 2), sublinear_tf=True, stop_words="english")
            self._matrix = self._tfidf.fit_transform([d.text for d in docs])
        else:
            self._matrix = self._embed([d.text for d in docs])

    def _embed(self, texts: list[str]) -> np.ndarray:
        return np.asarray(self._model.encode(texts, normalize_embeddings=True))

    def search(self, query: str, k: int = 4) -> list[tuple[Doc, float]]:
        if self.backend == "tfidf":
            q = self._tfidf.transform([query])
            scores = (self._matrix @ q.T).toarray().ravel()
        else:
            scores = self._matrix @ self._embed([query])[0]
        order = np.argsort(-scores)[:k]
        return [(self.docs[i], float(scores[i])) for i in order]

    def search_expanded(self, query: str, k: int = 4) -> list[Doc]:
        """Top-k docs, plus the table docs any retrieved metric links to (deduped, rank order kept)."""
        by_table = {d.table: d for d in self.docs if d.table}
        out: list[Doc] = []
        for d, _ in self.search(query, k):
            for doc in [d, *(by_table[t] for t in d.links if t in by_table)]:
                if doc not in out:
                    out.append(doc)
        return out
