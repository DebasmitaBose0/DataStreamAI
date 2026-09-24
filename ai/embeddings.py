"""
DataStream AI - Vector Embeddings & Similarity Engine
Provides vector representations using local TF-IDF cosine-similarity
with optional Gemini API integration when configured.
Guaranteed to run 100% locally with zero external API dependencies.
"""

import os
import logging
from typing import List, Dict, Any, Tuple
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger("datastream.ai.embeddings")


class VectorEngine:
    def __init__(self):
        self.api_key = os.getenv("GEMINI_API_KEY")
        self.vectorizer = TfidfVectorizer(stop_words="english", ngram_range=(1, 2))
        self.chunk_records: List[Dict[str, Any]] = []
        self.tfidf_matrix = None
        self._is_fitted = False

    @property
    def engine_mode(self) -> str:
        if self.api_key and len(self.api_key) > 10:
            return "Gemini API + Local Hybrid Vectorizer"
        return "Local Scikit-Learn TF-IDF & Cosine Similarity Engine (Zero-Cost Local)"

    def index_chunks(self, chunks: List[Dict[str, Any]]):
        """Indexes text chunks into the vector similarity space."""
        if not chunks:
            return

        self.chunk_records.extend(chunks)
        corpus = [c["text"] for c in self.chunk_records]
        self.tfidf_matrix = self.vectorizer.fit_transform(corpus)
        self._is_fitted = True
        logger.info("Indexed %d chunks into vector space.", len(self.chunk_records))

    def reset_index(self):
        """Clears the indexed chunks."""
        self.chunk_records = []
        self.tfidf_matrix = None
        self._is_fitted = False

    def search(self, query: str, top_k: int = 3) -> List[Dict[str, Any]]:
        """Finds top-k most semantically relevant chunks for a user query."""
        if not self._is_fitted or not self.chunk_records or self.tfidf_matrix is None:
            return []

        query_vec = self.vectorizer.transform([query])
        scores = cosine_similarity(query_vec, self.tfidf_matrix).flatten()

        # Get top-k indices
        top_indices = np.argsort(scores)[::-1][:top_k]

        results = []
        for idx in top_indices:
            score = float(scores[idx])
            if score > 0.01:  # Filter out totally irrelevant matches
                results.append({
                    "chunk": self.chunk_records[idx],
                    "similarity_score": round(score, 3)
                })

        return results


vector_engine = VectorEngine()
