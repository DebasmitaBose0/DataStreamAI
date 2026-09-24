"""
DataStream AI - RAG (Retrieval Augmented Generation) Engine
Manages document indexing, chunk retrieval, and question answering
with dual-mode generation (Gemini LLM API or zero-dependency local synthesizer).
"""

import os
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
import requests
from dotenv import load_dotenv

from ai.chunking import chunker
from ai.embeddings import vector_engine

load_dotenv()
logger = logging.getLogger("datastream.ai.rag")
BASE_DIR = Path(__file__).resolve().parent.parent
DOCS_DIR = BASE_DIR / "docs"


class RAGEngine:
    def __init__(self):
        self.vector_engine = vector_engine
        self.chunker = chunker
        self.indexed_documents: List[str] = []
        self._init_default_documents()

    def _init_default_documents(self):
        """Indexes default documentation files on startup."""
        for filename in ["platform_overview.txt", "ott_content_catalog.txt"]:
            file_p = DOCS_DIR / filename
            if file_p.exists():
                self.index_file(str(file_p), doc_name=filename)

    def index_file(self, file_path: str, doc_name: Optional[str] = None) -> int:
        """Reads a text document, chunks it, and indexes into vector memory."""
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
            name = doc_name or Path(file_path).name
            return self.index_raw_text(content, doc_name=name)
        except Exception as e:
            logger.error("Failed to index file %s: %s", file_path, e)
            return 0

    def index_raw_text(self, text: str, doc_name: str = "uploaded_doc.txt") -> int:
        """Chunks and indexes raw text into vector space."""
        chunks = self.chunker.chunk_text(text, doc_id=doc_name, metadata={"source": doc_name})
        if chunks:
            self.vector_engine.index_chunks(chunks)
            if doc_name not in self.indexed_documents:
                self.indexed_documents.append(doc_name)
        return len(chunks)

    def answer_question(self, query: str, top_k: int = 3) -> Dict[str, Any]:
        """
        Retrieves top relevant chunks and generates a grounded response.
        Uses Gemini API if key is present, otherwise falls back to local synthesis.
        """
        retrieved = self.vector_engine.search(query, top_k=top_k)

        if not retrieved:
            return {
                "query": query,
                "answer": "No relevant documentation or knowledge base records found matching your query. Try rephrasing or uploading relevant documents.",
                "retrieved_chunks": [],
                "engine_used": "Local Semantic Search (No matches)"
            }

        context_texts = [r["chunk"]["text"] for r in retrieved]
        context_block = "\n\n---\n\n".join(context_texts)

        # Check for Gemini API Key
        api_key = os.getenv("GEMINI_API_KEY")
        if api_key and len(api_key) > 10:
            try:
                answer = self._call_gemini_api(api_key, query, context_block)
                return {
                    "query": query,
                    "answer": answer,
                    "retrieved_chunks": retrieved,
                    "engine_used": "Gemini 1.5 Flash (Cloud LLM)"
                }
            except Exception as e:
                logger.warning("Gemini API call failed (%s). Falling back to local synthesizer.", e)

        # Local Extractive Synthesis Fallback (Zero external dependency)
        synthesized_answer = self._local_synthesize(query, retrieved)
        return {
            "query": query,
            "answer": synthesized_answer,
            "retrieved_chunks": retrieved,
            "engine_used": "DataStream Local RAG Synthesizer (Zero-Cost Local)"
        }

    def _local_synthesize(self, query: str, retrieved: List[Dict[str, Any]]) -> str:
        """Generates structured answer highlighting relevant facts from chunks."""
        top_match = retrieved[0]
        doc_source = top_match["chunk"]["metadata"].get("source", "Documentation")
        score = top_match["similarity_score"]

        response_lines = [
            f"**Information Retrieved from:** `{doc_source}` (Relevance Score: `{score:.2f}`)",
            "",
            "### Summary & Key Findings:",
            f"> {top_match['chunk']['text'][:400]}...",
            "",
            "### Additional Context Details:"
        ]

        for i, item in enumerate(retrieved[1:], start=2):
            src = item["chunk"]["metadata"].get("source", "Document")
            response_lines.append(f"- **[Source {i} - {src}]**: {item['chunk']['text'][:200]}...")

        return "\n".join(response_lines)

    def _call_gemini_api(self, api_key: str, query: str, context: str) -> str:
        """Invokes Gemini REST API with retrieval context."""
        url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={api_key}"
        prompt = (
            f"You are DataStream AI's OTT Knowledge Assistant. Answer the question accurately using ONLY the provided context.\n\n"
            f"Context:\n{context}\n\n"
            f"Question: {query}\n"
            f"Answer concisely in clean markdown with citations."
        )
        payload = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {"temperature": 0.2, "maxOutputTokens": 600}
        }
        res = requests.post(url, json=payload, timeout=8)
        if res.status_code == 200:
            data = res.json()
            return data["candidates"][0]["content"]["parts"][0]["text"]
        else:
            raise RuntimeError(f"Gemini API returned status {res.status_code}: {res.text}")


rag_engine = RAGEngine()
