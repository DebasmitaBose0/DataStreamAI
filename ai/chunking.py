"""
DataStream AI - Document Extraction and Text Chunking Engine
Splits unstructured documents into semantic sliding-window chunks with overlap.
"""

from typing import List, Dict, Any
import re


class DocumentChunker:
    def __init__(self, chunk_size: int = 250, chunk_overlap: int = 40):
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap

    def clean_text(self, text: str) -> str:
        """Cleans whitespace, newlines, and special characters."""
        if not text:
            return ""
        # Normalize linebreaks and multiple spaces
        text = re.sub(r"\r\n", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = re.sub(r"[ \t]+", " ", text)
        return text.strip()

    def chunk_text(self, text: str, doc_id: str = "doc", metadata: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """Splits text into chunks by word boundaries with specified overlap."""
        cleaned = self.clean_text(text)
        words = cleaned.split()
        if not words:
            return []

        chunks = []
        start_idx = 0
        chunk_num = 1

        while start_idx < len(words):
            end_idx = min(start_idx + self.chunk_size, len(words))
            chunk_words = words[start_idx:end_idx]
            chunk_str = " ".join(chunk_words)

            chunks.append({
                "chunk_id": f"{doc_id}_c{chunk_num}",
                "doc_id": doc_id,
                "text": chunk_str,
                "word_count": len(chunk_words),
                "start_word_index": start_idx,
                "metadata": metadata or {}
            })

            chunk_num += 1
            if end_idx == len(words):
                break
            start_idx += self.chunk_size - self.chunk_overlap

        return chunks


chunker = DocumentChunker()
