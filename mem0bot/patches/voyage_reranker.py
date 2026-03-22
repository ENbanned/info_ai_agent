from typing import Any, Dict, List, Optional

try:
    import voyageai
except ImportError:
    raise ImportError("The 'voyageai' library is required. Install with: uv add voyageai")

from mem0.configs.rerankers.base import BaseRerankerConfig
from mem0.reranker.base import BaseReranker


class VoyageReranker(BaseReranker):
    """Reranker using Voyage AI with adaptive knee cutoff."""

    def __init__(self, config=None):
        self.config = config
        api_key = config.api_key if config else None
        self.client = voyageai.Client(api_key=api_key)
        self.model = (config.model if config else None) or "rerank-2.5"
        self.max_k = getattr(config, "max_k", 7) if config else 7
        self.min_score = getattr(config, "min_score", 0.4) if config else 0.4

    def rerank(
        self,
        query: str,
        documents: List[Dict[str, Any]],
        top_k: int = None,
    ) -> List[Dict[str, Any]]:
        if not documents:
            return documents

        doc_texts = [
            doc.get("memory") or doc.get("text") or doc.get("content") or str(doc)
            for doc in documents
        ]

        response = self.client.rerank(
            query=query,
            documents=doc_texts,
            model=self.model,
            top_k=len(documents),  # get all scores, then apply adaptive cutoff
        )

        # Build scored list
        scored = []
        for result in response.results:
            doc = documents[result.index].copy()
            doc["rerank_score"] = result.relevance_score
            scored.append(doc)

        # Adaptive knee cutoff
        scored.sort(key=lambda x: x["rerank_score"], reverse=True)
        cutoff_idx = self._find_knee(scored)

        # Apply cutoff, cap at max_k, ensure at least 1 result
        result = scored[:cutoff_idx]
        if not result and scored:
            result = [scored[0]]
        return result[:self.max_k]

    def _find_knee(self, scored: List[Dict]) -> int:
        """Find the knee point — largest score gap between consecutive results."""
        if len(scored) <= 1:
            return len(scored)

        scores = [d["rerank_score"] for d in scored]

        # Find largest gap
        max_gap = 0
        max_gap_idx = len(scores)
        for i in range(len(scores) - 1):
            gap = scores[i] - scores[i + 1]
            if gap > max_gap:
                max_gap = gap
                max_gap_idx = i + 1

        # If no significant gap found, use min_score threshold
        if max_gap < 0.1:
            for i, score in enumerate(scores):
                if score < self.min_score:
                    return max(i, 1)
            return len(scores)

        return max_gap_idx
