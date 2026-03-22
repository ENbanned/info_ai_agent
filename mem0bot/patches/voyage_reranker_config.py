from typing import Optional

from pydantic import Field

from mem0.configs.rerankers.base import BaseRerankerConfig


class VoyageRerankerConfig(BaseRerankerConfig):
    """Configuration for Voyage AI reranker."""
    model: Optional[str] = "rerank-2.5"
    top_k: int = 5
    max_k: int = Field(default=7, description="Maximum results after adaptive cutoff")
    min_score: float = Field(default=0.4, description="Minimum relevance score fallback threshold")
