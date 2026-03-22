from typing import Literal, Optional

from mem0.configs.embeddings.base import BaseEmbedderConfig
from mem0.embeddings.base import EmbeddingBase

try:
    import voyageai
except ImportError:
    raise ImportError("The 'voyageai' library is required. Install with: pip install voyageai")


class VoyageEmbedding(EmbeddingBase):
    def __init__(self, config: Optional[BaseEmbedderConfig] = None):
        super().__init__(config)
        self.config.model = self.config.model or "voyage-4"
        self.config.embedding_dims = self.config.embedding_dims or 1024

        api_key = self.config.api_key
        self.client = voyageai.Client(api_key=api_key)

    def embed(self, text, memory_action: Optional[Literal["add", "search", "update"]] = None):
        # Map mem0 memory_action to Voyage input_type for better retrieval quality
        input_type_map = {
            "add": "document",
            "search": "query",
            "update": "document",
        }
        input_type = input_type_map.get(memory_action)

        result = self.client.embed(
            texts=[text] if isinstance(text, str) else text,
            model=self.config.model,
            input_type=input_type,
        )
        return result.embeddings[0]
