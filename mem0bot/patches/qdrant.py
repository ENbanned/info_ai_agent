import logging
import os
import shutil

from qdrant_client import QdrantClient, models
from qdrant_client.models import (
    Distance,
    FieldCondition,
    Filter,
    MatchAny,
    MatchValue,
    PointIdsList,
    PointStruct,
    Range,
    VectorParams,
    SparseVectorParams,
    Modifier,
    Prefetch,
    FusionQuery,
    Fusion,
)

from mem0.vector_stores.base import VectorStoreBase

logger = logging.getLogger(__name__)


class Qdrant(VectorStoreBase):
    def __init__(
        self,
        collection_name: str,
        embedding_model_dims: int,
        client: QdrantClient = None,
        host: str = None,
        port: int = None,
        path: str = None,
        url: str = None,
        api_key: str = None,
        on_disk: bool = False,
    ):
        if client:
            self.client = client
            self.is_local = False
        else:
            params = {}
            if api_key:
                params["api_key"] = api_key
            if url:
                params["url"] = url
            if host and port:
                params["host"] = host
                params["port"] = port

            if not params:
                params["path"] = path
                self.is_local = True
                if not on_disk:
                    if os.path.exists(path) and os.path.isdir(path):
                        shutil.rmtree(path)
            else:
                self.is_local = False

            self.client = QdrantClient(**params)

        self.collection_name = collection_name
        self.embedding_model_dims = embedding_model_dims
        self.on_disk = on_disk
        self.create_col(embedding_model_dims, on_disk)

    def create_col(self, vector_size: int, on_disk: bool, distance: Distance = Distance.COSINE):
        """Create collection with named dense + BM25 sparse vectors for hybrid search."""
        response = self.list_cols()
        for collection in response.collections:
            if collection.name == self.collection_name:
                logger.debug(f"Collection {self.collection_name} already exists. Skipping creation.")
                self._create_filter_indexes()
                return

        self.client.create_collection(
            collection_name=self.collection_name,
            vectors_config={
                "dense": VectorParams(size=vector_size, distance=distance, on_disk=on_disk),
            },
            sparse_vectors_config={
                "bm25": SparseVectorParams(modifier=Modifier.IDF),
            },
        )
        self._create_filter_indexes()

    def _create_filter_indexes(self):
        """Create indexes for commonly used filter fields."""
        if self.is_local:
            logger.debug("Skipping payload index creation for local Qdrant")
            return

        keyword_fields = ["user_id", "agent_id", "run_id", "actor_id", "source", "lifecycle_state"]
        integer_fields = ["timestamp", "times_reported", "reported_at"]

        for field in keyword_fields:
            try:
                self.client.create_payload_index(
                    collection_name=self.collection_name,
                    field_name=field,
                    field_schema="keyword"
                )
                logger.info(f"Created keyword index for {field}")
            except Exception as e:
                logger.debug(f"Index for {field} might already exist: {e}")

        for field in integer_fields:
            try:
                self.client.create_payload_index(
                    collection_name=self.collection_name,
                    field_name=field,
                    field_schema="integer"
                )
                logger.info(f"Created integer index for {field}")
            except Exception as e:
                logger.debug(f"Index for {field} might already exist: {e}")

    def insert(self, vectors: list, payloads: list = None, ids: list = None):
        """Insert vectors with both dense embeddings and BM25 sparse vectors."""
        logger.info(f"Inserting {len(vectors)} vectors into collection {self.collection_name}")
        points = []
        for idx, vector in enumerate(vectors):
            payload = payloads[idx] if payloads else {}
            point_id = idx if ids is None else ids[idx]

            # Extract text for BM25 from payload
            text = payload.get("data", "") if payload else ""

            point_vector = {"dense": vector}
            if text:
                point_vector["bm25"] = models.Document(text=text, model="Qdrant/bm25")

            points.append(PointStruct(id=point_id, vector=point_vector, payload=payload))

        self.client.upsert(collection_name=self.collection_name, points=points)

    def _create_filter(self, filters: dict) -> Filter:
        if not filters:
            return None

        conditions = []
        for key, value in filters.items():
            if isinstance(value, dict) and ("gte" in value or "lte" in value):
                range_kwargs = {}
                if "gte" in value:
                    range_kwargs["gte"] = value["gte"]
                if "lte" in value:
                    range_kwargs["lte"] = value["lte"]
                conditions.append(FieldCondition(key=key, range=Range(**range_kwargs)))
            elif isinstance(value, list):
                # List of values: match any (e.g. lifecycle_state: ["new", "developing"])
                conditions.append(FieldCondition(key=key, match=MatchAny(any=value)))
            else:
                conditions.append(FieldCondition(key=key, match=MatchValue(value=value)))
        return Filter(must=conditions) if conditions else None

    def search(self, query: str, vectors: list, limit: int = 5, filters: dict = None) -> list:
        """Hybrid search: BM25 sparse + dense vectors fused with RRF."""
        query_filter = self._create_filter(filters) if filters else None

        prefetch = [
            Prefetch(query=vectors, using="dense", limit=20, filter=query_filter),
        ]
        # Add BM25 branch if we have query text
        if query:
            prefetch.append(
                Prefetch(
                    query=models.Document(text=query, model="Qdrant/bm25"),
                    using="bm25",
                    limit=20,
                    filter=query_filter,
                )
            )

        hits = self.client.query_points(
            collection_name=self.collection_name,
            prefetch=prefetch,
            query=FusionQuery(fusion=Fusion.RRF),
            limit=limit,
        )
        return hits.points

    def delete(self, vector_id: int):
        self.client.delete(
            collection_name=self.collection_name,
            points_selector=PointIdsList(points=[vector_id]),
        )

    def update(self, vector_id: int, vector: list = None, payload: dict = None):
        """Update a vector and its payload."""
        if vector is not None:
            text = payload.get("data", "") if payload else ""
            point_vector = {"dense": vector}
            if text:
                point_vector["bm25"] = models.Document(text=text, model="Qdrant/bm25")
            point = PointStruct(id=vector_id, vector=point_vector, payload=payload)
            self.client.upsert(collection_name=self.collection_name, points=[point])
        elif payload is not None:
            # Metadata-only update — no vector needed
            self.client.overwrite_payload(
                collection_name=self.collection_name,
                payload=payload,
                points=[vector_id],
            )

    def update_payload(self, vector_id: str, payload: dict) -> None:
        """Update specific payload fields without touching the vector.

        Unlike update() which uses overwrite_payload (replaces entire payload),
        this uses set_payload to merge only the provided fields. This is ideal
        for lifecycle transitions (e.g. marking facts as reported) without
        recalculating embeddings or losing other payload fields.
        """
        self.client.set_payload(
            collection_name=self.collection_name,
            payload=payload,
            points=[vector_id],
        )

    def get(self, vector_id: int) -> dict:
        result = self.client.retrieve(collection_name=self.collection_name, ids=[vector_id], with_payload=True)
        return result[0] if result else None

    def list_cols(self) -> list:
        return self.client.get_collections()

    def delete_col(self):
        self.client.delete_collection(collection_name=self.collection_name)

    def col_info(self) -> dict:
        return self.client.get_collection(collection_name=self.collection_name)

    def list(self, filters: dict = None, limit: int = 100) -> list:
        """Single-page scroll. Used by mem0 internals."""
        query_filter = self._create_filter(filters) if filters else None
        result = self.client.scroll(
            collection_name=self.collection_name,
            scroll_filter=query_filter,
            limit=limit,
            with_payload=True,
            with_vectors=False,
        )
        return result

    def scroll_all(self, filters: dict = None, page_size: int = 100) -> list:
        """Paginated scroll: returns ALL records matching the filter."""
        query_filter = self._create_filter(filters) if filters else None
        all_points = []
        offset = None

        while True:
            points, next_offset = self.client.scroll(
                collection_name=self.collection_name,
                scroll_filter=query_filter,
                limit=page_size,
                offset=offset,
                with_payload=True,
                with_vectors=False,
            )
            all_points.extend(points)
            if next_offset is None:
                break
            offset = next_offset

        return all_points

    def scroll_with_filter(self, filters: dict, limit: int = 500, page_size: int = 100) -> list:
        """Scroll with filter, capped at limit. More memory-efficient than scroll_all.

        Unlike scroll_all() which loads ALL matching records into memory,
        this stops as soon as `limit` records are collected. Use this
        for lifecycle queries where you need e.g. the first 500 active facts
        without risking OOM on large collections.
        """
        query_filter = self._create_filter(filters) if filters else None
        results = []
        offset = None

        while len(results) < limit:
            batch_size = min(page_size, limit - len(results))
            points, next_offset = self.client.scroll(
                collection_name=self.collection_name,
                scroll_filter=query_filter,
                limit=batch_size,
                offset=offset,
                with_payload=True,
                with_vectors=False,
            )
            results.extend(points)
            if next_offset is None:
                break
            offset = next_offset

        return results

    def count(self, filters: dict = None) -> int:
        """Count records matching the filter."""
        query_filter = self._create_filter(filters) if filters else None
        result = self.client.count(
            collection_name=self.collection_name,
            count_filter=query_filter,
            exact=True,
        )
        return result.count

    def reset(self):
        """Reset the index by deleting and recreating it."""
        logger.warning(f"Resetting index {self.collection_name}...")
        self.delete_col()
        self.create_col(self.embedding_model_dims, self.on_disk)
