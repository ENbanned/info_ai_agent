import asyncio
import concurrent
import gc
import hashlib
import json
import logging
import os
import time
import uuid
import warnings
from copy import deepcopy
from datetime import datetime
from typing import Any, Dict, Optional

import pytz
from pydantic import ValidationError

from mem0.configs.base import MemoryConfig, MemoryItem
from mem0.configs.enums import MemoryType
from mem0.configs.prompts import (
    PROCEDURAL_MEMORY_SYSTEM_PROMPT,
    get_update_memory_messages,
)
from mem0.exceptions import ValidationError as Mem0ValidationError
from mem0.memory.base import MemoryBase
from mem0.memory.setup import mem0_dir, setup_config
from mem0.memory.storage import SQLiteManager
from mem0.memory.telemetry import capture_event
from mem0.memory.utils import (
    extract_json,
    get_fact_retrieval_messages,
    parse_messages,
    parse_vision_messages,
    process_telemetry_filters,
    remove_code_blocks,
)
from mem0.utils.factory import (
    EmbedderFactory,
    GraphStoreFactory,
    LlmFactory,
    VectorStoreFactory,
    RerankerFactory,
)

# Suppress SWIG deprecation warnings globally
warnings.filterwarnings("ignore", category=DeprecationWarning, message=".*SwigPy.*")
warnings.filterwarnings("ignore", category=DeprecationWarning, message=".*swigvarlink.*")

# Initialize logger early for util functions
logger = logging.getLogger(__name__)


def _safe_deepcopy_config(config):
    """Safely deepcopy config, falling back to JSON serialization for non-serializable objects."""
    try:
        return deepcopy(config)
    except Exception as e:
        logger.debug(f"Deepcopy failed, using JSON serialization: {e}")
        
        config_class = type(config)
        
        if hasattr(config, "model_dump"):
            try:
                clone_dict = config.model_dump(mode="json")
            except Exception:
                clone_dict = {k: v for k, v in config.__dict__.items()}
        elif hasattr(config, "__dataclass_fields__"):
            from dataclasses import asdict
            clone_dict = asdict(config)
        else:
            clone_dict = {k: v for k, v in config.__dict__.items()}
        
        sensitive_tokens = ("auth", "credential", "password", "token", "secret", "key", "connection_class")
        for field_name in list(clone_dict.keys()):
            if any(token in field_name.lower() for token in sensitive_tokens):
                clone_dict[field_name] = None
        
        try:
            return config_class(**clone_dict)
        except Exception as reconstruction_error:
            logger.warning(
                f"Failed to reconstruct config: {reconstruction_error}. "
                f"Telemetry may be affected."
            )
            raise


def _build_filters_and_metadata(
    *,  # Enforce keyword-only arguments
    user_id: Optional[str] = None,
    agent_id: Optional[str] = None,
    run_id: Optional[str] = None,
    actor_id: Optional[str] = None,  # For query-time filtering
    input_metadata: Optional[Dict[str, Any]] = None,
    input_filters: Optional[Dict[str, Any]] = None,
) -> tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Constructs metadata for storage and filters for querying based on session and actor identifiers.

    This helper supports multiple session identifiers (`user_id`, `agent_id`, and/or `run_id`)
    for flexible session scoping and optionally narrows queries to a specific `actor_id`. It returns two dicts:

    1. `base_metadata_template`: Used as a template for metadata when storing new memories.
       It includes all provided session identifier(s) and any `input_metadata`.
    2. `effective_query_filters`: Used for querying existing memories. It includes all
       provided session identifier(s), any `input_filters`, and a resolved actor
       identifier for targeted filtering if specified by any actor-related inputs.

    Actor filtering precedence: explicit `actor_id` arg -> `filters["actor_id"]`
    This resolved actor ID is used for querying but is not added to `base_metadata_template`,
    as the actor for storage is typically derived from message content at a later stage.

    Args:
        user_id (Optional[str]): User identifier, for session scoping.
        agent_id (Optional[str]): Agent identifier, for session scoping.
        run_id (Optional[str]): Run identifier, for session scoping.
        actor_id (Optional[str]): Explicit actor identifier, used as a potential source for
            actor-specific filtering. See actor resolution precedence in the main description.
        input_metadata (Optional[Dict[str, Any]]): Base dictionary to be augmented with
            session identifiers for the storage metadata template. Defaults to an empty dict.
        input_filters (Optional[Dict[str, Any]]): Base dictionary to be augmented with
            session and actor identifiers for query filters. Defaults to an empty dict.

    Returns:
        tuple[Dict[str, Any], Dict[str, Any]]: A tuple containing:
            - base_metadata_template (Dict[str, Any]): Metadata template for storing memories,
              scoped to the provided session(s).
            - effective_query_filters (Dict[str, Any]): Filters for querying memories,
              scoped to the provided session(s) and potentially a resolved actor.
    """

    base_metadata_template = deepcopy(input_metadata) if input_metadata else {}
    effective_query_filters = deepcopy(input_filters) if input_filters else {}

    # ---------- add all provided session ids ----------
    session_ids_provided = []

    if user_id:
        base_metadata_template["user_id"] = user_id
        effective_query_filters["user_id"] = user_id
        session_ids_provided.append("user_id")

    if agent_id:
        base_metadata_template["agent_id"] = agent_id
        effective_query_filters["agent_id"] = agent_id
        session_ids_provided.append("agent_id")

    if run_id:
        base_metadata_template["run_id"] = run_id
        effective_query_filters["run_id"] = run_id
        session_ids_provided.append("run_id")

    if not session_ids_provided:
        raise Mem0ValidationError(
            message="At least one of 'user_id', 'agent_id', or 'run_id' must be provided.",
            error_code="VALIDATION_001",
            details={"provided_ids": {"user_id": user_id, "agent_id": agent_id, "run_id": run_id}},
            suggestion="Please provide at least one identifier to scope the memory operation."
        )

    # ---------- optional actor filter ----------
    resolved_actor_id = actor_id or effective_query_filters.get("actor_id")
    if resolved_actor_id:
        effective_query_filters["actor_id"] = resolved_actor_id

    return base_metadata_template, effective_query_filters


setup_config()
logger = logging.getLogger(__name__)


class Memory(MemoryBase):
    def __init__(self, config: MemoryConfig = MemoryConfig()):
        self.config = config

        self.custom_fact_extraction_prompt = self.config.custom_fact_extraction_prompt
        self.custom_update_memory_prompt = self.config.custom_update_memory_prompt
        self.embedding_model = EmbedderFactory.create(
            self.config.embedder.provider,
            self.config.embedder.config,
            self.config.vector_store.config,
        )
        self.vector_store = VectorStoreFactory.create(
            self.config.vector_store.provider, self.config.vector_store.config
        )
        self.llm = LlmFactory.create(self.config.llm.provider, self.config.llm.config)
        self.db = SQLiteManager(self.config.history_db_path)
        self.collection_name = self.config.vector_store.config.collection_name
        self.api_version = self.config.version
        
        # Initialize reranker if configured
        self.reranker = None
        if config.reranker:
            self.reranker = RerankerFactory.create(
                config.reranker.provider, 
                config.reranker.config
            )

        self.enable_graph = False

        if self.config.graph_store.config:
            provider = self.config.graph_store.provider
            self.graph = GraphStoreFactory.create(provider, self.config)
            self.enable_graph = True
        else:
            self.graph = None
        # Create telemetry config manually to avoid deepcopy issues with thread locks
        telemetry_config_dict = {}
        if hasattr(self.config.vector_store.config, 'model_dump'):
            # For pydantic models
            telemetry_config_dict = self.config.vector_store.config.model_dump()
        else:
            # For other objects, manually copy common attributes
            for attr in ['host', 'port', 'path', 'api_key', 'index_name', 'dimension', 'metric']:
                if hasattr(self.config.vector_store.config, attr):
                    telemetry_config_dict[attr] = getattr(self.config.vector_store.config, attr)

        # Override collection name for telemetry
        telemetry_config_dict['collection_name'] = "mem0migrations"

        # Set path for file-based vector stores
        telemetry_config = _safe_deepcopy_config(self.config.vector_store.config)
        if self.config.vector_store.provider in ["faiss", "qdrant"]:
            provider_path = f"migrations_{self.config.vector_store.provider}"
            telemetry_config_dict['path'] = os.path.join(mem0_dir, provider_path)
            os.makedirs(telemetry_config_dict['path'], exist_ok=True)

        # Create the config object using the same class as the original
        telemetry_config = self.config.vector_store.config.__class__(**telemetry_config_dict)
        self._telemetry_vector_store = VectorStoreFactory.create(
            self.config.vector_store.provider, telemetry_config
        )
        capture_event("mem0.init", self, {"sync_type": "sync"})

    @classmethod
    def from_config(cls, config_dict: Dict[str, Any]):
        try:
            config = cls._process_config(config_dict)
            config = MemoryConfig(**config_dict)
        except ValidationError as e:
            logger.error(f"Configuration validation error: {e}")
            raise
        return cls(config)

    @staticmethod
    def _process_config(config_dict: Dict[str, Any]) -> Dict[str, Any]:
        if "graph_store" in config_dict:
            if "vector_store" not in config_dict and "embedder" in config_dict:
                config_dict["vector_store"] = {}
                config_dict["vector_store"]["config"] = {}
                config_dict["vector_store"]["config"]["embedding_model_dims"] = config_dict["embedder"]["config"][
                    "embedding_dims"
                ]
        try:
            return config_dict
        except ValidationError as e:
            logger.error(f"Configuration validation error: {e}")
            raise

    def _should_use_agent_memory_extraction(self, messages, metadata):
        """Determine whether to use agent memory extraction based on the logic:
        - If agent_id is present and messages contain assistant role -> True
        - Otherwise -> False
        """
        has_agent_id = metadata.get("agent_id") is not None
        has_assistant_messages = any(msg.get("role") == "assistant" for msg in messages)
        return has_agent_id and has_assistant_messages

    def add(
        self,
        messages,
        *,
        user_id: Optional[str] = None,
        agent_id: Optional[str] = None,
        run_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        infer: bool = True,
        memory_type: Optional[str] = None,
        prompt: Optional[str] = None,
    ):
        """
        Create a new memory.

        Args:
            messages (str or List[Dict[str, str]]): The message content or list of messages.
            user_id (str, optional): ID of the user creating the memory. Defaults to None.
            agent_id (str, optional): ID of the agent creating the memory. Defaults to None.
            run_id (str, optional): ID of the run creating the memory. Defaults to None.
            metadata (dict, optional): Metadata to store with the memory. Defaults to None.
            infer (bool, optional): If True, LLM extracts facts. If False, raw storage.
            memory_type (str, optional): Type of memory to create. Defaults to None.
            prompt (str, optional): Prompt to use for the memory creation. Defaults to None.

        Returns:
            dict: A dictionary containing the result of the memory addition operation.
        """

        processed_metadata, effective_filters = _build_filters_and_metadata(
            user_id=user_id,
            agent_id=agent_id,
            run_id=run_id,
            input_metadata=metadata,
        )

        if memory_type is not None and memory_type != MemoryType.PROCEDURAL.value:
            raise Mem0ValidationError(
                message=f"Invalid 'memory_type'. Please pass {MemoryType.PROCEDURAL.value} to create procedural memories.",
                error_code="VALIDATION_002",
                details={"provided_type": memory_type, "valid_type": MemoryType.PROCEDURAL.value},
                suggestion=f"Use '{MemoryType.PROCEDURAL.value}' to create procedural memories."
            )

        if isinstance(messages, str):
            messages = [{"role": "user", "content": messages}]

        elif isinstance(messages, dict):
            messages = [messages]

        elif not isinstance(messages, list):
            raise Mem0ValidationError(
                message="messages must be str, dict, or list[dict]",
                error_code="VALIDATION_003",
                details={"provided_type": type(messages).__name__, "valid_types": ["str", "dict", "list[dict]"]},
                suggestion="Convert your input to a string, dictionary, or list of dictionaries."
            )

        if agent_id is not None and memory_type == MemoryType.PROCEDURAL.value:
            results = self._create_procedural_memory(messages, metadata=processed_metadata, prompt=prompt)
            return results

        if self.config.llm.config.get("enable_vision"):
            messages = parse_vision_messages(messages, self.llm, self.config.llm.config.get("vision_details"))
        else:
            messages = parse_vision_messages(messages)

        with concurrent.futures.ThreadPoolExecutor() as executor:
            future1 = executor.submit(self._add_to_vector_store, messages, processed_metadata, effective_filters, infer)
            future2 = executor.submit(self._add_to_graph, messages, effective_filters)

            concurrent.futures.wait([future1, future2])

            vector_store_result = future1.result()
            graph_result = future2.result()

        if self.enable_graph:
            return {
                "results": vector_store_result,
                "relations": graph_result,
            }

        return {"results": vector_store_result}

    def _add_to_vector_store(self, messages, metadata, filters, infer):
        if not infer:
            returned_memories = []
            for message_dict in messages:
                if (
                    not isinstance(message_dict, dict)
                    or message_dict.get("role") is None
                    or message_dict.get("content") is None
                ):
                    logger.warning(f"Skipping invalid message format: {message_dict}")
                    continue

                if message_dict["role"] == "system":
                    continue

                per_msg_meta = deepcopy(metadata)
                per_msg_meta["role"] = message_dict["role"]

                actor_name = message_dict.get("name")
                if actor_name:
                    per_msg_meta["actor_id"] = actor_name

                msg_content = message_dict["content"]
                msg_embeddings = self.embedding_model.embed(msg_content, "add")
                mem_id = self._create_memory(msg_content, msg_embeddings, per_msg_meta)

                returned_memories.append(
                    {
                        "id": mem_id,
                        "memory": msg_content,
                        "event": "ADD",
                        "actor_id": actor_name if actor_name else None,
                        "role": message_dict["role"],
                    }
                )
            return returned_memories

        parsed_messages = parse_messages(messages)

        if self.config.custom_fact_extraction_prompt:
            system_prompt = self.config.custom_fact_extraction_prompt
            user_prompt = f"Input:\n{parsed_messages}"
        else:
            is_agent_memory = self._should_use_agent_memory_extraction(messages, metadata)
            system_prompt, user_prompt = get_fact_retrieval_messages(parsed_messages, is_agent_memory)

        response = self.llm.generate_response(
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            response_format={"type": "json_object"},
        )

        try:
            response = remove_code_blocks(response)
            if not response.strip():
                new_retrieved_facts = []
            else:
                try:
                    new_retrieved_facts = json.loads(response)["facts"]
                except json.JSONDecodeError:
                    extracted_json = extract_json(response)
                    new_retrieved_facts = json.loads(extracted_json)["facts"]
        except Exception as e:
            logger.error(f"Error in new_retrieved_facts: {e}")
            new_retrieved_facts = []

        if not new_retrieved_facts:
            logger.debug("No new facts retrieved from input. Skipping memory update LLM call.")

        retrieved_old_memory = []
        new_message_embeddings = {}
        search_filters = {}
        if filters.get("user_id"):
            search_filters["user_id"] = filters["user_id"]
        if filters.get("agent_id"):
            search_filters["agent_id"] = filters["agent_id"]
        if filters.get("run_id"):
            search_filters["run_id"] = filters["run_id"]
        for new_mem in new_retrieved_facts:
            messages_embeddings = self.embedding_model.embed(new_mem, "add")
            new_message_embeddings[new_mem] = messages_embeddings
            existing_memories = self.vector_store.search(
                query=new_mem,
                vectors=messages_embeddings,
                limit=5,
                filters=search_filters,
            )
            for mem in existing_memories:
                retrieved_old_memory.append({"id": mem.id, "text": mem.payload.get("data", "")})

        unique_data = {}
        for item in retrieved_old_memory:
            unique_data[item["id"]] = item
        retrieved_old_memory = list(unique_data.values())
        logger.info(f"Total existing memories: {len(retrieved_old_memory)}")

        # mapping UUIDs with integers for handling UUID hallucinations
        temp_uuid_mapping = {}
        for idx, item in enumerate(retrieved_old_memory):
            temp_uuid_mapping[str(idx)] = item["id"]
            retrieved_old_memory[idx]["id"] = str(idx)

        if new_retrieved_facts:
            function_calling_prompt = get_update_memory_messages(
                retrieved_old_memory, new_retrieved_facts, self.config.custom_update_memory_prompt
            )

            try:
                response: str = self.llm.generate_response(
                    messages=[{"role": "user", "content": function_calling_prompt}],
                    response_format={"type": "json_object"},
                )
            except Exception as e:
                logger.error(f"Error in new memory actions response: {e}")
                response = ""

            try:
                if not response or not response.strip():
                    logger.warning("Empty response from LLM, no memories to extract")
                    new_memories_with_actions = {}
                else:
                    response = remove_code_blocks(response)
                    new_memories_with_actions = json.loads(response)
            except Exception as e:
                logger.error(f"Invalid JSON response: {e}")
                new_memories_with_actions = {}
        else:
            new_memories_with_actions = {}

        returned_memories = []
        try:
            for resp in new_memories_with_actions.get("memory", []):
                logger.info(resp)
                try:
                    action_text = resp.get("text")
                    if not action_text:
                        logger.info("Skipping memory entry because of empty `text` field.")
                        continue

                    event_type = resp.get("event")
                    if event_type == "ADD":
                        memory_id = self._create_memory(
                            data=action_text,
                            existing_embeddings=new_message_embeddings,
                            metadata=deepcopy(metadata),
                        )
                        returned_memories.append({"id": memory_id, "memory": action_text, "event": event_type})
                    elif event_type == "UPDATE":
                        self._update_memory(
                            memory_id=temp_uuid_mapping[resp.get("id")],
                            data=action_text,
                            existing_embeddings=new_message_embeddings,
                            metadata=deepcopy(metadata),
                        )
                        returned_memories.append(
                            {
                                "id": temp_uuid_mapping[resp.get("id")],
                                "memory": action_text,
                                "event": event_type,
                                "previous_memory": resp.get("old_memory"),
                            }
                        )
                    elif event_type == "DELETE":
                        self._delete_memory(memory_id=temp_uuid_mapping[resp.get("id")])
                        returned_memories.append(
                            {
                                "id": temp_uuid_mapping[resp.get("id")],
                                "memory": action_text,
                                "event": event_type,
                            }
                        )
                    elif event_type == "NONE":
                        memory_id = temp_uuid_mapping.get(resp.get("id"))
                        if memory_id and (metadata.get("agent_id") or metadata.get("run_id")):
                            existing_memory = self.vector_store.get(vector_id=memory_id)
                            updated_metadata = deepcopy(existing_memory.payload)
                            if metadata.get("agent_id"):
                                updated_metadata["agent_id"] = metadata["agent_id"]
                            if metadata.get("run_id"):
                                updated_metadata["run_id"] = metadata["run_id"]
                            updated_metadata["updated_at"] = datetime.now(pytz.timezone("US/Pacific")).isoformat()

                            self.vector_store.update(
                                vector_id=memory_id,
                                vector=None,
                                payload=updated_metadata,
                            )
                            logger.info(f"Updated session IDs for memory {memory_id}")
                        else:
                            logger.info("NOOP for Memory.")
                except Exception as e:
                    logger.error(f"Error processing memory action: {resp}, Error: {e}")
        except Exception as e:
            logger.error(f"Error iterating new_memories_with_actions: {e}")

        keys, encoded_ids = process_telemetry_filters(filters)
        capture_event(
            "mem0.add",
            self,
            {"version": self.api_version, "keys": keys, "encoded_ids": encoded_ids, "sync_type": "sync"},
        )
        return returned_memories

    def _add_to_graph(self, messages, filters):
        added_entities = []
        if self.enable_graph:
            if filters.get("user_id") is None:
                filters["user_id"] = "user"

            data = "\n".join([msg["content"] for msg in messages if "content" in msg and msg["role"] != "system"])
            added_entities = self.graph.add(data, filters)

        return added_entities

    def get(self, memory_id):
        """
        Retrieve a memory by ID.

        Args:
            memory_id (str): ID of the memory to retrieve.

        Returns:
            dict: Retrieved memory.
        """
        capture_event("mem0.get", self, {"memory_id": memory_id, "sync_type": "sync"})
        memory = self.vector_store.get(vector_id=memory_id)
        if not memory:
            return None

        promoted_payload_keys = [
            "user_id",
            "agent_id",
            "run_id",
            "actor_id",
            "role",
        ]

        core_and_promoted_keys = {"data", "hash", "created_at", "updated_at", "id", *promoted_payload_keys}

        result_item = MemoryItem(
            id=memory.id,
            memory=memory.payload.get("data", ""),
            hash=memory.payload.get("hash"),
            created_at=memory.payload.get("created_at"),
            updated_at=memory.payload.get("updated_at"),
        ).model_dump()

        for key in promoted_payload_keys:
            if key in memory.payload:
                result_item[key] = memory.payload[key]

        additional_metadata = {k: v for k, v in memory.payload.items() if k not in core_and_promoted_keys}
        if additional_metadata:
            result_item["metadata"] = additional_metadata

        return result_item

    def get_all(
        self,
        *,
        user_id: Optional[str] = None,
        agent_id: Optional[str] = None,
        run_id: Optional[str] = None,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 100,
    ):
        """
        List all memories.

        Args:
            user_id (str, optional): user id
            agent_id (str, optional): agent id
            run_id (str, optional): run id
            filters (dict, optional): Additional custom key-value filters.
            limit (int, optional): The maximum number of memories to return. Defaults to 100.

        Returns:
            dict: A dictionary containing a list of memories under the "results" key.
        """

        _, effective_filters = _build_filters_and_metadata(
            user_id=user_id, agent_id=agent_id, run_id=run_id, input_filters=filters
        )

        if not any(key in effective_filters for key in ("user_id", "agent_id", "run_id")):
            raise ValueError("At least one of 'user_id', 'agent_id', or 'run_id' must be specified.")

        keys, encoded_ids = process_telemetry_filters(effective_filters)
        capture_event(
            "mem0.get_all", self, {"limit": limit, "keys": keys, "encoded_ids": encoded_ids, "sync_type": "sync"}
        )

        with concurrent.futures.ThreadPoolExecutor() as executor:
            future_memories = executor.submit(self._get_all_from_vector_store, effective_filters, limit)
            future_graph_entities = (
                executor.submit(self.graph.get_all, effective_filters, limit) if self.enable_graph else None
            )

            concurrent.futures.wait(
                [future_memories, future_graph_entities] if future_graph_entities else [future_memories]
            )

            all_memories_result = future_memories.result()
            graph_entities_result = future_graph_entities.result() if future_graph_entities else None

        if self.enable_graph:
            return {"results": all_memories_result, "relations": graph_entities_result}

        return {"results": all_memories_result}

    def _get_all_from_vector_store(self, filters, limit):
        memories_result = self.vector_store.list(filters=filters, limit=limit)

        if isinstance(memories_result, (tuple, list)) and len(memories_result) > 0:
            first_element = memories_result[0]
            if isinstance(first_element, (list, tuple)):
                actual_memories = first_element
            else:
                actual_memories = memories_result
        else:
            actual_memories = memories_result

        promoted_payload_keys = [
            "user_id",
            "agent_id",
            "run_id",
            "actor_id",
            "role",
        ]
        core_and_promoted_keys = {"data", "hash", "created_at", "updated_at", "id", *promoted_payload_keys}

        formatted_memories = []
        for mem in actual_memories:
            memory_item_dict = MemoryItem(
                id=mem.id,
                memory=mem.payload.get("data", ""),
                hash=mem.payload.get("hash"),
                created_at=mem.payload.get("created_at"),
                updated_at=mem.payload.get("updated_at"),
            ).model_dump(exclude={"score"})

            for key in promoted_payload_keys:
                if key in mem.payload:
                    memory_item_dict[key] = mem.payload[key]

            additional_metadata = {k: v for k, v in mem.payload.items() if k not in core_and_promoted_keys}
            if additional_metadata:
                memory_item_dict["metadata"] = additional_metadata

            formatted_memories.append(memory_item_dict)

        return formatted_memories

    def search(
        self,
        query: str,
        *,
        user_id: Optional[str] = None,
        agent_id: Optional[str] = None,
        run_id: Optional[str] = None,
        limit: int = 100,
        filters: Optional[Dict[str, Any]] = None,
        threshold: Optional[float] = None,
        rerank: bool = True,
    ):
        """
        Searches for memories based on a query.

        Args:
            query (str): Query to search for.
            user_id (str, optional): ID of the user to search for.
            agent_id (str, optional): ID of the agent to search for.
            run_id (str, optional): ID of the run to search for.
            limit (int, optional): Limit the number of results. Defaults to 100.
            filters (dict, optional): Filters to apply to the search.
            threshold (float, optional): Minimum score for inclusion.
            rerank (bool, optional): Whether to rerank results.

        Returns:
            dict: Search results.
        """
        _, effective_filters = _build_filters_and_metadata(
            user_id=user_id, agent_id=agent_id, run_id=run_id, input_filters=filters
        )

        if not any(key in effective_filters for key in ("user_id", "agent_id", "run_id")):
            raise ValueError("At least one of 'user_id', 'agent_id', or 'run_id' must be specified.")

        if filters and self._has_advanced_operators(filters):
            processed_filters = self._process_metadata_filters(filters)
            effective_filters.update(processed_filters)
        elif filters:
            effective_filters.update(filters)

        keys, encoded_ids = process_telemetry_filters(effective_filters)
        capture_event(
            "mem0.search",
            self,
            {
                "limit": limit,
                "version": self.api_version,
                "keys": keys,
                "encoded_ids": encoded_ids,
                "sync_type": "sync",
                "threshold": threshold,
                "advanced_filters": bool(filters and self._has_advanced_operators(filters)),
            },
        )

        with concurrent.futures.ThreadPoolExecutor() as executor:
            future_memories = executor.submit(self._search_vector_store, query, effective_filters, limit, threshold)
            future_graph_entities = (
                executor.submit(self.graph.search, query, effective_filters, limit) if self.enable_graph else None
            )

            concurrent.futures.wait(
                [future_memories, future_graph_entities] if future_graph_entities else [future_memories]
            )

            original_memories = future_memories.result()
            graph_entities = future_graph_entities.result() if future_graph_entities else None

        if rerank and self.reranker and original_memories:
            try:
                reranked_memories = self.reranker.rerank(query, original_memories, limit)
                original_memories = reranked_memories
            except Exception as e:
                logger.warning(f"Reranking failed, using original results: {e}")

        if self.enable_graph:
            return {"results": original_memories, "relations": graph_entities}

        return {"results": original_memories}

    def _process_metadata_filters(self, metadata_filters: Dict[str, Any]) -> Dict[str, Any]:
        """Process enhanced metadata filters and convert them to vector store compatible format."""
        processed_filters = {}
        
        def process_condition(key: str, condition: Any) -> Dict[str, Any]:
            if not isinstance(condition, dict):
                if condition == "*":
                    return {key: "*"}
                return {key: condition}
            
            result = {}
            for operator, value in condition.items():
                operator_map = {
                    "eq": "eq", "ne": "ne", "gt": "gt", "gte": "gte", 
                    "lt": "lt", "lte": "lte", "in": "in", "nin": "nin",
                    "contains": "contains", "icontains": "icontains"
                }
                
                if operator in operator_map:
                    result[key] = {operator_map[operator]: value}
                else:
                    raise ValueError(f"Unsupported metadata filter operator: {operator}")
            return result
        
        for key, value in metadata_filters.items():
            if key == "AND":
                if not isinstance(value, list):
                    raise ValueError("AND operator requires a list of conditions")
                for condition in value:
                    for sub_key, sub_value in condition.items():
                        processed_filters.update(process_condition(sub_key, sub_value))
            elif key == "OR":
                if not isinstance(value, list) or not value:
                    raise ValueError("OR operator requires a non-empty list of conditions")
                processed_filters["$or"] = []
                for condition in value:
                    or_condition = {}
                    for sub_key, sub_value in condition.items():
                        or_condition.update(process_condition(sub_key, sub_value))
                    processed_filters["$or"].append(or_condition)
            elif key == "NOT":
                if not isinstance(value, list) or not value:
                    raise ValueError("NOT operator requires a non-empty list of conditions")
                processed_filters["$not"] = []
                for condition in value:
                    not_condition = {}
                    for sub_key, sub_value in condition.items():
                        not_condition.update(process_condition(sub_key, sub_value))
                    processed_filters["$not"].append(not_condition)
            else:
                processed_filters.update(process_condition(key, value))
        
        return processed_filters

    def _has_advanced_operators(self, filters: Dict[str, Any]) -> bool:
        """Check if filters contain advanced operators that need special processing."""
        if not isinstance(filters, dict):
            return False
            
        for key, value in filters.items():
            if key in ["AND", "OR", "NOT"]:
                return True
            if isinstance(value, dict):
                for op in value.keys():
                    if op in ["eq", "ne", "gt", "gte", "lt", "lte", "in", "nin", "contains", "icontains"]:
                        return True
            if value == "*":
                return True
        return False

    def _search_vector_store(self, query, filters, limit, threshold: Optional[float] = None):
        embeddings = self.embedding_model.embed(query, "search")
        memories = self.vector_store.search(query=query, vectors=embeddings, limit=limit, filters=filters)

        promoted_payload_keys = [
            "user_id",
            "agent_id",
            "run_id",
            "actor_id",
            "role",
        ]

        core_and_promoted_keys = {"data", "hash", "created_at", "updated_at", "id", *promoted_payload_keys}

        original_memories = []
        for mem in memories:
            memory_item_dict = MemoryItem(
                id=mem.id,
                memory=mem.payload.get("data", ""),
                hash=mem.payload.get("hash"),
                created_at=mem.payload.get("created_at"),
                updated_at=mem.payload.get("updated_at"),
                score=mem.score,
            ).model_dump()

            for key in promoted_payload_keys:
                if key in mem.payload:
                    memory_item_dict[key] = mem.payload[key]

            additional_metadata = {k: v for k, v in mem.payload.items() if k not in core_and_promoted_keys}
            if additional_metadata:
                memory_item_dict["metadata"] = additional_metadata

            if threshold is None or mem.score >= threshold:
                original_memories.append(memory_item_dict)

        return original_memories

    def update(self, memory_id, data):
        """Update a memory by ID."""
        capture_event("mem0.update", self, {"memory_id": memory_id, "sync_type": "sync"})
        existing_embeddings = {data: self.embedding_model.embed(data, "update")}
        self._update_memory(memory_id, data, existing_embeddings)
        return {"message": "Memory updated successfully!"}

    def delete(self, memory_id):
        """Delete a memory by ID."""
        capture_event("mem0.delete", self, {"memory_id": memory_id, "sync_type": "sync"})
        self._delete_memory(memory_id)
        return {"message": "Memory deleted successfully!"}

    def delete_all(self, user_id: Optional[str] = None, agent_id: Optional[str] = None, run_id: Optional[str] = None):
        """Delete all memories."""
        filters: Dict[str, Any] = {}
        if user_id:
            filters["user_id"] = user_id
        if agent_id:
            filters["agent_id"] = agent_id
        if run_id:
            filters["run_id"] = run_id

        if not filters:
            raise ValueError(
                "At least one filter is required to delete all memories. If you want to delete all memories, use the `reset()` method."
            )

        keys, encoded_ids = process_telemetry_filters(filters)
        capture_event("mem0.delete_all", self, {"keys": keys, "encoded_ids": encoded_ids, "sync_type": "sync"})
        memories = self.vector_store.list(filters=filters)[0]
        for memory in memories:
            self._delete_memory(memory.id)
        self.vector_store.reset()

        logger.info(f"Deleted {len(memories)} memories")

        if self.enable_graph:
            self.graph.delete_all(filters)

        return {"message": "Memories deleted successfully!"}

    def history(self, memory_id):
        """Get the history of changes for a memory by ID."""
        capture_event("mem0.history", self, {"memory_id": memory_id, "sync_type": "sync"})
        return self.db.get_history(memory_id)

    def _create_memory(self, data, existing_embeddings, metadata=None):
        logger.debug(f"Creating memory with {data=}")
        if data in existing_embeddings:
            embeddings = existing_embeddings[data]
        else:
            embeddings = self.embedding_model.embed(data, memory_action="add")
        memory_id = str(uuid.uuid4())
        metadata = metadata or {}
        metadata["data"] = data
        metadata["hash"] = hashlib.md5(data.encode()).hexdigest()
        metadata["created_at"] = datetime.now(pytz.timezone("US/Pacific")).isoformat()

        self.vector_store.insert(
            vectors=[embeddings],
            ids=[memory_id],
            payloads=[metadata],
        )
        self.db.add_history(
            memory_id,
            None,
            data,
            "ADD",
            created_at=metadata.get("created_at"),
            actor_id=metadata.get("actor_id"),
            role=metadata.get("role"),
        )
        return memory_id

    def _create_procedural_memory(self, messages, metadata=None, prompt=None):
        """Create a procedural memory."""
        logger.info("Creating procedural memory")

        parsed_messages = [
            {"role": "system", "content": prompt or PROCEDURAL_MEMORY_SYSTEM_PROMPT},
            *messages,
            {
                "role": "user",
                "content": "Create procedural memory of the above conversation.",
            },
        ]

        try:
            procedural_memory = self.llm.generate_response(messages=parsed_messages)
            procedural_memory = remove_code_blocks(procedural_memory)
        except Exception as e:
            logger.error(f"Error generating procedural memory summary: {e}")
            raise

        if metadata is None:
            raise ValueError("Metadata cannot be done for procedural memory.")

        metadata["memory_type"] = MemoryType.PROCEDURAL.value
        embeddings = self.embedding_model.embed(procedural_memory, memory_action="add")
        memory_id = self._create_memory(procedural_memory, {procedural_memory: embeddings}, metadata=metadata)
        capture_event("mem0._create_procedural_memory", self, {"memory_id": memory_id, "sync_type": "sync"})

        result = {"results": [{"id": memory_id, "memory": procedural_memory, "event": "ADD"}]}

        return result

    def _update_memory(self, memory_id, data, existing_embeddings, metadata=None):
        logger.info(f"Updating memory with {data=}")

        try:
            existing_memory = self.vector_store.get(vector_id=memory_id)
        except Exception:
            logger.error(f"Error getting memory with ID {memory_id} during update.")
            raise ValueError(f"Error getting memory with ID {memory_id}. Please provide a valid 'memory_id'")

        prev_value = existing_memory.payload.get("data")

        new_metadata = deepcopy(metadata) if metadata is not None else {}

        new_metadata["data"] = data
        new_metadata["hash"] = hashlib.md5(data.encode()).hexdigest()
        new_metadata["created_at"] = existing_memory.payload.get("created_at")
        new_metadata["updated_at"] = datetime.now(pytz.timezone("US/Pacific")).isoformat()

        if "user_id" not in new_metadata and "user_id" in existing_memory.payload:
            new_metadata["user_id"] = existing_memory.payload["user_id"]
        if "agent_id" not in new_metadata and "agent_id" in existing_memory.payload:
            new_metadata["agent_id"] = existing_memory.payload["agent_id"]
        if "run_id" not in new_metadata and "run_id" in existing_memory.payload:
            new_metadata["run_id"] = existing_memory.payload["run_id"]
        if "actor_id" not in new_metadata and "actor_id" in existing_memory.payload:
            new_metadata["actor_id"] = existing_memory.payload["actor_id"]
        if "role" not in new_metadata and "role" in existing_memory.payload:
            new_metadata["role"] = existing_memory.payload["role"]

        if data in existing_embeddings:
            embeddings = existing_embeddings[data]
        else:
            embeddings = self.embedding_model.embed(data, "update")

        self.vector_store.update(
            vector_id=memory_id,
            vector=embeddings,
            payload=new_metadata,
        )
        logger.info(f"Updating memory with ID {memory_id=} with {data=}")

        self.db.add_history(
            memory_id,
            prev_value,
            data,
            "UPDATE",
            created_at=new_metadata["created_at"],
            updated_at=new_metadata["updated_at"],
            actor_id=new_metadata.get("actor_id"),
            role=new_metadata.get("role"),
        )
        return memory_id

    def _delete_memory(self, memory_id):
        logger.info(f"Deleting memory with {memory_id=}")
        existing_memory = self.vector_store.get(vector_id=memory_id)
        prev_value = existing_memory.payload.get("data", "")
        self.vector_store.delete(vector_id=memory_id)
        self.db.add_history(
            memory_id,
            prev_value,
            None,
            "DELETE",
            actor_id=existing_memory.payload.get("actor_id"),
            role=existing_memory.payload.get("role"),
            is_deleted=1,
        )
        return memory_id

    def reset(self):
        """
        Reset the memory store by:
            Deletes the vector store collection
            Resets the database
            Clears the graph store
            Recreates the vector store with a new client
        """
        logger.warning("Resetting all memories")

        if hasattr(self.db, "connection") and self.db.connection:
            self.db.connection.execute("DROP TABLE IF EXISTS history")
            self.db.connection.close()

        self.db = SQLiteManager(self.config.history_db_path)

        if hasattr(self.vector_store, "reset"):
            self.vector_store = VectorStoreFactory.reset(self.vector_store)
        else:
            logger.warning("Vector store does not support reset. Skipping.")
            self.vector_store.delete_col()
            self.vector_store = VectorStoreFactory.create(
                self.config.vector_store.provider, self.config.vector_store.config
            )
        if hasattr(self, "graph") and self.graph is not None:
            try:
                self.graph.reset()
            except Exception as e:
                logger.warning(f"Failed to reset graph: {e}")
        capture_event("mem0.reset", self, {"sync_type": "sync"})

    def chat(self, query):
        raise NotImplementedError("Chat function not implemented yet.")


class AsyncMemory(MemoryBase):
    def __init__(self, config: MemoryConfig = MemoryConfig()):
        self.config = config

        self.embedding_model = EmbedderFactory.create(
            self.config.embedder.provider,
            self.config.embedder.config,
            self.config.vector_store.config,
        )
        self.vector_store = VectorStoreFactory.create(
            self.config.vector_store.provider, self.config.vector_store.config
        )
        self.llm = LlmFactory.create(self.config.llm.provider, self.config.llm.config)
        self.db = SQLiteManager(self.config.history_db_path)
        self.collection_name = self.config.vector_store.config.collection_name
        self.api_version = self.config.version
        
        # Initialize reranker if configured
        self.reranker = None
        if config.reranker:
            self.reranker = RerankerFactory.create(
                config.reranker.provider, 
                config.reranker.config
            )

        self.enable_graph = False

        if self.config.graph_store.config:
            provider = self.config.graph_store.provider
            self.graph = GraphStoreFactory.create(provider, self.config)
            self.enable_graph = True
        else:
            self.graph = None

        telemetry_config = _safe_deepcopy_config(self.config.vector_store.config)
        telemetry_config.collection_name = "mem0migrations"
        if self.config.vector_store.provider in ["faiss", "qdrant"]:
            provider_path = f"migrations_{self.config.vector_store.provider}"
            telemetry_config.path = os.path.join(mem0_dir, provider_path)
            os.makedirs(telemetry_config.path, exist_ok=True)
        self._telemetry_vector_store = VectorStoreFactory.create(self.config.vector_store.provider, telemetry_config)

        capture_event("mem0.init", self, {"sync_type": "async"})

    async def _call_llm(self, **kwargs):
        """Call LLM — native async if provider supports it, otherwise via thread."""
        if hasattr(self.llm, "async_generate_response"):
            return await self.llm.async_generate_response(**kwargs)
        return await asyncio.to_thread(self.llm.generate_response, **kwargs)

    @classmethod
    async def from_config(cls, config_dict: Dict[str, Any]):
        try:
            config = cls._process_config(config_dict)
            config = MemoryConfig(**config_dict)
        except ValidationError as e:
            logger.error(f"Configuration validation error: {e}")
            raise
        return cls(config)

    @staticmethod
    def _process_config(config_dict: Dict[str, Any]) -> Dict[str, Any]:
        if "graph_store" in config_dict:
            if "vector_store" not in config_dict and "embedder" in config_dict:
                config_dict["vector_store"] = {}
                config_dict["vector_store"]["config"] = {}
                config_dict["vector_store"]["config"]["embedding_model_dims"] = config_dict["embedder"]["config"][
                    "embedding_dims"
                ]
        try:
            return config_dict
        except ValidationError as e:
            logger.error(f"Configuration validation error: {e}")
            raise

    def _should_use_agent_memory_extraction(self, messages, metadata):
        """Determine whether to use agent memory extraction."""
        has_agent_id = metadata.get("agent_id") is not None
        has_assistant_messages = any(msg.get("role") == "assistant" for msg in messages)
        return has_agent_id and has_assistant_messages

    async def add(
        self,
        messages,
        *,
        user_id: Optional[str] = None,
        agent_id: Optional[str] = None,
        run_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        infer: bool = True,
        memory_type: Optional[str] = None,
        prompt: Optional[str] = None,
        llm=None,
    ):
        """
        Create a new memory asynchronously.

        Args:
            messages (str or List[Dict[str, str]]): Messages to store in the memory.
            user_id (str, optional): ID of the user creating the memory.
            agent_id (str, optional): ID of the agent creating the memory.
            run_id (str, optional): ID of the run creating the memory.
            metadata (dict, optional): Metadata to store with the memory.
            infer (bool, optional): Whether to infer the memories. Defaults to True.
            memory_type (str, optional): Type of memory to create.
            prompt (str, optional): Prompt to use for the memory creation.
            llm (BaseChatModel, optional): LLM class to use for generating procedural memories.

        Returns:
            dict: A dictionary containing the result of the memory addition operation.
        """
        processed_metadata, effective_filters = _build_filters_and_metadata(
            user_id=user_id, agent_id=agent_id, run_id=run_id, input_metadata=metadata
        )

        if memory_type is not None and memory_type != MemoryType.PROCEDURAL.value:
            raise ValueError(
                f"Invalid 'memory_type'. Please pass {MemoryType.PROCEDURAL.value} to create procedural memories."
            )

        if isinstance(messages, str):
            messages = [{"role": "user", "content": messages}]

        elif isinstance(messages, dict):
            messages = [messages]

        elif not isinstance(messages, list):
            raise Mem0ValidationError(
                message="messages must be str, dict, or list[dict]",
                error_code="VALIDATION_003",
                details={"provided_type": type(messages).__name__, "valid_types": ["str", "dict", "list[dict]"]},
                suggestion="Convert your input to a string, dictionary, or list of dictionaries."
            )

        if agent_id is not None and memory_type == MemoryType.PROCEDURAL.value:
            results = await self._create_procedural_memory(
                messages, metadata=processed_metadata, prompt=prompt, llm=llm
            )
            return results

        if self.config.llm.config.get("enable_vision"):
            messages = parse_vision_messages(messages, self.llm, self.config.llm.config.get("vision_details"))
        else:
            messages = parse_vision_messages(messages)

        # Unified extraction: if custom_fact_extraction_prompt is set AND graph is enabled,
        # run unified extraction first to avoid duplicate LLM calls
        unified_extraction = None
        if infer and self.enable_graph and self.config.custom_fact_extraction_prompt:
            unified_extraction = await self._unified_extract(messages, effective_filters)

        if unified_extraction:
            pre_facts = unified_extraction.get("facts", [])
            pre_entities = unified_extraction.get("entities", [])
            pre_relationships = unified_extraction.get("relationships", [])
            extraction_meta = {
                "temporal_type": unified_extraction.get("temporal_type", "event"),
                "temporal_types": unified_extraction.get("temporal_types", []),
                "event_id": unified_extraction.get("event_id", ""),
            }

            vector_store_task = asyncio.create_task(
                self._add_to_vector_store(
                    messages, processed_metadata, effective_filters, infer,
                    pre_extracted_facts=pre_facts, extraction_meta=extraction_meta,
                )
            )
            graph_task = asyncio.create_task(
                self._add_to_graph(messages, effective_filters, pre_extracted=(pre_entities, pre_relationships))
            )
        else:
            vector_store_task = asyncio.create_task(
                self._add_to_vector_store(messages, processed_metadata, effective_filters, infer)
            )
            graph_task = asyncio.create_task(self._add_to_graph(messages, effective_filters))

        vector_store_result, graph_result = await asyncio.gather(vector_store_task, graph_task)

        if self.enable_graph:
            return {
                "results": vector_store_result,
                "relations": graph_result,
            }

        return {"results": vector_store_result}

    async def _unified_extract(self, messages, filters):
        """Run unified extraction: facts + entities + relationships in one LLM call."""
        parsed_messages = parse_messages(messages)
        system_prompt = self.config.custom_fact_extraction_prompt
        user_prompt = f"Input:\n{parsed_messages}"

        try:
            response = await self._call_llm(
                messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}],
                response_format={"type": "json_object"},
            )
            response = remove_code_blocks(response)
            if not response or not response.strip():
                return None

            try:
                parsed = json.loads(response)
            except json.JSONDecodeError:
                extracted = extract_json(response)
                parsed = json.loads(extracted)

            # Validate required keys
            if "facts" not in parsed:
                return None

            return {
                "facts": parsed.get("facts", []),
                "entities": parsed.get("entities", []),
                "relationships": parsed.get("relationships", []),
                "temporal_type": parsed.get("temporal_type", "event"),
                "temporal_types": parsed.get("temporal_types", []),
                "event_id": parsed.get("event_id", ""),
            }
        except Exception as e:
            logger.error(f"Unified extraction failed, falling back to separate extraction: {e}")
            return None

    async def _add_to_vector_store(
        self,
        messages: list,
        metadata: dict,
        effective_filters: dict,
        infer: bool,
        pre_extracted_facts: list = None,
        extraction_meta: dict = None,
    ):
        if not infer:
            returned_memories = []
            for message_dict in messages:
                if (
                    not isinstance(message_dict, dict)
                    or message_dict.get("role") is None
                    or message_dict.get("content") is None
                ):
                    logger.warning(f"Skipping invalid message format (async): {message_dict}")
                    continue

                if message_dict["role"] == "system":
                    continue

                per_msg_meta = deepcopy(metadata)
                per_msg_meta["role"] = message_dict["role"]

                actor_name = message_dict.get("name")
                if actor_name:
                    per_msg_meta["actor_id"] = actor_name

                msg_content = message_dict["content"]
                msg_embeddings = await asyncio.to_thread(self.embedding_model.embed, msg_content, "add")
                mem_id = await self._create_memory(msg_content, msg_embeddings, per_msg_meta)

                returned_memories.append(
                    {
                        "id": mem_id,
                        "memory": msg_content,
                        "event": "ADD",
                        "actor_id": actor_name if actor_name else None,
                        "role": message_dict["role"],
                    }
                )
            return returned_memories

        # Use pre-extracted facts from unified extraction if available
        if pre_extracted_facts is not None:
            new_retrieved_facts = pre_extracted_facts
            if extraction_meta:
                # Merge extraction metadata into base metadata,
                # but keep temporal_types separate (it's a per-fact parallel array)
                meta_to_merge = {k: v for k, v in extraction_meta.items() if k != "temporal_types"}
                metadata.update(meta_to_merge)
        else:
            parsed_messages = parse_messages(messages)
            if self.config.custom_fact_extraction_prompt:
                system_prompt = self.config.custom_fact_extraction_prompt
                user_prompt = f"Input:\n{parsed_messages}"
            else:
                is_agent_memory = self._should_use_agent_memory_extraction(messages, metadata)
                system_prompt, user_prompt = get_fact_retrieval_messages(parsed_messages, is_agent_memory)

            response = await self._call_llm(
                messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}],
                response_format={"type": "json_object"},
            )
            try:
                response = remove_code_blocks(response)
                if not response.strip():
                    new_retrieved_facts = []
                else:
                    try:
                        new_retrieved_facts = json.loads(response)["facts"]
                    except json.JSONDecodeError:
                        extracted_json = extract_json(response)
                        new_retrieved_facts = json.loads(extracted_json)["facts"]
            except Exception as e:
                logger.error(f"Error in new_retrieved_facts: {e}")
                new_retrieved_facts = []

        if not new_retrieved_facts:
            logger.debug("No new facts retrieved from input. Skipping memory update LLM call.")

        retrieved_old_memory = []
        new_message_embeddings = {}
        search_filters = {}
        if effective_filters.get("user_id"):
            search_filters["user_id"] = effective_filters["user_id"]
        if effective_filters.get("agent_id"):
            search_filters["agent_id"] = effective_filters["agent_id"]
        if effective_filters.get("run_id"):
            search_filters["run_id"] = effective_filters["run_id"]

        async def process_fact_for_search(new_mem_content):
            embeddings = await asyncio.to_thread(self.embedding_model.embed, new_mem_content, "add")
            new_message_embeddings[new_mem_content] = embeddings
            existing_mems = await asyncio.to_thread(
                self.vector_store.search,
                query=new_mem_content,
                vectors=embeddings,
                limit=5,
                filters=search_filters,
            )
            return [{"id": mem.id, "text": mem.payload.get("data", "")} for mem in existing_mems]

        search_tasks = [process_fact_for_search(fact) for fact in new_retrieved_facts]
        search_results_list = await asyncio.gather(*search_tasks)
        for result_group in search_results_list:
            retrieved_old_memory.extend(result_group)

        unique_data = {}
        for item in retrieved_old_memory:
            unique_data[item["id"]] = item
        retrieved_old_memory = list(unique_data.values())
        logger.info(f"Total existing memories: {len(retrieved_old_memory)}")
        temp_uuid_mapping = {}
        for idx, item in enumerate(retrieved_old_memory):
            temp_uuid_mapping[str(idx)] = item["id"]
            retrieved_old_memory[idx]["id"] = str(idx)

        if new_retrieved_facts:
            function_calling_prompt = get_update_memory_messages(
                retrieved_old_memory, new_retrieved_facts, self.config.custom_update_memory_prompt
            )
            try:
                response = await self._call_llm(
                    messages=[{"role": "user", "content": function_calling_prompt}],
                    response_format={"type": "json_object"},
                )
            except Exception as e:
                logger.error(f"Error in new memory actions response: {e}")
                response = ""
            try:
                if not response or not response.strip():
                    logger.warning("Empty response from LLM, no memories to extract")
                    new_memories_with_actions = {}
                else:
                    response = remove_code_blocks(response)
                    new_memories_with_actions = json.loads(response)
            except Exception as e:
                logger.error(f"Invalid JSON response: {e}")
                new_memories_with_actions = {}
        else:
            new_memories_with_actions = {}

        # Build fact-text → index mapping for per-fact temporal_type lookup
        _fact_text_to_idx = {}
        for _fi, _ft in enumerate(new_retrieved_facts):
            _fact_text_to_idx.setdefault(_ft, _fi)

        # temporal_types parallel array (may be empty)
        _temporal_types = (extraction_meta or {}).get("temporal_types", [])

        returned_memories = []
        try:
            memory_tasks = []
            for resp in new_memories_with_actions.get("memory", []):
                logger.info(resp)
                try:
                    action_text = resp.get("text")
                    if not action_text:
                        continue
                    event_type = resp.get("event")

                    if event_type == "ADD":
                        add_meta = deepcopy(metadata)
                        # Apply per-fact temporal_type from extraction
                        if _temporal_types:
                            _fidx = _fact_text_to_idx.get(action_text)
                            if _fidx is not None and _fidx < len(_temporal_types):
                                add_meta["temporal_type"] = _temporal_types[_fidx]
                            elif "temporal_type" not in add_meta:
                                add_meta["temporal_type"] = "event"
                        task = asyncio.create_task(
                            self._create_memory(
                                data=action_text,
                                existing_embeddings=new_message_embeddings,
                                metadata=add_meta,
                            )
                        )
                        memory_tasks.append((task, resp, "ADD", None))
                    elif event_type == "UPDATE":
                        update_meta = deepcopy(metadata)
                        # Preserve lifecycle metadata from existing memory
                        existing_mem_id = temp_uuid_mapping[resp["id"]]
                        try:
                            existing_mem_obj = await asyncio.to_thread(
                                self.vector_store.get, vector_id=existing_mem_id
                            )
                            existing_payload = existing_mem_obj.payload if existing_mem_obj else {}
                        except Exception:
                            existing_payload = {}
                        update_meta["times_reported"] = existing_payload.get("times_reported", 0)
                        # Reset to active because new info was added
                        update_meta["lifecycle_state"] = "active"
                        task = asyncio.create_task(
                            self._update_memory(
                                memory_id=existing_mem_id,
                                data=action_text,
                                existing_embeddings=new_message_embeddings,
                                metadata=update_meta,
                            )
                        )
                        memory_tasks.append((task, resp, "UPDATE", existing_mem_id))
                    elif event_type == "DELETE":
                        task = asyncio.create_task(self._delete_memory(memory_id=temp_uuid_mapping[resp.get("id")]))
                        memory_tasks.append((task, resp, "DELETE", temp_uuid_mapping[resp.get("id")]))
                    elif event_type == "NONE":
                        memory_id = temp_uuid_mapping.get(resp.get("id"))
                        if memory_id and (metadata.get("agent_id") or metadata.get("run_id")):
                            async def update_session_ids(mem_id, meta):
                                existing_memory = await asyncio.to_thread(self.vector_store.get, vector_id=mem_id)
                                updated_metadata = deepcopy(existing_memory.payload)
                                if meta.get("agent_id"):
                                    updated_metadata["agent_id"] = meta["agent_id"]
                                if meta.get("run_id"):
                                    updated_metadata["run_id"] = meta["run_id"]
                                updated_metadata["updated_at"] = datetime.now(pytz.timezone("US/Pacific")).isoformat()

                                await asyncio.to_thread(
                                    self.vector_store.update,
                                    vector_id=mem_id,
                                    vector=None,
                                    payload=updated_metadata,
                                )
                                logger.info(f"Updated session IDs for memory {mem_id} (async)")

                            task = asyncio.create_task(update_session_ids(memory_id, metadata))
                            memory_tasks.append((task, resp, "NONE", memory_id))
                        else:
                            logger.info("NOOP for Memory (async).")
                except Exception as e:
                    logger.error(f"Error processing memory action (async): {resp}, Error: {e}")

            for task, resp, event_type, mem_id in memory_tasks:
                try:
                    result_id = await task
                    if event_type == "ADD":
                        returned_memories.append({"id": result_id, "memory": resp.get("text"), "event": event_type})
                    elif event_type == "UPDATE":
                        returned_memories.append(
                            {
                                "id": mem_id,
                                "memory": resp.get("text"),
                                "event": event_type,
                                "previous_memory": resp.get("old_memory"),
                            }
                        )
                    elif event_type == "DELETE":
                        returned_memories.append({"id": mem_id, "memory": resp.get("text"), "event": event_type})
                except Exception as e:
                    logger.error(f"Error awaiting memory task (async): {e}")
        except Exception as e:
            logger.error(f"Error in memory processing loop (async): {e}")

        keys, encoded_ids = process_telemetry_filters(effective_filters)
        capture_event(
            "mem0.add",
            self,
            {"version": self.api_version, "keys": keys, "encoded_ids": encoded_ids, "sync_type": "async"},
        )
        return returned_memories

    async def _add_to_graph(self, messages, filters, pre_extracted=None):
        added_entities = []
        if self.enable_graph:
            if filters.get("user_id") is None:
                filters["user_id"] = "user"

            if pre_extracted:
                entities, relationships = pre_extracted
                added_entities = await asyncio.to_thread(
                    self.graph.add_from_extraction, entities, relationships, filters
                )
            else:
                data = "\n".join([msg["content"] for msg in messages if "content" in msg and msg["role"] != "system"])
                added_entities = await asyncio.to_thread(self.graph.add, data, filters)

        return added_entities

    async def get(self, memory_id):
        """Retrieve a memory by ID asynchronously."""
        capture_event("mem0.get", self, {"memory_id": memory_id, "sync_type": "async"})
        memory = await asyncio.to_thread(self.vector_store.get, vector_id=memory_id)
        if not memory:
            return None

        promoted_payload_keys = [
            "user_id",
            "agent_id",
            "run_id",
            "actor_id",
            "role",
        ]

        core_and_promoted_keys = {"data", "hash", "created_at", "updated_at", "id", *promoted_payload_keys}

        result_item = MemoryItem(
            id=memory.id,
            memory=memory.payload.get("data", ""),
            hash=memory.payload.get("hash"),
            created_at=memory.payload.get("created_at"),
            updated_at=memory.payload.get("updated_at"),
        ).model_dump()

        for key in promoted_payload_keys:
            if key in memory.payload:
                result_item[key] = memory.payload[key]

        additional_metadata = {k: v for k, v in memory.payload.items() if k not in core_and_promoted_keys}
        if additional_metadata:
            result_item["metadata"] = additional_metadata

        return result_item

    async def get_all(
        self,
        *,
        user_id: Optional[str] = None,
        agent_id: Optional[str] = None,
        run_id: Optional[str] = None,
        filters: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = 100,
    ):
        """List all memories asynchronously.

        Pass limit=None to retrieve ALL matching records (paginated scroll).
        """

        _, effective_filters = _build_filters_and_metadata(
            user_id=user_id, agent_id=agent_id, run_id=run_id, input_filters=filters
        )

        if not any(key in effective_filters for key in ("user_id", "agent_id", "run_id")):
            raise ValueError(
                "When 'conversation_id' is not provided (classic mode), "
                "at least one of 'user_id', 'agent_id', or 'run_id' must be specified for get_all."
            )

        # Merge custom filters (e.g. timestamp range) into effective_filters
        if filters:
            if self._has_advanced_operators(filters):
                processed_filters = self._process_metadata_filters(filters)
                effective_filters.update(processed_filters)
            else:
                effective_filters.update(filters)

        keys, encoded_ids = process_telemetry_filters(effective_filters)
        capture_event(
            "mem0.get_all", self, {"limit": limit, "keys": keys, "encoded_ids": encoded_ids, "sync_type": "async"}
        )

        vector_store_task = asyncio.create_task(self._get_all_from_vector_store(effective_filters, limit))

        graph_task = None
        if self.enable_graph:
            graph_get_all = getattr(self.graph, "get_all", None)
            if callable(graph_get_all):
                if asyncio.iscoroutinefunction(graph_get_all):
                    graph_task = asyncio.create_task(graph_get_all(effective_filters, limit))
                else:
                    graph_task = asyncio.create_task(asyncio.to_thread(graph_get_all, effective_filters, limit))

        results_dict = {}
        if graph_task:
            vector_store_result, graph_entities_result = await asyncio.gather(vector_store_task, graph_task)
            results_dict.update({"results": vector_store_result, "relations": graph_entities_result})
        else:
            results_dict.update({"results": await vector_store_task})

        return results_dict

    async def _get_all_from_vector_store(self, filters, limit):
        if limit is None and hasattr(self.vector_store, "scroll_all"):
            memories_result = await asyncio.to_thread(self.vector_store.scroll_all, filters=filters)
        else:
            memories_result = await asyncio.to_thread(self.vector_store.list, filters=filters, limit=limit or 100)

        if isinstance(memories_result, (tuple, list)) and len(memories_result) > 0:
            first_element = memories_result[0]
            if isinstance(first_element, (list, tuple)):
                actual_memories = first_element
            else:
                actual_memories = memories_result
        else:
            actual_memories = memories_result

        promoted_payload_keys = [
            "user_id",
            "agent_id",
            "run_id",
            "actor_id",
            "role",
        ]
        core_and_promoted_keys = {"data", "hash", "created_at", "updated_at", "id", *promoted_payload_keys}

        formatted_memories = []
        for mem in actual_memories:
            memory_item_dict = MemoryItem(
                id=mem.id,
                memory=mem.payload.get("data", ""),
                hash=mem.payload.get("hash"),
                created_at=mem.payload.get("created_at"),
                updated_at=mem.payload.get("updated_at"),
            ).model_dump(exclude={"score"})

            for key in promoted_payload_keys:
                if key in mem.payload:
                    memory_item_dict[key] = mem.payload[key]

            additional_metadata = {k: v for k, v in mem.payload.items() if k not in core_and_promoted_keys}
            if additional_metadata:
                memory_item_dict["metadata"] = additional_metadata

            formatted_memories.append(memory_item_dict)

        return formatted_memories

    async def search(
        self,
        query: str,
        *,
        user_id: Optional[str] = None,
        agent_id: Optional[str] = None,
        run_id: Optional[str] = None,
        limit: int = 100,
        filters: Optional[Dict[str, Any]] = None,
        threshold: Optional[float] = None,
        metadata_filters: Optional[Dict[str, Any]] = None,
        rerank: bool = True,
    ):
        """Searches for memories based on a query asynchronously."""

        _, effective_filters = _build_filters_and_metadata(
            user_id=user_id, agent_id=agent_id, run_id=run_id, input_filters=filters
        )

        if not any(key in effective_filters for key in ("user_id", "agent_id", "run_id")):
            raise ValueError("at least one of 'user_id', 'agent_id', or 'run_id' must be specified ")

        if filters and self._has_advanced_operators(filters):
            processed_filters = self._process_metadata_filters(filters)
            effective_filters.update(processed_filters)
        elif filters:
            effective_filters.update(filters)

        keys, encoded_ids = process_telemetry_filters(effective_filters)
        capture_event(
            "mem0.search",
            self,
            {
                "limit": limit,
                "version": self.api_version,
                "keys": keys,
                "encoded_ids": encoded_ids,
                "sync_type": "async",
                "threshold": threshold,
                "advanced_filters": bool(filters and self._has_advanced_operators(filters)),
            },
        )

        vector_store_task = asyncio.create_task(self._search_vector_store(query, effective_filters, limit, threshold))

        graph_task = None
        if self.enable_graph:
            if hasattr(self.graph.search, "__await__"):
                graph_task = asyncio.create_task(self.graph.search(query, effective_filters, limit))
            else:
                graph_task = asyncio.create_task(asyncio.to_thread(self.graph.search, query, effective_filters, limit))

        if graph_task:
            original_memories, graph_entities = await asyncio.gather(vector_store_task, graph_task)
        else:
            original_memories = await vector_store_task
            graph_entities = None

        if rerank and self.reranker and original_memories:
            try:
                reranked_memories = await asyncio.to_thread(
                    self.reranker.rerank, query, original_memories, limit
                )
                original_memories = reranked_memories
            except Exception as e:
                logger.warning(f"Reranking failed, using original results: {e}")

        if self.enable_graph:
            return {"results": original_memories, "relations": graph_entities}

        return {"results": original_memories}

    def _process_metadata_filters(self, metadata_filters: Dict[str, Any]) -> Dict[str, Any]:
        """Process enhanced metadata filters and convert them to vector store compatible format."""
        processed_filters = {}

        def process_condition(key: str, condition: Any) -> Dict[str, Any]:
            if not isinstance(condition, dict):
                if condition == "*":
                    return {key: "*"}
                return {key: condition}

            result = {}
            for operator, value in condition.items():
                operator_map = {
                    "eq": "eq", "ne": "ne", "gt": "gt", "gte": "gte",
                    "lt": "lt", "lte": "lte", "in": "in", "nin": "nin",
                    "contains": "contains", "icontains": "icontains"
                }

                if operator in operator_map:
                    result[key] = {operator_map[operator]: value}
                else:
                    raise ValueError(f"Unsupported metadata filter operator: {operator}")
            return result

        for key, value in metadata_filters.items():
            if key == "AND":
                if not isinstance(value, list):
                    raise ValueError("AND operator requires a list of conditions")
                for condition in value:
                    for sub_key, sub_value in condition.items():
                        processed_filters.update(process_condition(sub_key, sub_value))
            elif key == "OR":
                if not isinstance(value, list) or not value:
                    raise ValueError("OR operator requires a non-empty list of conditions")
                processed_filters["$or"] = []
                for condition in value:
                    or_condition = {}
                    for sub_key, sub_value in condition.items():
                        or_condition.update(process_condition(sub_key, sub_value))
                    processed_filters["$or"].append(or_condition)
            elif key == "NOT":
                if not isinstance(value, list) or not value:
                    raise ValueError("NOT operator requires a non-empty list of conditions")
                processed_filters["$not"] = []
                for condition in value:
                    not_condition = {}
                    for sub_key, sub_value in condition.items():
                        not_condition.update(process_condition(sub_key, sub_value))
                    processed_filters["$not"].append(not_condition)
            else:
                processed_filters.update(process_condition(key, value))

        return processed_filters

    def _has_advanced_operators(self, filters: Dict[str, Any]) -> bool:
        """Check if filters contain advanced operators that need special processing."""
        if not isinstance(filters, dict):
            return False

        for key, value in filters.items():
            if key in ["AND", "OR", "NOT"]:
                return True
            if isinstance(value, dict):
                for op in value.keys():
                    if op in ["eq", "ne", "gt", "gte", "lt", "lte", "in", "nin", "contains", "icontains"]:
                        return True
            if value == "*":
                return True
        return False

    async def _search_vector_store(self, query, filters, limit, threshold: Optional[float] = None):
        embeddings = await asyncio.to_thread(self.embedding_model.embed, query, "search")
        memories = await asyncio.to_thread(
            self.vector_store.search, query=query, vectors=embeddings, limit=limit, filters=filters
        )

        promoted_payload_keys = [
            "user_id",
            "agent_id",
            "run_id",
            "actor_id",
            "role",
        ]

        core_and_promoted_keys = {"data", "hash", "created_at", "updated_at", "id", *promoted_payload_keys}

        original_memories = []
        for mem in memories:
            memory_item_dict = MemoryItem(
                id=mem.id,
                memory=mem.payload.get("data", ""),
                hash=mem.payload.get("hash"),
                created_at=mem.payload.get("created_at"),
                updated_at=mem.payload.get("updated_at"),
                score=mem.score,
            ).model_dump()

            for key in promoted_payload_keys:
                if key in mem.payload:
                    memory_item_dict[key] = mem.payload[key]

            additional_metadata = {k: v for k, v in mem.payload.items() if k not in core_and_promoted_keys}
            if additional_metadata:
                memory_item_dict["metadata"] = additional_metadata

            if threshold is None or mem.score >= threshold:
                original_memories.append(memory_item_dict)

        return original_memories

    async def update(self, memory_id, data):
        """Update a memory by ID asynchronously."""
        capture_event("mem0.update", self, {"memory_id": memory_id, "sync_type": "async"})
        embeddings = await asyncio.to_thread(self.embedding_model.embed, data, "update")
        existing_embeddings = {data: embeddings}
        await self._update_memory(memory_id, data, existing_embeddings)
        return {"message": "Memory updated successfully!"}

    async def delete(self, memory_id):
        """Delete a memory by ID asynchronously."""
        capture_event("mem0.delete", self, {"memory_id": memory_id, "sync_type": "async"})
        await self._delete_memory(memory_id)
        return {"message": "Memory deleted successfully!"}

    async def delete_all(self, user_id=None, agent_id=None, run_id=None):
        """Delete all memories asynchronously."""
        filters = {}
        if user_id:
            filters["user_id"] = user_id
        if agent_id:
            filters["agent_id"] = agent_id
        if run_id:
            filters["run_id"] = run_id

        if not filters:
            raise ValueError(
                "At least one filter is required to delete all memories. If you want to delete all memories, use the `reset()` method."
            )

        keys, encoded_ids = process_telemetry_filters(filters)
        capture_event("mem0.delete_all", self, {"keys": keys, "encoded_ids": encoded_ids, "sync_type": "async"})
        memories = await asyncio.to_thread(self.vector_store.list, filters=filters)

        delete_tasks = []
        for memory in memories[0]:
            delete_tasks.append(self._delete_memory(memory.id))

        await asyncio.gather(*delete_tasks)

        logger.info(f"Deleted {len(memories[0])} memories")

        if self.enable_graph:
            await asyncio.to_thread(self.graph.delete_all, filters)

        return {"message": "Memories deleted successfully!"}

    async def history(self, memory_id):
        """Get the history of changes for a memory by ID asynchronously."""
        capture_event("mem0.history", self, {"memory_id": memory_id, "sync_type": "async"})
        return await asyncio.to_thread(self.db.get_history, memory_id)

    async def _create_memory(self, data, existing_embeddings, metadata=None):
        logger.debug(f"Creating memory with {data=}")
        if data in existing_embeddings:
            embeddings = existing_embeddings[data]
        else:
            embeddings = await asyncio.to_thread(self.embedding_model.embed, data, memory_action="add")

        memory_id = str(uuid.uuid4())
        metadata = metadata or {}
        metadata["data"] = data
        metadata["hash"] = hashlib.md5(data.encode()).hexdigest()
        metadata["created_at"] = datetime.now(pytz.timezone("US/Pacific")).isoformat()

        # Lifecycle metadata defaults
        metadata["lifecycle_state"] = metadata.get("lifecycle_state", "active")
        metadata["times_reported"] = metadata.get("times_reported", 0)
        metadata["reported_at"] = metadata.get("reported_at", 0)

        await asyncio.to_thread(
            self.vector_store.insert,
            vectors=[embeddings],
            ids=[memory_id],
            payloads=[metadata],
        )

        await asyncio.to_thread(
            self.db.add_history,
            memory_id,
            None,
            data,
            "ADD",
            created_at=metadata.get("created_at"),
            actor_id=metadata.get("actor_id"),
            role=metadata.get("role"),
        )

        return memory_id

    async def _create_procedural_memory(self, messages, metadata=None, llm=None, prompt=None):
        """Create a procedural memory asynchronously."""
        try:
            from langchain_core.messages.utils import (
                convert_to_messages,
            )
        except Exception:
            logger.error(
                "Import error while loading langchain-core. Please install 'langchain-core' to use procedural memory."
            )
            raise

        logger.info("Creating procedural memory")

        parsed_messages = [
            {"role": "system", "content": prompt or PROCEDURAL_MEMORY_SYSTEM_PROMPT},
            *messages,
            {"role": "user", "content": "Create procedural memory of the above conversation."},
        ]

        try:
            if llm is not None:
                parsed_messages = convert_to_messages(parsed_messages)
                response = await asyncio.to_thread(llm.invoke, input=parsed_messages)
                procedural_memory = response.content
            else:
                procedural_memory = await self._call_llm(messages=parsed_messages)
                procedural_memory = remove_code_blocks(procedural_memory)
        
        except Exception as e:
            logger.error(f"Error generating procedural memory summary: {e}")
            raise

        if metadata is None:
            raise ValueError("Metadata cannot be done for procedural memory.")

        metadata["memory_type"] = MemoryType.PROCEDURAL.value
        embeddings = await asyncio.to_thread(self.embedding_model.embed, procedural_memory, memory_action="add")
        memory_id = await self._create_memory(procedural_memory, {procedural_memory: embeddings}, metadata=metadata)
        capture_event("mem0._create_procedural_memory", self, {"memory_id": memory_id, "sync_type": "async"})

        result = {"results": [{"id": memory_id, "memory": procedural_memory, "event": "ADD"}]}

        return result

    async def _update_memory(self, memory_id, data, existing_embeddings, metadata=None):
        logger.info(f"Updating memory with {data=}")

        try:
            existing_memory = await asyncio.to_thread(self.vector_store.get, vector_id=memory_id)
        except Exception:
            logger.error(f"Error getting memory with ID {memory_id} during update.")
            raise ValueError(f"Error getting memory with ID {memory_id}. Please provide a valid 'memory_id'")

        prev_value = existing_memory.payload.get("data")

        new_metadata = deepcopy(metadata) if metadata is not None else {}

        new_metadata["data"] = data
        new_metadata["hash"] = hashlib.md5(data.encode()).hexdigest()
        new_metadata["created_at"] = existing_memory.payload.get("created_at")
        new_metadata["updated_at"] = datetime.now(pytz.timezone("US/Pacific")).isoformat()

        if "user_id" not in new_metadata and "user_id" in existing_memory.payload:
            new_metadata["user_id"] = existing_memory.payload["user_id"]
        if "agent_id" not in new_metadata and "agent_id" in existing_memory.payload:
            new_metadata["agent_id"] = existing_memory.payload["agent_id"]
        if "run_id" not in new_metadata and "run_id" in existing_memory.payload:
            new_metadata["run_id"] = existing_memory.payload["run_id"]

        if "actor_id" not in new_metadata and "actor_id" in existing_memory.payload:
            new_metadata["actor_id"] = existing_memory.payload["actor_id"]
        if "role" not in new_metadata and "role" in existing_memory.payload:
            new_metadata["role"] = existing_memory.payload["role"]

        if data in existing_embeddings:
            embeddings = existing_embeddings[data]
        else:
            embeddings = await asyncio.to_thread(self.embedding_model.embed, data, "update")

        await asyncio.to_thread(
            self.vector_store.update,
            vector_id=memory_id,
            vector=embeddings,
            payload=new_metadata,
        )
        logger.info(f"Updating memory with ID {memory_id=} with {data=}")

        await asyncio.to_thread(
            self.db.add_history,
            memory_id,
            prev_value,
            data,
            "UPDATE",
            created_at=new_metadata["created_at"],
            updated_at=new_metadata["updated_at"],
            actor_id=new_metadata.get("actor_id"),
            role=new_metadata.get("role"),
        )
        return memory_id

    async def _delete_memory(self, memory_id):
        logger.info(f"Deleting memory with {memory_id=}")
        existing_memory = await asyncio.to_thread(self.vector_store.get, vector_id=memory_id)
        prev_value = existing_memory.payload.get("data", "")

        await asyncio.to_thread(self.vector_store.delete, vector_id=memory_id)
        await asyncio.to_thread(
            self.db.add_history,
            memory_id,
            prev_value,
            None,
            "DELETE",
            actor_id=existing_memory.payload.get("actor_id"),
            role=existing_memory.payload.get("role"),
            is_deleted=1,
        )

        return memory_id

    async def reset(self):
        """
        Reset the memory store asynchronously by:
            Deletes the vector store collection
            Resets the database
            Clears the graph store
            Recreates the vector store with a new client
        """
        logger.warning("Resetting all memories")
        await asyncio.to_thread(self.vector_store.delete_col)

        gc.collect()

        if hasattr(self.vector_store, "client") and hasattr(self.vector_store.client, "close"):
            await asyncio.to_thread(self.vector_store.client.close)

        if hasattr(self.db, "connection") and self.db.connection:
            await asyncio.to_thread(lambda: self.db.connection.execute("DROP TABLE IF EXISTS history"))
            await asyncio.to_thread(self.db.connection.close)

        self.db = SQLiteManager(self.config.history_db_path)

        self.vector_store = VectorStoreFactory.create(
            self.config.vector_store.provider, self.config.vector_store.config
        )

        if hasattr(self, "graph") and self.graph is not None:
            try:
                await asyncio.to_thread(self.graph.reset)
            except Exception as e:
                logger.warning(f"Failed to reset graph: {e}")

        capture_event("mem0.reset", self, {"sync_type": "async"})

    async def mark_as_reported(self, memory_ids: list[str], cycle_id: str) -> int:
        """Mark facts as reported after they appear in an analyst report.

        Updates lifecycle_state to 'reported', increments times_reported,
        sets reported_at to current timestamp.

        Returns number of updated memories.
        """
        updated = 0
        now_ts = int(time.time())

        for memory_id in memory_ids:
            try:
                existing = await asyncio.to_thread(self.vector_store.get, vector_id=memory_id)
                if not existing:
                    logger.warning(f"mark_as_reported: memory {memory_id} not found, skipping")
                    continue

                current_times_reported = existing.payload.get("times_reported", 0)

                payload_update = {
                    "lifecycle_state": "reported",
                    "times_reported": current_times_reported + 1,
                    "reported_at": now_ts,
                    "last_reported_cycle": cycle_id,
                }

                await asyncio.to_thread(
                    self.vector_store.update_payload, memory_id, payload_update
                )
                updated += 1
            except Exception as e:
                logger.error(f"mark_as_reported: failed to update {memory_id}: {e}")

        logger.info(f"mark_as_reported: updated {updated}/{len(memory_ids)} memories for cycle {cycle_id}")
        return updated

    async def get_by_lifecycle(
        self,
        *,
        user_id: str,
        state: "str | list[str]",
        agent_id: str = None,
        limit: int = 500,
        time_filter: dict = None,
    ) -> list:
        """Get memories filtered by lifecycle state(s).

        Args:
            user_id: User ID to scope the query.
            state: Single state or list of states
                   ("active", "reported", "background", "archived").
            agent_id: Optional agent ID for additional scoping.
            limit: Maximum number of records to return. Defaults to 500.
            time_filter: Optional dict with 'gte' and/or 'lte' timestamps
                         to filter by the 'timestamp' payload field.

        Returns:
            list: List of formatted memory dicts.
        """
        _, effective_filters = _build_filters_and_metadata(
            user_id=user_id, agent_id=agent_id,
        )

        # Add lifecycle_state filter (list → MatchAny, str → MatchValue)
        if isinstance(state, list):
            effective_filters["lifecycle_state"] = state
        else:
            effective_filters["lifecycle_state"] = state

        # Add optional timestamp range filter
        if time_filter:
            ts_range = {}
            if "gte" in time_filter:
                ts_range["gte"] = time_filter["gte"]
            if "lte" in time_filter:
                ts_range["lte"] = time_filter["lte"]
            if ts_range:
                effective_filters["timestamp"] = ts_range

        points = await asyncio.to_thread(
            self.vector_store.scroll_with_filter, effective_filters, limit
        )

        promoted_payload_keys = ["user_id", "agent_id", "run_id", "actor_id", "role"]
        core_and_promoted_keys = {"data", "hash", "created_at", "updated_at", "id", *promoted_payload_keys}

        results = []
        for mem in points:
            memory_item_dict = MemoryItem(
                id=mem.id,
                memory=mem.payload.get("data", ""),
                hash=mem.payload.get("hash"),
                created_at=mem.payload.get("created_at"),
                updated_at=mem.payload.get("updated_at"),
            ).model_dump(exclude={"score"})

            for key in promoted_payload_keys:
                if key in mem.payload:
                    memory_item_dict[key] = mem.payload[key]

            additional_metadata = {k: v for k, v in mem.payload.items() if k not in core_and_promoted_keys}
            if additional_metadata:
                memory_item_dict["metadata"] = additional_metadata

            results.append(memory_item_dict)

        return results

    async def chat(self, query):
        raise NotImplementedError("Chat function not implemented yet.")