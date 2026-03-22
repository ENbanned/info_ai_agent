import re
import logging

from mem0.memory.utils import format_entities, sanitize_relationship_for_cypher

try:
    from langchain_neo4j import Neo4jGraph
except ImportError:
    raise ImportError("langchain_neo4j is not installed. Please install it using pip install langchain-neo4j")

try:
    from rank_bm25 import BM25Okapi
except ImportError:
    raise ImportError("rank_bm25 is not installed. Please install it using pip install rank-bm25")

from mem0.graphs.tools import (
    DELETE_MEMORY_STRUCT_TOOL_GRAPH,
    DELETE_MEMORY_TOOL_GRAPH,
    EXTRACT_ENTITIES_STRUCT_TOOL,
    EXTRACT_ENTITIES_TOOL,
    RELATIONS_STRUCT_TOOL,
    RELATIONS_TOOL,
)
from mem0.graphs.utils import EXTRACT_RELATIONS_PROMPT, get_delete_messages
from mem0.utils.factory import EmbedderFactory, LlmFactory

logger = logging.getLogger(__name__)


class MemoryGraph:
    def __init__(self, config):
        self.config = config
        self.graph = Neo4jGraph(
            self.config.graph_store.config.url,
            self.config.graph_store.config.username,
            self.config.graph_store.config.password,
            self.config.graph_store.config.database,
            refresh_schema=False,
            driver_config={"notifications_min_severity": "OFF"},
        )
        self.embedding_model = EmbedderFactory.create(
            self.config.embedder.provider, self.config.embedder.config, self.config.vector_store.config
        )
        self.node_label = ":`__Entity__`" if self.config.graph_store.config.base_label else ""

        if self.config.graph_store.config.base_label:
            try:
                self.graph.query(f"CREATE INDEX entity_single IF NOT EXISTS FOR (n {self.node_label}) ON (n.user_id)")
            except Exception:
                pass
            try:
                self.graph.query(
                    f"CREATE INDEX entity_composite IF NOT EXISTS FOR (n {self.node_label}) ON (n.name, n.user_id)"
                )
            except Exception:
                pass

        # Ensure uniqueness constraint on entity_key (prevents race-condition duplicates)
        self._ensure_constraints()

        self.llm_provider = "openai"
        if self.config.llm and self.config.llm.provider:
            self.llm_provider = self.config.llm.provider
        if self.config.graph_store and self.config.graph_store.llm and self.config.graph_store.llm.provider:
            self.llm_provider = self.config.graph_store.llm.provider

        llm_config = None
        if self.config.graph_store and self.config.graph_store.llm and hasattr(self.config.graph_store.llm, "config"):
            llm_config = self.config.graph_store.llm.config
        elif hasattr(self.config.llm, "config"):
            llm_config = self.config.llm.config
        self.llm = LlmFactory.create(self.llm_provider, llm_config)
        self.user_id = None
        self.threshold = self.config.graph_store.threshold if hasattr(self.config.graph_store, 'threshold') else 0.85

        self.entity_validator = None
        try:
            from mem0.memory.validator import EntityValidator
            self.entity_validator = EntityValidator()
        except ImportError:
            pass

    def _ensure_constraints(self):
        """Create uniqueness constraint on entity_key (idempotent).

        entity_key = user_id + "::" + name — a computed property that gives MERGE
        a lock target, preventing race-condition duplicates (Neo4j issue #12674).
        Community Edition supports single-property uniqueness constraints.
        """
        if not self.config.graph_store.config.base_label:
            return
        try:
            self.graph.query(
                f"CREATE CONSTRAINT entity_unique_key IF NOT EXISTS "
                f"FOR (n {self.node_label}) REQUIRE n.entity_key IS UNIQUE"
            )
            logger.info("Ensured uniqueness constraint on entity_key")
        except Exception as e:
            logger.warning(f"Could not create entity_key uniqueness constraint: {e}")

    def add(self, data, filters):
        """Adds data to the graph (LLM-based extraction)."""
        entity_type_map = self._retrieve_nodes_from_data(data, filters)
        to_be_added = self._establish_nodes_relations_from_data(data, filters, entity_type_map)

        if self.entity_validator and to_be_added:
            to_be_added = self.entity_validator.filter_triples(to_be_added)

        search_output = self._search_graph_db(node_list=list(entity_type_map.keys()), filters=filters)
        to_be_deleted = self._get_delete_entities_from_search_output(search_output, data, filters)

        deleted_entities = self._delete_entities(to_be_deleted, filters)
        added_entities = self._add_entities(to_be_added, filters, entity_type_map)

        return {"deleted_entities": deleted_entities, "added_entities": added_entities}

    def add_from_extraction(self, entities, relationships, filters):
        """Add pre-extracted entities and relationships to graph. No LLM calls."""
        entity_type_map = {e["name"].lower().replace(" ", "_"): e["type"].lower().replace(" ", "_") for e in entities}
        to_be_added = [{"source": r["source"], "relationship": r["relationship"], "destination": r["destination"]} for r in relationships]
        to_be_added = self._remove_spaces_from_entities(to_be_added)

        # Filter out relations where user_id leaked as an entity
        user_id_normalized = filters.get("user_id", "").lower().replace(" ", "_")
        if user_id_normalized:
            def _contains_user_id(val):
                return val == user_id_normalized or user_id_normalized in val
            to_be_added = [e for e in to_be_added if not _contains_user_id(e["source"]) and not _contains_user_id(e["destination"])]

        if self.entity_validator and to_be_added:
            to_be_added = self.entity_validator.filter_triples(to_be_added)

        added = self._add_entities(to_be_added, filters, entity_type_map)
        return {"added_entities": added}

    def search(self, query, filters, limit=100):
        """Search for memories and related graph data."""
        entity_type_map = self._retrieve_nodes_from_data(query, filters)
        search_output = self._search_graph_db(node_list=list(entity_type_map.keys()), filters=filters)

        if not search_output:
            return []

        search_outputs_sequence = [
            [item["source"], item["relationship"], item["destination"]] for item in search_output
        ]
        bm25 = BM25Okapi(search_outputs_sequence)

        tokenized_query = query.split(" ")
        reranked_results = bm25.get_top_n(tokenized_query, search_outputs_sequence, n=10)

        # Deduplicate search results
        seen = set()
        search_results = []
        for item in reranked_results:
            key = (item[0], item[1], item[2])
            if key not in seen:
                seen.add(key)
                search_results.append({"source": item[0], "relationship": item[1], "destination": item[2]})

        logger.info(f"Returned {len(search_results)} search results")
        return search_results

    def delete_all(self, filters):
        node_props = ["user_id: $user_id"]
        if filters.get("agent_id"):
            node_props.append("agent_id: $agent_id")
        if filters.get("run_id"):
            node_props.append("run_id: $run_id")
        node_props_str = ", ".join(node_props)

        cypher = f"""
        MATCH (n {self.node_label} {{{node_props_str}}})
        DETACH DELETE n
        """
        params = {"user_id": filters["user_id"]}
        if filters.get("agent_id"):
            params["agent_id"] = filters["agent_id"]
        if filters.get("run_id"):
            params["run_id"] = filters["run_id"]
        self.graph.query(cypher, params=params)

    def get_all(self, filters, limit=1000, sort_by="mentions DESC", offset=0):
        """Retrieves nodes and relationships from the graph database with pagination.

        Args:
            filters: dict with user_id (required), agent_id, run_id (optional).
            limit: Max relationships to return. Default 1000. Pass None for all (backward compat).
            sort_by: Sort order for results. Default "mentions DESC" (most mentioned first).
                     Supported: "mentions DESC", "mentions ASC", "created_at DESC", "created_at ASC".
            offset: Number of results to skip (for pagination). Default 0.

        Returns:
            List of dicts with source, relationship, target keys.
        """
        params = {"user_id": filters["user_id"]}

        node_props = ["user_id: $user_id"]
        if filters.get("agent_id"):
            node_props.append("agent_id: $agent_id")
            params["agent_id"] = filters["agent_id"]
        if filters.get("run_id"):
            node_props.append("run_id: $run_id")
            params["run_id"] = filters["run_id"]
        node_props_str = ", ".join(node_props)

        # Build ORDER BY clause
        order_clause = ""
        sort_mapping = {
            "mentions DESC": "ORDER BY coalesce(r.mentions, 0) DESC",
            "mentions ASC": "ORDER BY coalesce(r.mentions, 0) ASC",
            "created_at DESC": "ORDER BY coalesce(r.created_at, 0) DESC",
            "created_at ASC": "ORDER BY coalesce(r.created_at, 0) ASC",
        }
        if sort_by and sort_by in sort_mapping:
            order_clause = sort_mapping[sort_by]

        # Build SKIP/LIMIT clause
        skip_clause = ""
        if offset and offset > 0:
            skip_clause = "SKIP $offset"
            params["offset"] = offset

        limit_clause = ""
        if limit is not None:
            limit_clause = "LIMIT $limit"
            params["limit"] = limit

        query = f"""
        MATCH (n {self.node_label} {{{node_props_str}}})-[r]->(m {self.node_label} {{{node_props_str}}})
        RETURN n.name AS source, type(r) AS relationship, m.name AS target,
               coalesce(r.mentions, 0) AS rel_mentions
        {order_clause}
        {skip_clause}
        {limit_clause}
        """
        results = self.graph.query(query, params=params)

        final_results = []
        for result in results:
            final_results.append(
                {
                    "source": result["source"],
                    "relationship": result["relationship"],
                    "target": result["target"],
                }
            )

        logger.info(f"Retrieved {len(final_results)} relationships (limit={limit}, offset={offset})")
        return final_results

    def get_entity_relationships(self, entity_name, filters, limit=50):
        """Get relationships for a specific entity using direct Cypher query.

        This is O(1) by index instead of O(N) full scan via get_all() + filter.
        Designed to replace the pattern in memory_tools.py:query_entity.

        Args:
            entity_name: Entity name to look up (case-insensitive, partial match).
            filters: dict with user_id (required), agent_id, run_id (optional).
            limit: Max relationships to return. Default 50.

        Returns:
            List of dicts with source, relationship, target, source_type, target_type keys.
        """
        entity_name = entity_name.lower().replace(" ", "_")
        params = {
            "name": entity_name,
            "user_id": filters["user_id"],
            "limit": limit,
        }

        node_filter_parts = ["n.user_id = $user_id"]
        other_filter_parts = ["m.user_id = $user_id"]
        if filters.get("agent_id"):
            node_filter_parts.append("n.agent_id = $agent_id")
            other_filter_parts.append("m.agent_id = $agent_id")
            params["agent_id"] = filters["agent_id"]
        if filters.get("run_id"):
            node_filter_parts.append("n.run_id = $run_id")
            other_filter_parts.append("m.run_id = $run_id")
            params["run_id"] = filters["run_id"]

        node_filter = " AND ".join(node_filter_parts)
        other_filter = " AND ".join(other_filter_parts)

        cypher = f"""
        MATCH (n {self.node_label})-[r]-(m {self.node_label})
        WHERE toLower(n.name) CONTAINS toLower($name)
          AND {node_filter}
          AND {other_filter}
        WITH n, r, m,
             CASE WHEN startNode(r) = n THEN n.name ELSE m.name END AS source,
             CASE WHEN startNode(r) = n THEN m.name ELSE n.name END AS target,
             CASE WHEN startNode(r) = n THEN coalesce(n.entity_type, '') ELSE coalesce(m.entity_type, '') END AS source_type,
             CASE WHEN startNode(r) = n THEN coalesce(m.entity_type, '') ELSE coalesce(n.entity_type, '') END AS target_type
        RETURN DISTINCT source, type(r) AS relationship, target, source_type, target_type,
               coalesce(r.mentions, 0) AS rel_mentions
        ORDER BY rel_mentions DESC
        LIMIT $limit
        """
        results = self.graph.query(cypher, params=params)

        final_results = []
        for result in results:
            final_results.append(
                {
                    "source": result["source"],
                    "relationship": result["relationship"],
                    "target": result["target"],
                    "source_type": result.get("source_type", ""),
                    "target_type": result.get("target_type", ""),
                }
            )

        logger.info(f"get_entity_relationships('{entity_name}'): {len(final_results)} results")
        return final_results

    def get_top_entities(self, filters, limit=30, min_mentions=2):
        """Get top entities by mention count with their relationships.

        Args:
            filters: dict with user_id (required), agent_id, run_id (optional).
            limit: Max entities to return. Default 30.
            min_mentions: Minimum mention count to include. Default 2.

        Returns:
            List of dicts with entity info and their top relationships.
        """
        params = {
            "user_id": filters["user_id"],
            "limit": limit,
            "min_mentions": min_mentions,
        }

        node_filter_parts = ["n.user_id = $user_id"]
        if filters.get("agent_id"):
            node_filter_parts.append("n.agent_id = $agent_id")
            params["agent_id"] = filters["agent_id"]
        if filters.get("run_id"):
            node_filter_parts.append("n.run_id = $run_id")
            params["run_id"] = filters["run_id"]
        node_filter = " AND ".join(node_filter_parts)

        # Step 1: Get top entities by mentions
        entities_cypher = f"""
        MATCH (n {self.node_label})
        WHERE {node_filter}
          AND coalesce(n.mentions, 0) >= $min_mentions
        RETURN n.name AS name, coalesce(n.entity_type, '') AS entity_type,
               coalesce(n.mentions, 0) AS mentions,
               coalesce(n.lifecycle_state, 'active') AS lifecycle_state,
               n.created AS created, n.last_active_at AS last_active_at
        ORDER BY mentions DESC
        LIMIT $limit
        """
        top_entities = self.graph.query(entities_cypher, params=params)

        # Step 2: For each entity, get top relationships (batch-friendly)
        results = []
        for entity in top_entities:
            entity_name = entity["name"]

            rel_filter_parts = ["m.user_id = $user_id"]
            if filters.get("agent_id"):
                rel_filter_parts.append("m.agent_id = $agent_id")
            if filters.get("run_id"):
                rel_filter_parts.append("m.run_id = $run_id")
            rel_filter = " AND ".join(rel_filter_parts)

            rels_cypher = f"""
            MATCH (n {self.node_label} {{name: $entity_name, user_id: $user_id}})-[r]-(m {self.node_label})
            WHERE {rel_filter}
            RETURN
                CASE WHEN startNode(r) = n THEN n.name ELSE m.name END AS source,
                type(r) AS relationship,
                CASE WHEN startNode(r) = n THEN m.name ELSE n.name END AS target
            ORDER BY coalesce(r.mentions, 0) DESC
            LIMIT 10
            """
            rels_params = {
                "entity_name": entity_name,
                "user_id": filters["user_id"],
            }
            if filters.get("agent_id"):
                rels_params["agent_id"] = filters["agent_id"]
            if filters.get("run_id"):
                rels_params["run_id"] = filters["run_id"]

            relationships = self.graph.query(rels_cypher, params=rels_params)

            results.append({
                "name": entity_name,
                "entity_type": entity.get("entity_type", ""),
                "mentions": entity.get("mentions", 0),
                "lifecycle_state": entity.get("lifecycle_state", "active"),
                "created": entity.get("created"),
                "last_active_at": entity.get("last_active_at"),
                "relationships": [
                    {
                        "source": r["source"],
                        "relationship": r["relationship"],
                        "target": r["target"],
                    }
                    for r in relationships
                ],
            })

        logger.info(f"get_top_entities: {len(results)} entities (min_mentions={min_mentions})")
        return results

    def update_entity_lifecycle(self, filters, transitions=None):
        """Batch update lifecycle states for entities based on activity.

        Implements the lifecycle state machine:
            ACTIVE -> DECLINING (not mentioned in 7+ days, < 5 mentions in 30d)
            DECLINING -> DORMANT (not mentioned in 30+ days)
            Any state -> ACTIVE (re-mentioned via _add_entities ON MATCH)

        Args:
            filters: dict with user_id (required).
            transitions: Optional dict to customize thresholds. Keys:
                - active_to_declining_days (default 7)
                - declining_to_dormant_days (default 30)
                - min_recent_mentions (default 5)

        Returns:
            dict with counts of transitioned entities.
        """
        if transitions is None:
            transitions = {}

        active_to_declining_days = transitions.get("active_to_declining_days", 7)
        declining_to_dormant_days = transitions.get("declining_to_dormant_days", 30)
        min_recent_mentions = transitions.get("min_recent_mentions", 5)

        params = {"user_id": filters["user_id"]}
        ms_per_day = 24 * 3600 * 1000

        # ACTIVE -> DECLINING
        cypher_declining = f"""
        MATCH (n {self.node_label} {{user_id: $user_id, lifecycle_state: 'active'}})
        WHERE n.last_active_at < timestamp() - $declining_threshold
          AND coalesce(n.mentions, 0) < $min_recent
        SET n.lifecycle_state = 'declining'
        RETURN count(n) AS transitioned
        """
        params_d = {
            **params,
            "declining_threshold": active_to_declining_days * ms_per_day,
            "min_recent": min_recent_mentions,
        }
        result_declining = self.graph.query(cypher_declining, params=params_d)

        # DECLINING -> DORMANT
        cypher_dormant = f"""
        MATCH (n {self.node_label} {{user_id: $user_id, lifecycle_state: 'declining'}})
        WHERE n.last_active_at < timestamp() - $dormant_threshold
        SET n.lifecycle_state = 'dormant'
        RETURN count(n) AS transitioned
        """
        params_do = {
            **params,
            "dormant_threshold": declining_to_dormant_days * ms_per_day,
        }
        result_dormant = self.graph.query(cypher_dormant, params=params_do)

        declining_count = result_declining[0]["transitioned"] if result_declining else 0
        dormant_count = result_dormant[0]["transitioned"] if result_dormant else 0

        logger.info(
            f"Lifecycle update: {declining_count} ACTIVE->DECLINING, "
            f"{dormant_count} DECLINING->DORMANT"
        )

        return {
            "active_to_declining": declining_count,
            "declining_to_dormant": dormant_count,
        }

    def _retrieve_nodes_from_data(self, data, filters):
        """Extracts all the entities mentioned in the query."""
        _tools = [EXTRACT_ENTITIES_TOOL]
        if self.llm_provider in ["azure_openai_structured", "openai_structured"]:
            _tools = [EXTRACT_ENTITIES_STRUCT_TOOL]
        search_results = self.llm.generate_response(
            messages=[
                {
                    "role": "system",
                    "content": f"You are a smart assistant who understands entities and their types in a given text. If user message contains self reference such as 'I', 'me', 'my' etc. then use {filters['user_id']} as the source entity. Extract all the entities from the text. ***DO NOT*** answer the question itself if the given text is a question.",
                },
                {"role": "user", "content": data},
            ],
            tools=_tools,
        )

        entity_type_map = {}

        try:
            for tool_call in search_results["tool_calls"]:
                if tool_call["name"] != "extract_entities":
                    continue
                for item in tool_call["arguments"]["entities"]:
                    entity_type_map[item["entity"]] = item["entity_type"]
        except Exception as e:
            logger.exception(
                f"Error in search tool: {e}, llm_provider={self.llm_provider}, search_results={search_results}"
            )

        entity_type_map = {k.lower().replace(" ", "_"): v.lower().replace(" ", "_") for k, v in entity_type_map.items()}
        logger.debug(f"Entity type map: {entity_type_map}\n search_results={search_results}")
        return entity_type_map

    def _establish_nodes_relations_from_data(self, data, filters, entity_type_map):
        """Establish relations among the extracted nodes."""
        user_identity = f"user_id: {filters['user_id']}"
        if filters.get("agent_id"):
            user_identity += f", agent_id: {filters['agent_id']}"
        if filters.get("run_id"):
            user_identity += f", run_id: {filters['run_id']}"

        if self.config.graph_store.custom_prompt:
            system_content = EXTRACT_RELATIONS_PROMPT.replace("USER_ID", user_identity)
            system_content = system_content.replace("CUSTOM_PROMPT", f"4. {self.config.graph_store.custom_prompt}")
            messages = [
                {"role": "system", "content": system_content},
                {"role": "user", "content": data},
            ]
        else:
            system_content = EXTRACT_RELATIONS_PROMPT.replace("USER_ID", user_identity)
            messages = [
                {"role": "system", "content": system_content},
                {"role": "user", "content": f"List of entities: {list(entity_type_map.keys())}. \n\nText: {data}"},
            ]

        _tools = [RELATIONS_TOOL]
        if self.llm_provider in ["azure_openai_structured", "openai_structured"]:
            _tools = [RELATIONS_STRUCT_TOOL]

        extracted_entities = self.llm.generate_response(
            messages=messages,
            tools=_tools,
        )

        entities = []
        if extracted_entities.get("tool_calls"):
            entities = extracted_entities["tool_calls"][0].get("arguments", {}).get("entities", [])

        entities = self._remove_spaces_from_entities(entities)

        # Filter out relations where user_id leaked as an entity
        user_id_normalized = filters.get("user_id", "").lower().replace(" ", "_")
        if user_id_normalized:
            def _contains_user_id(val):
                return val == user_id_normalized or user_id_normalized in val
            entities = [e for e in entities if not _contains_user_id(e["source"]) and not _contains_user_id(e["destination"])]

        logger.debug(f"Extracted entities: {entities}")
        return entities

    def _search_graph_db(self, node_list, filters, limit=100):
        """Search similar nodes among and their respective incoming and outgoing relations."""
        result_relations = []

        node_props = ["user_id: $user_id"]
        if filters.get("agent_id"):
            node_props.append("agent_id: $agent_id")
        if filters.get("run_id"):
            node_props.append("run_id: $run_id")
        node_props_str = ", ".join(node_props)

        for node in node_list:
            n_embedding = self.embedding_model.embed(node)

            cypher_query = f"""
            MATCH (n {self.node_label} {{{node_props_str}}})
            WHERE n.embedding IS NOT NULL
            WITH n, round(2 * vector.similarity.cosine(n.embedding, $n_embedding) - 1, 4) AS similarity
            WHERE similarity >= $threshold
            CALL {{
                WITH n
                MATCH (n)-[r]->(m {self.node_label} {{{node_props_str}}})
                RETURN n.name AS source, elementId(n) AS source_id, type(r) AS relationship, elementId(r) AS relation_id, m.name AS destination, elementId(m) AS destination_id
                UNION
                WITH n
                MATCH (n)<-[r]-(m {self.node_label} {{{node_props_str}}})
                RETURN m.name AS source, elementId(m) AS source_id, type(r) AS relationship, elementId(r) AS relation_id, n.name AS destination, elementId(n) AS destination_id
            }}
            WITH distinct source, source_id, relationship, relation_id, destination, destination_id, similarity
            RETURN source, source_id, relationship, relation_id, destination, destination_id, similarity
            ORDER BY similarity DESC
            LIMIT $limit
            """

            params = {
                "n_embedding": n_embedding,
                "threshold": self.threshold,
                "user_id": filters["user_id"],
                "limit": limit,
            }
            if filters.get("agent_id"):
                params["agent_id"] = filters["agent_id"]
            if filters.get("run_id"):
                params["run_id"] = filters["run_id"]

            ans = self.graph.query(cypher_query, params=params)
            result_relations.extend(ans)

        return result_relations

    def _get_delete_entities_from_search_output(self, search_output, data, filters):
        """Get the entities to be deleted from the search output."""
        search_output_string = format_entities(search_output)

        user_identity = f"user_id: {filters['user_id']}"
        if filters.get("agent_id"):
            user_identity += f", agent_id: {filters['agent_id']}"
        if filters.get("run_id"):
            user_identity += f", run_id: {filters['run_id']}"

        system_prompt, user_prompt = get_delete_messages(search_output_string, data, user_identity)

        _tools = [DELETE_MEMORY_TOOL_GRAPH]
        if self.llm_provider in ["azure_openai_structured", "openai_structured"]:
            _tools = [DELETE_MEMORY_STRUCT_TOOL_GRAPH]

        memory_updates = self.llm.generate_response(
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            tools=_tools,
        )

        to_be_deleted = []
        for item in memory_updates.get("tool_calls", []):
            if item.get("name") == "delete_graph_memory":
                to_be_deleted.append(item.get("arguments"))
        to_be_deleted = self._remove_spaces_from_entities(to_be_deleted)
        logger.debug(f"Deleted relationships: {to_be_deleted}")
        return to_be_deleted

    def _delete_entities(self, to_be_deleted, filters):
        """Delete the entities from the graph."""
        user_id = filters["user_id"]
        agent_id = filters.get("agent_id", None)
        run_id = filters.get("run_id", None)
        results = []

        for item in to_be_deleted:
            source = item["source"]
            destination = item["destination"]
            relationship = item["relationship"]

            params = {
                "source_name": source,
                "dest_name": destination,
                "user_id": user_id,
            }

            if agent_id:
                params["agent_id"] = agent_id
            if run_id:
                params["run_id"] = run_id

            source_props = ["name: $source_name", "user_id: $user_id"]
            dest_props = ["name: $dest_name", "user_id: $user_id"]
            if agent_id:
                source_props.append("agent_id: $agent_id")
                dest_props.append("agent_id: $agent_id")
            if run_id:
                source_props.append("run_id: $run_id")
                dest_props.append("run_id: $run_id")
            source_props_str = ", ".join(source_props)
            dest_props_str = ", ".join(dest_props)

            cypher = f"""
            MATCH (n {self.node_label} {{{source_props_str}}})
            -[r:{relationship}]->
            (m {self.node_label} {{{dest_props_str}}})

            DELETE r
            RETURN
                n.name AS source,
                m.name AS target,
                type(r) AS relationship
            """

            result = self.graph.query(cypher, params=params)
            results.append(result)

        return results

    def _add_entities(self, to_be_added, filters, entity_type_map):
        """Add entities to graph using entity_key-based MERGE (prevents race-condition duplicates).

        entity_key = user_id + "::" + name is used as MERGE target with a uniqueness constraint,
        giving Neo4j a lock target to prevent duplicate creation under concurrent writes.

        Lifecycle properties:
        - ON CREATE: lifecycle_state='active', created_at=timestamp(), last_active_at=timestamp(), mentions=1
        - ON MATCH: last_active_at=timestamp(), mentions+=1, lifecycle_state='active'
        """
        user_id = filters["user_id"]
        agent_id = filters.get("agent_id", None)
        run_id = filters.get("run_id", None)
        results = []
        for item in to_be_added:
            source = item["source"]
            destination = item["destination"]
            relationship = item["relationship"]

            source_type = entity_type_map.get(source, "__User__")
            source_label = self.node_label if self.node_label else f":`{source_type}`"
            source_extra_set = f", source:`{source_type}`" if self.node_label else ""
            destination_type = entity_type_map.get(destination, "__User__")
            destination_label = self.node_label if self.node_label else f":`{destination_type}`"
            destination_extra_set = f", destination:`{destination_type}`" if self.node_label else ""

            source_embedding = self.embedding_model.embed(source)
            dest_embedding = self.embedding_model.embed(destination)

            # Compute entity_key for uniqueness constraint-backed MERGE
            source_key = f"{user_id}::{source}"
            dest_key = f"{user_id}::{destination}"

            # Build additional property setters for agent_id / run_id
            source_extra_props = ""
            dest_extra_props = ""
            if agent_id:
                source_extra_props += ", source.agent_id = $agent_id"
                dest_extra_props += ", destination.agent_id = $agent_id"
            if run_id:
                source_extra_props += ", source.run_id = $run_id"
                dest_extra_props += ", destination.run_id = $run_id"

            cypher = f"""
            MERGE (source {source_label} {{entity_key: $source_key}})
            ON CREATE SET source.name = $source_name,
                        source.user_id = $user_id,
                        source.created = timestamp(),
                        source.created_at = timestamp(),
                        source.last_active_at = timestamp(),
                        source.lifecycle_state = 'active',
                        source.mentions = 1,
                        source.entity_type = $source_type
                        {source_extra_set}
                        {source_extra_props}
            ON MATCH SET source.mentions = coalesce(source.mentions, 0) + 1,
                        source.last_active_at = timestamp(),
                        source.lifecycle_state = 'active'
            WITH source
            CALL db.create.setNodeVectorProperty(source, 'embedding', $source_embedding)
            WITH source
            MERGE (destination {destination_label} {{entity_key: $dest_key}})
            ON CREATE SET destination.name = $dest_name,
                        destination.user_id = $user_id,
                        destination.created = timestamp(),
                        destination.created_at = timestamp(),
                        destination.last_active_at = timestamp(),
                        destination.lifecycle_state = 'active',
                        destination.mentions = 1,
                        destination.entity_type = $dest_type
                        {destination_extra_set}
                        {dest_extra_props}
            ON MATCH SET destination.mentions = coalesce(destination.mentions, 0) + 1,
                        destination.last_active_at = timestamp(),
                        destination.lifecycle_state = 'active'
            WITH source, destination
            CALL db.create.setNodeVectorProperty(destination, 'embedding', $dest_embedding)
            With source, destination
            MERGE (source)-[rel:{relationship}]->(destination)
            ON CREATE SET rel.created_at = timestamp(), rel.mentions = 1
            ON MATCH SET rel.mentions = coalesce(rel.mentions, 0) + 1, rel.last_mentioned_at = timestamp()
            RETURN source.name AS source, type(rel) AS relationship, destination.name AS target
            """

            params = {
                "source_name": source,
                "dest_name": destination,
                "source_key": source_key,
                "dest_key": dest_key,
                "source_embedding": source_embedding,
                "dest_embedding": dest_embedding,
                "source_type": source_type,
                "dest_type": destination_type,
                "user_id": user_id,
            }
            if agent_id:
                params["agent_id"] = agent_id
            if run_id:
                params["run_id"] = run_id
            result = self.graph.query(cypher, params=params)
            results.append(result)
        return results

    def _remove_spaces_from_entities(self, entity_list):
        for item in entity_list:
            item["source"] = item["source"].lower().replace(" ", "_")
            item["relationship"] = re.sub(r"[^a-zA-Z0-9_]", "_", item["relationship"].lower().replace(" ", "_"))
            item["relationship"] = re.sub(r"_+", "_", item["relationship"]).strip("_")
            if item["relationship"] and item["relationship"][0].isdigit():
                item["relationship"] = "rel_" + item["relationship"]
            if not item["relationship"]:
                item["relationship"] = "related_to"
            item["destination"] = item["destination"].lower().replace(" ", "_")
        entity_list = [item for item in entity_list if item["source"] != item["destination"]]
        return entity_list

    def reset(self):
        """Reset the graph by clearing all nodes and relationships."""
        logger.warning("Clearing graph...")
        cypher_query = """
        MATCH (n) DETACH DELETE n
        """
        return self.graph.query(cypher_query)
