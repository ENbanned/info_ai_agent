"""Batch alias resolver for Neo4j entity graph.

Finds entity aliases (different names for the same real-world entity)
using embedding similarity as blocking + LLM judge for matching.
Merges confirmed aliases using APOC mergeNodes.

Usage: uv run python -m patches.alias_resolver --user-id trader
"""
import argparse
import asyncio
import logging
import os
from itertools import combinations

from claude_agent_sdk import ClaudeAgentOptions, AssistantMessage, ResultMessage, TextBlock, query
from langchain_neo4j import Neo4jGraph

logger = logging.getLogger(__name__)


def get_entities(graph, user_id: str):
    """Fetch all entity nodes with embeddings."""
    cypher = """
    MATCH (n:`__Entity__` {user_id: $user_id})
    WHERE n.embedding IS NOT NULL
    RETURN n.name AS name, n.entity_type AS entity_type, n.embedding AS embedding, elementId(n) AS id
    """
    return graph.query(cypher, params={"user_id": user_id})


def find_candidates(entities, similarity_threshold=0.8):
    """Find candidate pairs using cosine similarity blocking."""
    import numpy as np

    candidates = []
    for i, j in combinations(range(len(entities)), 2):
        a, b = entities[i], entities[j]
        # Only compare same entity_type
        if a.get("entity_type") != b.get("entity_type"):
            continue
        # Skip if names are identical (already merged by exact-name MERGE)
        if a["name"] == b["name"]:
            continue

        # Cosine similarity
        va = np.array(a["embedding"])
        vb = np.array(b["embedding"])
        sim = float(np.dot(va, vb) / (np.linalg.norm(va) * np.linalg.norm(vb)))

        if sim >= similarity_threshold:
            candidates.append((a, b, sim))

    return candidates


async def llm_judge(name_a, type_a, name_b, type_b):
    """Ask LLM whether two entities are the same real-world entity."""
    os.environ.pop("CLAUDECODE", None)
    prompt = (
        f"Entity A: '{name_a}' (type: {type_a}). "
        f"Entity B: '{name_b}' (type: {type_b}). "
        f"Are these the same real-world entity? Answer only YES or NO."
    )
    options = ClaudeAgentOptions(
        model="haiku",
        max_turns=1,
        permission_mode="acceptEdits",
        tools=[],
        cwd="/tmp/mem0-alias-resolver",
    )
    os.makedirs("/tmp/mem0-alias-resolver", exist_ok=True)

    result_text = ""
    async for message in query(prompt=prompt, options=options):
        if isinstance(message, ResultMessage) and message.result:
            result_text = message.result
        elif isinstance(message, AssistantMessage):
            for block in message.content:
                if isinstance(block, TextBlock):
                    result_text = block.text
                    break

    return "YES" in result_text.upper()


def merge_nodes(graph, id_a, id_b, name_a, name_b):
    """Merge two nodes using APOC, keeping the first as canonical."""
    cypher = """
    MATCH (a) WHERE elementId(a) = $id_a
    MATCH (b) WHERE elementId(b) = $id_b
    CALL apoc.refactor.mergeNodes([a, b], {properties: 'combine', mergeRels: true})
    YIELD node
    RETURN node.name AS merged_name
    """
    result = graph.query(cypher, params={"id_a": id_a, "id_b": id_b})
    return result


async def main():
    parser = argparse.ArgumentParser(description="Batch alias resolver for entity graph")
    parser.add_argument("--user-id", required=True, help="User ID to resolve aliases for")
    parser.add_argument("--threshold", type=float, default=0.8, help="Similarity threshold for candidates")
    parser.add_argument("--neo4j-url", default="bolt://localhost:7687")
    parser.add_argument("--neo4j-user", default="neo4j")
    parser.add_argument("--neo4j-pass", default="mem0graphpass")
    parser.add_argument("--dry-run", action="store_true", help="Show candidates without merging")
    args = parser.parse_args()

    graph = Neo4jGraph(args.neo4j_url, args.neo4j_user, args.neo4j_pass, refresh_schema=False)

    print(f"Fetching entities for user_id={args.user_id}...")
    entities = get_entities(graph, args.user_id)
    print(f"Found {len(entities)} entities")

    print(f"Finding candidates (threshold={args.threshold})...")
    candidates = find_candidates(entities, args.threshold)
    print(f"Found {len(candidates)} candidate pairs")

    if not candidates:
        print("No alias candidates found.")
        return

    merges = []
    for a, b, sim in candidates:
        print(f"\n  Candidate: '{a['name']}' vs '{b['name']}' (sim={sim:.3f}, type={a.get('entity_type')})")
        is_same = await llm_judge(a["name"], a.get("entity_type", ""), b["name"], b.get("entity_type", ""))
        verdict = "YES — SAME" if is_same else "NO — DIFFERENT"
        print(f"  LLM judge: {verdict}")
        if is_same:
            merges.append((a, b))

    if not merges:
        print("\nNo aliases confirmed by LLM judge.")
        return

    print(f"\n{'='*60}")
    print(f"Confirmed merges: {len(merges)}")
    print(f"{'='*60}")

    for a, b in merges:
        print(f"  MERGE: '{a['name']}' + '{b['name']}' → '{a['name']}'")
        if not args.dry_run:
            result = merge_nodes(graph, a["id"], b["id"], a["name"], b["name"])
            print(f"    Result: {result}")

    if args.dry_run:
        print("\n(Dry run — no merges performed)")


if __name__ == "__main__":
    asyncio.run(main())
