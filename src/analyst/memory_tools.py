"""MCP server exposing memory search tools for the analyst agent."""

from datetime import datetime, timezone

from claude_agent_sdk import create_sdk_mcp_server, tool


def create_memory_server(memory):
    """Create an MCP server with memory search tools bound to the given memory instance."""

    @tool(
        name="search_memory",
        description=(
            "Semantic search across all stored facts and analyst conclusions. "
            "Use this to find historical context, track narrative evolution, "
            "recall previous analysis, or find related events."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Natural language search query (e.g., 'BTC funding rates spike', 'SEC enforcement actions', 'ai narrative momentum')",
                },
                "scope": {
                    "type": "string",
                    "enum": ["all", "facts", "analyst"],
                    "description": "Search scope: 'all' for everything, 'facts' for channel-sourced facts only, 'analyst' for previous analyst conclusions only",
                    "default": "all",
                },
                "limit": {
                    "type": "integer",
                    "description": "Max results to return (default 20)",
                    "default": 20,
                },
                "state_filter": {
                    "type": "string",
                    "description": "Filter by lifecycle state (e.g., 'active', 'reported', 'declining'). Leave empty for all states.",
                },
            },
            "required": ["query"],
        },
    )
    async def search_memory(args):
        query = args["query"]
        scope = args.get("scope", "all")
        limit = args.get("limit", 20)
        state_filter = args.get("state_filter")

        results = []

        # Build metadata filters if state_filter is provided
        metadata_filters = {}
        if state_filter:
            metadata_filters["lifecycle_state"] = state_filter

        if scope in ("all", "facts"):
            search_kwargs = {"user_id": "trader", "limit": limit}
            if metadata_filters:
                search_kwargs["filters"] = metadata_filters
            fact_results = await memory.search(query, **search_kwargs)
            for mem in fact_results.get("results", []):
                text = mem.get("memory", mem.get("data", ""))
                if text:
                    results.append(f"[fact] {text}")

        if scope in ("all", "analyst"):
            search_kwargs = {"user_id": "trader", "agent_id": "analyst", "limit": limit}
            if metadata_filters:
                search_kwargs["filters"] = metadata_filters
            analyst_results = await memory.search(query, **search_kwargs)
            for mem in analyst_results.get("results", []):
                text = mem.get("memory", mem.get("data", ""))
                if text:
                    results.append(f"[analyst] {text}")

        if not results:
            return {"content": [{"type": "text", "text": f"No results found for: {query}"}]}

        return {
            "content": [
                {
                    "type": "text",
                    "text": f"Found {len(results)} results for '{query}':\n\n"
                    + "\n\n".join(results[:limit]),
                }
            ]
        }

    @tool(
        name="query_entity",
        description=(
            "Look up a specific entity in the knowledge graph to see all its "
            "relationships and connections. Use this to understand how tokens, "
            "people, projects, and events are connected."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "entity": {
                    "type": "string",
                    "description": "Entity name to look up (e.g., 'btc', 'sec', 'arbitrum', 'vitalik')",
                },
            },
            "required": ["entity"],
        },
    )
    async def query_entity(args):
        entity = args["entity"].lower().replace(" ", "_")

        # Use direct Cypher query via get_entity_relationships (O(1) by index)
        # instead of get_all() + Python filter (O(N) full graph scan)
        rels = memory.graph.get_entity_relationships(
            entity, filters={"user_id": "trader"}, limit=50
        )

        matches = []
        for rel in rels:
            src = rel.get("source", "")
            dst = rel.get("target", rel.get("destination", ""))
            r = rel.get("relationship", "?")
            matches.append(f"{src} —[{r}]→ {dst}")

        if not matches:
            return {"content": [{"type": "text", "text": f"No graph relationships found for entity: {entity}"}]}

        return {
            "content": [
                {
                    "type": "text",
                    "text": f"Entity '{entity}' — {len(matches)} relationships:\n\n"
                    + "\n".join(matches),
                }
            ]
        }

    @tool(
        name="get_cycle_summary",
        description=(
            "Get a summary of how a topic has evolved across previous analyst cycles. "
            "Use this to understand the history of a topic before writing about it. "
            "Helps avoid repetition by showing what you've already written."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "topic": {
                    "type": "string",
                    "description": "Topic to look up history for (e.g., 'BTC ETF inflows', 'Solana ecosystem growth', 'DeFi yields')",
                },
            },
            "required": ["topic"],
        },
    )
    async def get_cycle_summary(args):
        topic = args["topic"]

        # Search previous analyst conclusions about this topic
        results = await memory.search(
            topic, user_id="trader", agent_id="analyst", limit=10
        )

        memories = results.get("results", [])
        if not memories:
            return {"content": [{"type": "text", "text": f"No previous analyst coverage found for: {topic}"}]}

        # Sort chronologically by timestamp (oldest first)
        def _get_ts(mem):
            md = mem.get("metadata", {})
            return md.get("timestamp", md.get("created_at", 0))

        memories.sort(key=_get_ts)

        lines = []
        for mem in memories:
            text = mem.get("memory", mem.get("data", ""))
            if not text:
                continue

            md = mem.get("metadata", {})
            ts = _get_ts(mem)

            # Format date
            if ts and ts > 0:
                try:
                    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                    date_str = dt.strftime("%Y-%m-%d %H:%M UTC")
                except (ValueError, OSError):
                    date_str = "unknown date"
            else:
                date_str = "unknown date"

            # Extract thesis/confidence if available
            extras = []
            if md.get("thesis"):
                extras.append(f"thesis: {md['thesis']}")
            if md.get("confidence"):
                extras.append(f"confidence: {md['confidence']}")
            extra_str = f" ({', '.join(extras)})" if extras else ""

            lines.append(f"[{date_str}]{extra_str}\n{text}")

        if not lines:
            return {"content": [{"type": "text", "text": f"No previous analyst coverage found for: {topic}"}]}

        return {
            "content": [
                {
                    "type": "text",
                    "text": f"Cycle history for '{topic}' — {len(lines)} entries:\n\n"
                    + "\n\n---\n\n".join(lines),
                }
            ]
        }

    return create_sdk_mcp_server(
        name="memory_tools",
        version="1.0.0",
        tools=[search_memory, query_entity, get_cycle_summary],
    )
