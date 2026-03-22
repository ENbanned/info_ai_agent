import json
from pathlib import Path

_CONFIG_PATH = Path(__file__).resolve().parent.parent / "config.json"

with open(_CONFIG_PATH) as f:
    CONFIG = json.load(f)

TG_CONFIG = CONFIG["telegram"]
VOYAGE_CONFIG = CONFIG["voyage"]
BOT_CONFIG = CONFIG["bot"]
SOURCES_CONFIG = CONFIG["sources"]
MODELS_CONFIG = CONFIG["models"]

# Lazy import to avoid circular dependency at module level
from src.pipeline.prompts import CRYPTO_EXTRACTION_PROMPT, GRAPH_CUSTOM_PROMPT

CUSTOM_UPDATE_MEMORY_PROMPT = """\
You are updating a memory entry with new information. Follow these rules strictly:

1. TIMESTAMPS ARE SACRED. Every fact has a timestamp (e.g., "On 2026-03-16 at 16:51 UTC, ..."). \
Never drop, merge, or summarize away timestamps. They are the most important metadata.

2. When merging new information into an existing memory:
   - Preserve chronological order. Earlier events come first.
   - Keep all timestamps intact. Do not replace specific times with vague references like "recently" or "earlier".
   - Example of CORRECT merge: "On 2026-03-16 at 16:51 UTC, BTC broke above $90k. \
Later on 2026-03-16 at 17:44 UTC, BTC reached $91.2k with $200M in liquidations."
   - Example of WRONG merge: "BTC rose from $90k to $91.2k with liquidations." (timestamps lost)

3. If the new information contradicts the old, keep BOTH with their timestamps. \
The reader needs to see the timeline, not just the latest state.

4. Never summarize a sequence of timestamped events into a single statement. \
The chronological record IS the value.

5. LIFECYCLE STATES: If the existing memory has a lifecycle_state field, respect it:
   - "active" facts can be updated normally.
   - "reported" facts should only be updated if NEW information changes their meaning. \
Do not update a reported fact just to rephrase it.
   - "archived" facts should not be updated — they are historical records. \
If new information arrives about an archived topic, create a new memory instead of updating the old one.

6. SUPERSESSION: If new information directly contradicts or replaces old information \
(not just adds to it), note the supersession clearly. Keep both the old and new information \
with their timestamps so the reader can see what changed. \
Example: "On 2026-03-16 at 14:00 UTC, Project X announced partnership with Y. \
[SUPERSEDED by: On 2026-03-17 at 09:00 UTC, Project X denied partnership with Y, calling earlier reports inaccurate.]" """

# Override mem0 LLM model with models.extraction
_mem0_base = {**CONFIG["mem0"]}
_mem0_base["llm"] = {**_mem0_base["llm"], "config": {"model": MODELS_CONFIG["extraction"]}}

MEM0_CONFIG = {
    **_mem0_base,
    "custom_fact_extraction_prompt": CRYPTO_EXTRACTION_PROMPT,
    "custom_update_memory_prompt": CUSTOM_UPDATE_MEMORY_PROMPT,
    "embedder": {
        "provider": "voyage",
        "config": {
            "model": VOYAGE_CONFIG["embed_model"],
            "embedding_dims": VOYAGE_CONFIG["embedding_dims"],
            "api_key": VOYAGE_CONFIG["api_key"],
        },
    },
    "reranker": {
        "provider": "voyage",
        "config": {
            "model": VOYAGE_CONFIG["rerank_model"],
            "api_key": VOYAGE_CONFIG["api_key"],
        },
    },
}

# Inject custom graph prompt into graph_store config
MEM0_CONFIG["graph_store"]["custom_prompt"] = GRAPH_CUSTOM_PROMPT
