import json
from pathlib import Path

from src.pipeline.prompts import CRYPTO_EXTRACTION_PROMPT, GRAPH_CUSTOM_PROMPT, CUSTOM_UPDATE_MEMORY_PROMPT

_CONFIG_PATH = Path(__file__).resolve().parent.parent / "config.json"

with open(_CONFIG_PATH) as f:
    CONFIG = json.load(f)

TG_CONFIG = CONFIG["telegram"]
VOYAGE_CONFIG = CONFIG["voyage"]
BOT_CONFIG = CONFIG["bot"]
SOURCES_CONFIG = CONFIG["sources"]
MODELS_CONFIG = CONFIG["models"]

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
