#!/bin/bash
set -e
DIR="$(dirname "$0")"
ROOT="$(cd "$DIR/../.." && pwd)"

# Find python in .venv (project root) or fall back to system
PYTHON="$ROOT/.venv/bin/python3"
[ -f "$PYTHON" ] || PYTHON="python3"

# Get site-packages path without importing mem0 (avoids circular import issues)
SITE=$($PYTHON -c "import site; print([p for p in site.getsitepackages() if 'site-packages' in p][0])")/mem0

# Configs first (factory depends on these)
cp "$DIR/claude_code_config.py" "$SITE/configs/llms/claude_code.py"
cp "$DIR/voyage_reranker_config.py" "$SITE/configs/rerankers/voyage.py"
cp "$DIR/llm_configs.py" "$SITE/llms/configs.py"
cp "$DIR/embeddings_configs.py" "$SITE/embeddings/configs.py"

# Core modules
cp "$DIR/factory.py" "$SITE/utils/factory.py"
cp "$DIR/claude_code.py" "$SITE/llms/claude_code.py"
cp "$DIR/anthropic.py" "$SITE/llms/anthropic.py"
cp "$DIR/voyage.py" "$SITE/embeddings/voyage.py"
cp "$DIR/voyage_reranker.py" "$SITE/reranker/voyage_reranker.py"
cp "$DIR/qdrant.py" "$SITE/vector_stores/qdrant.py"
cp "$DIR/main.py" "$SITE/memory/main.py"
cp "$DIR/graph_memory.py" "$SITE/memory/graph_memory.py"
cp "$DIR/validator.py" "$SITE/memory/validator.py"

echo "All patches applied to $SITE"
