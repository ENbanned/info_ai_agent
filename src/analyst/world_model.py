"""Persistent world model — structured knowledge accumulated across analyst cycles.

The world model is a JSON file on disk (data/world_model.json) that the analyst
updates every cycle. It stores market regime, active theses, narratives,
macro environment, source reliability notes, and meta-cognitive observations.

The analyst receives the current world model in its prompt and returns a
structured update block (<world_model_update>...</world_model_update>) at the
end of each report.  This module handles loading, saving, formatting for the
prompt, parsing the update block, and applying changes.
"""

import copy
import json
import re
import time
from pathlib import Path

from loguru import logger

# ---------------------------------------------------------------------------
# Path
# ---------------------------------------------------------------------------
WORLD_MODEL_PATH = Path(__file__).resolve().parent.parent.parent / "data" / "world_model.json"

# ---------------------------------------------------------------------------
# Default (empty) world model — used on first launch
# ---------------------------------------------------------------------------
DEFAULT_WORLD_MODEL: dict = {
    "version": 1,
    "last_updated": 0,
    "last_cycle_id": "",

    "market_regime": {
        "current": "unknown",        # bull / bear / sideways / crisis / transition
        "confidence": 0.5,
        "since": "",
        "description": "",
    },

    "active_theses": [
        # {
        #     "id": "thesis_001",
        #     "name": "BTC Safe Haven",
        #     "state": "active",             # active / confirmed / invalidated / developing / retired
        #     "confidence": 0.5,
        #     "direction": "stable",          # up / down / stable
        #     "created_cycle": "",
        #     "last_updated_cycle": "",
        #     "history": [],                  # [{cycle_id, confidence, note}]
        #     "falsification_criteria": "",
        #     "description": "",
        # }
    ],

    "active_narratives": [
        # {
        #     "name": "RWA Tokenization",
        #     "phase": "growth",             # emerging / growth / mature / declining / dead
        #     "key_catalysts": [],
        #     "last_updated_cycle": "",
        # }
    ],

    "macro_environment": {
        "fed_rate": "",
        "inflation_trend": "",
        "oil_situation": "",
        "geopolitical_risks": [],
        "key_dates": [],                     # [{date, event, impact}]
    },

    "source_reliability": {
        # "channel_name": {"accuracy_notes": "", "bias": "", "speed": ""}
    },

    "meta_cognitive": {
        "known_biases": [],                  # Systematic errors the analyst has noticed in itself
        "learned_patterns": [],              # Patterns that proved reliable
        "failed_patterns": [],               # Patterns that didn't work
    },
}


# ---------------------------------------------------------------------------
# Load / Save
# ---------------------------------------------------------------------------

def load_world_model() -> dict:
    """Load world model from disk. Returns default if file does not exist."""
    if WORLD_MODEL_PATH.exists():
        try:
            data = json.loads(WORLD_MODEL_PATH.read_text(encoding="utf-8"))
            logger.debug(f"World model loaded | {WORLD_MODEL_PATH}")
            return data
        except (json.JSONDecodeError, Exception) as e:
            logger.warning(f"Failed to load world model: {e}, using default")
    return copy.deepcopy(DEFAULT_WORLD_MODEL)


def save_world_model(model: dict) -> None:
    """Save world model to disk."""
    model["last_updated"] = int(time.time())
    WORLD_MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    WORLD_MODEL_PATH.write_text(
        json.dumps(model, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    n_theses = len(model.get("active_theses", []))
    n_narratives = len(model.get("active_narratives", []))
    logger.info(
        f"World model saved | {n_theses} theses, "
        f"{n_narratives} narratives"
    )


# ---------------------------------------------------------------------------
# Format for analyst prompt
# ---------------------------------------------------------------------------

def format_world_model_for_prompt(model: dict) -> str:
    """Format the world model as readable Markdown for the analyst prompt."""

    parts: list[str] = []
    parts.append("## Your World Model (accumulated knowledge)\n")

    # --- Market Regime ---
    regime = model.get("market_regime", {})
    regime_name = regime.get("current", "unknown").upper()
    regime_conf = regime.get("confidence", 0.5)
    regime_since = regime.get("since", "")
    regime_desc = regime.get("description", "")
    since_str = f"Since: {regime_since}. " if regime_since else ""
    desc_str = regime_desc if regime_desc else "No description yet."
    parts.append(
        f"### Market Regime: {regime_name} (confidence: {int(regime_conf * 100)}%)\n"
        f"{since_str}{desc_str}\n"
    )

    # --- Active Theses ---
    theses = model.get("active_theses", [])
    active_theses = [t for t in theses if t.get("state") not in ("retired", "invalidated", "confirmed")]
    resolved_theses = [t for t in theses if t.get("state") in ("invalidated", "confirmed")]

    if active_theses:
        parts.append(f"### Active Theses ({len(active_theses)}):")
        for i, t in enumerate(active_theses, 1):
            state = t.get("state", "active").upper()
            conf = t.get("confidence", 0.5)
            direction = t.get("direction", "stable")
            dir_symbol = {"up": "\u2191", "down": "\u2193", "stable": "="}.get(direction, "?")
            name = t.get("name", "Unnamed")
            desc = t.get("description", "")
            desc_str = f" \u2014 {desc}" if desc else ""
            parts.append(f"{i}. [{state} {int(conf * 100)}%{dir_symbol}] {name}{desc_str}")
        parts.append("")

    if resolved_theses:
        parts.append(f"### Recently Resolved Theses ({len(resolved_theses)}):")
        for t in resolved_theses[-5:]:  # last 5 resolved
            state = t.get("state", "?").upper()
            name = t.get("name", "Unnamed")
            parts.append(f"- [{state}] {name}")
        parts.append("")

    if not theses:
        parts.append("### Active Theses: none yet (first cycles)\n")

    # --- Active Narratives ---
    narratives = model.get("active_narratives", [])
    if narratives:
        parts.append(f"### Active Narratives ({len(narratives)}):")
        for n in narratives:
            name = n.get("name", "?")
            phase = n.get("phase", "?")
            catalysts = n.get("key_catalysts", [])
            cat_str = f" | catalysts: {', '.join(catalysts)}" if catalysts else ""
            parts.append(f"- {name} [{phase} phase]{cat_str}")
        parts.append("")
    else:
        parts.append("### Active Narratives: none tracked yet\n")

    # --- Macro Environment ---
    macro = model.get("macro_environment", {})
    macro_lines = []
    if macro.get("fed_rate"):
        macro_lines.append(f"- Fed Rate: {macro['fed_rate']}")
    if macro.get("inflation_trend"):
        macro_lines.append(f"- Inflation trend: {macro['inflation_trend']}")
    if macro.get("oil_situation"):
        macro_lines.append(f"- Oil: {macro['oil_situation']}")
    for risk in macro.get("geopolitical_risks", []):
        macro_lines.append(f"- Geopolitical: {risk}")
    for kd in macro.get("key_dates", []):
        date = kd.get("date", "?")
        event = kd.get("event", "?")
        impact = kd.get("impact", "")
        imp_str = f" ({impact})" if impact else ""
        macro_lines.append(f"- Upcoming: {date} — {event}{imp_str}")

    if macro_lines:
        parts.append("### Macro Environment:")
        parts.extend(macro_lines)
        parts.append("")
    else:
        parts.append("### Macro Environment: no data yet\n")

    # --- Source Reliability ---
    sources = model.get("source_reliability", {})
    if sources:
        parts.append(f"### Source Reliability Notes ({len(sources)} sources):")
        for channel, info in list(sources.items())[:10]:
            notes = info.get("accuracy_notes", "")
            bias = info.get("bias", "")
            extras = []
            if notes:
                extras.append(notes)
            if bias:
                extras.append(f"bias: {bias}")
            detail = " | ".join(extras) if extras else "no notes"
            parts.append(f"- {channel}: {detail}")
        parts.append("")

    # --- Meta-Cognitive ---
    meta = model.get("meta_cognitive", {})
    has_meta = False

    biases = meta.get("known_biases", [])
    if biases:
        has_meta = True
        parts.append("### Meta-Cognitive Notes:")
        parts.append("**Known biases:**")
        for b in biases:
            parts.append(f"- {b}")

    learned = meta.get("learned_patterns", [])
    if learned:
        has_meta = True
        parts.append("**Learned patterns (reliable):**")
        for p in learned:
            parts.append(f"- {p}")

    failed = meta.get("failed_patterns", [])
    if failed:
        has_meta = True
        parts.append("**Failed patterns (do not repeat):**")
        for p in failed:
            parts.append(f"- {p}")

    if has_meta:
        parts.append("")

    return "\n".join(parts)


# ---------------------------------------------------------------------------
# World model update instruction (appended to analyst prompt)
# ---------------------------------------------------------------------------

WORLD_MODEL_UPDATE_INSTRUCTION = """\

## World Model Update (MANDATORY)

After writing the report, you MUST update your World Model. Write a JSON block between markers.
This block will be parsed automatically and will NOT appear in the user-facing report.

Rules:
- Only include fields you want to change. Omit fields that stay the same.
- For thesis_updates: reference theses by name. Include confidence (0.0-1.0), direction (up/down/stable), and a short note.
- For new_theses: provide name, confidence, direction, description, falsification_criteria.
- For retired_theses: list names of theses to retire (no longer relevant).
- meta_cognitive_note: any self-reflection insight from this cycle (string or null).

<world_model_update>
{
  "market_regime": {"current": "crisis", "confidence": 0.85, "since": "2026-03-01", "description": "Stagflation + geopolitical risk"},
  "thesis_updates": [
    {"name": "BTC Safe Haven", "confidence": 0.30, "direction": "down", "note": "ETF outflows continue"}
  ],
  "new_theses": [
    {
      "name": "Example Thesis",
      "confidence": 0.60,
      "direction": "stable",
      "description": "Description of the thesis",
      "falsification_criteria": "What would prove this wrong"
    }
  ],
  "retired_theses": ["Old Thesis Name"],
  "narrative_updates": [
    {"name": "RWA Tokenization", "phase": "growth", "key_catalysts": ["BlackRock fund launch"]}
  ],
  "new_narratives": [
    {"name": "New Narrative", "phase": "emerging", "key_catalysts": ["catalyst1"]}
  ],
  "macro_update": {
    "fed_rate": "3.50-3.75% (hold)",
    "inflation_trend": "sticky above 4%",
    "oil_situation": "$110-119 structural shock",
    "geopolitical_risks": ["Iran conflict escalation", "US-China trade tensions"],
    "key_dates": [{"date": "2026-04-01", "event": "OPEC meeting", "impact": "oil supply decision"}]
  },
  "source_reliability_updates": {
    "channel_name": {"accuracy_notes": "Reliable on macro", "bias": "slightly bullish", "speed": "fast"}
  },
  "meta_cognitive_note": "I tend to overweight geopolitical risk on short-term crypto prices"
}
</world_model_update>
"""


# ---------------------------------------------------------------------------
# Parse world model update from report text
# ---------------------------------------------------------------------------

_WM_UPDATE_RE = re.compile(
    r"<world_model_update>\s*(\{.*?\})\s*</world_model_update>",
    re.DOTALL,
)


def parse_world_model_update(report_text: str) -> dict | None:
    """Extract world model update JSON from analyst report text.

    Returns the parsed dict, or None if not found / invalid JSON.
    """
    match = _WM_UPDATE_RE.search(report_text)
    if not match:
        logger.warning("World model update block not found in report")
        return None

    raw = match.group(1)
    try:
        update = json.loads(raw)
        logger.info("World model update parsed successfully")
        return update
    except json.JSONDecodeError as e:
        logger.warning(f"World model update JSON parse error: {e}")
        return None


def strip_world_model_block(report_text: str) -> str:
    """Remove <world_model_update>...</world_model_update> block from report text.

    This must be called before sending the report to Telegram / the user.
    """
    cleaned = re.sub(
        r"\s*<world_model_update>.*?</world_model_update>\s*",
        "\n",
        report_text,
        flags=re.DOTALL,
    )
    return cleaned.strip()


# ---------------------------------------------------------------------------
# Apply update to model
# ---------------------------------------------------------------------------

def apply_world_model_update(model: dict, update: dict, cycle_id: str) -> dict:
    """Apply the analyst's structured update to the world model.

    Mutates and returns *model*.
    """
    # --- Market regime ---
    if "market_regime" in update:
        mr = update["market_regime"]
        for key in ("current", "confidence", "since", "description"):
            if key in mr:
                model["market_regime"][key] = mr[key]

    # --- Thesis updates (existing theses) ---
    for tu in update.get("thesis_updates", []):
        name = tu.get("name", "")
        if not name:
            continue
        found = False
        for thesis in model.get("active_theses", []):
            if thesis.get("name", "").lower() == name.lower():
                found = True
                if "confidence" in tu:
                    thesis["confidence"] = tu["confidence"]
                if "direction" in tu:
                    thesis["direction"] = tu["direction"]
                if "state" in tu:
                    thesis["state"] = tu["state"]
                thesis["last_updated_cycle"] = cycle_id
                # Append to history
                thesis.setdefault("history", []).append({
                    "cycle_id": cycle_id,
                    "confidence": tu.get("confidence", thesis.get("confidence")),
                    "note": tu.get("note", ""),
                })
                break
        if not found:
            logger.debug(f"Thesis update for unknown thesis '{name}', treating as new")
            # Treat as new thesis
            new_t = _make_thesis(name, tu, cycle_id)
            model.setdefault("active_theses", []).append(new_t)

    # --- New theses ---
    for nt in update.get("new_theses", []):
        name = nt.get("name", "")
        if not name:
            continue
        # Check for duplicates (case-insensitive)
        existing_names = {t.get("name", "").lower() for t in model.get("active_theses", [])}
        if name.lower() in existing_names:
            logger.debug(f"New thesis '{name}' already exists, skipping")
            continue
        new_t = _make_thesis(name, nt, cycle_id)
        model.setdefault("active_theses", []).append(new_t)

    # --- Retired theses ---
    for retired_name in update.get("retired_theses", []):
        for thesis in model.get("active_theses", []):
            if thesis.get("name", "").lower() == retired_name.lower():
                thesis["state"] = "retired"
                thesis["last_updated_cycle"] = cycle_id
                break

    # --- Narrative updates ---
    for nu in update.get("narrative_updates", []):
        name = nu.get("name", "")
        if not name:
            continue
        found = False
        for narr in model.get("active_narratives", []):
            if narr.get("name", "").lower() == name.lower():
                found = True
                if "phase" in nu:
                    narr["phase"] = nu["phase"]
                if "key_catalysts" in nu:
                    narr["key_catalysts"] = nu["key_catalysts"]
                narr["last_updated_cycle"] = cycle_id
                break
        if not found:
            logger.debug(f"Narrative update for unknown narrative '{name}', treating as new")
            model.setdefault("active_narratives", []).append({
                "name": name,
                "phase": nu.get("phase", "emerging"),
                "key_catalysts": nu.get("key_catalysts", []),
                "last_updated_cycle": cycle_id,
            })

    # --- New narratives ---
    for nn in update.get("new_narratives", []):
        name = nn.get("name", "")
        if not name:
            continue
        existing_names = {n.get("name", "").lower() for n in model.get("active_narratives", [])}
        if name.lower() in existing_names:
            logger.debug(f"New narrative '{name}' already exists, skipping")
            continue
        model.setdefault("active_narratives", []).append({
            "name": name,
            "phase": nn.get("phase", "emerging"),
            "key_catalysts": nn.get("key_catalysts", []),
            "last_updated_cycle": cycle_id,
        })

    # --- Macro update ---
    if "macro_update" in update:
        mu = update["macro_update"]
        macro = model.setdefault("macro_environment", {})
        for key in ("fed_rate", "inflation_trend", "oil_situation"):
            if key in mu and mu[key]:
                macro[key] = mu[key]
        if "geopolitical_risks" in mu:
            macro["geopolitical_risks"] = mu["geopolitical_risks"]
        if "key_dates" in mu:
            macro["key_dates"] = mu["key_dates"]

    # --- Source reliability updates ---
    if "source_reliability_updates" in update:
        sr = model.setdefault("source_reliability", {})
        for channel, info in update["source_reliability_updates"].items():
            sr[channel] = info

    # --- Meta-cognitive note ---
    note = update.get("meta_cognitive_note")
    if note and isinstance(note, str) and note.strip():
        meta = model.setdefault("meta_cognitive", {
            "known_biases": [],
            "learned_patterns": [],
            "failed_patterns": [],
        })
        meta.setdefault("known_biases", []).append(f"[{cycle_id}] {note.strip()}")
        # Keep max 20 bias notes
        if len(meta["known_biases"]) > 20:
            meta["known_biases"] = meta["known_biases"][-20:]

    # --- Bookkeeping ---
    model["last_cycle_id"] = cycle_id

    return model


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_thesis(name: str, data: dict, cycle_id: str) -> dict:
    """Create a thesis dict from update data."""
    return {
        "id": f"thesis_{int(time.time())}_{name[:16].replace(' ', '_').lower()}",
        "name": name,
        "state": data.get("state", "active"),
        "confidence": data.get("confidence", 0.5),
        "direction": data.get("direction", "stable"),
        "created_cycle": cycle_id,
        "last_updated_cycle": cycle_id,
        "history": [{
            "cycle_id": cycle_id,
            "confidence": data.get("confidence", 0.5),
            "note": data.get("note", "Initial creation"),
        }],
        "falsification_criteria": data.get("falsification_criteria", ""),
        "description": data.get("description", ""),
    }
