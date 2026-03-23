"""Opus analyst: produces 6-hour intelligence digest."""

import asyncio
import os
import re
import time
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from xml.etree import ElementTree

from loguru import logger
from claude_agent_sdk import query, ClaudeAgentOptions, ResultMessage, AssistantMessage, TextBlock

from src.config import MODELS_CONFIG
from src.pipeline.prompts import ANALYST_SYSTEM_PROMPT, ANALYST_EXTRACTION_PROMPT
from src.analyst.memory_tools import create_memory_server
from src.analyst.world_model import (
    load_world_model,
    save_world_model,
    format_world_model_for_prompt,
    parse_world_model_update,
    apply_world_model_update,
    strip_world_model_block,
    WORLD_MODEL_UPDATE_INSTRUCTION,
)

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_CWD = str(_PROJECT_ROOT / "data" / "analyst_workdir")
_REPORTS_DIR = _PROJECT_ROOT / "data" / "reports"
_SKILLS_DIR = _PROJECT_ROOT / "data" / "skills"
os.makedirs(_CWD, exist_ok=True)

# ---------------------------------------------------------------------------
# Token budget constants
# ---------------------------------------------------------------------------
MAX_PROMPT_TOKENS = 80_000


def _estimate_tokens(text: str) -> int:
    """Rough token estimate: 1 token ~ 4 chars for mixed en/ru text."""
    return len(text) // 4


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

async def run_cycle(memory, cycle_start_ts: int, cycle_end_ts: int | None = None, memory_lock=None) -> tuple[str, str | None, str | None]:
    """Run a single analyst cycle: pull data, analyze, return report.

    cycle_start_ts: start of the 6h window (UTC timestamp)
    cycle_end_ts: end of the 6h window (UTC timestamp). If None, uses current time.

    Returns (report_text, docx_path_or_None, qa_supplement_or_None).
    """
    if cycle_end_ts is None:
        cycle_end_ts = int(time.time())

    start_time_str = datetime.fromtimestamp(cycle_start_ts, tz=timezone.utc).strftime("%H:%M UTC")
    end_time_str = datetime.fromtimestamp(cycle_end_ts, tz=timezone.utc).strftime("%H:%M UTC")
    logger.info(f"Analyst cycle | {start_time_str} -> {end_time_str}")

    cycle_id = datetime.fromtimestamp(cycle_start_ts, tz=timezone.utc).strftime("%Y%m%d_%H%M")

    # ------------------------------------------------------------------
    # 1. Pull facts for this 6h window — lifecycle-aware split
    # ------------------------------------------------------------------
    # NEW facts: created in this 6h window, not yet reported
    new_facts = await memory.get_by_lifecycle(
        user_id="trader",
        state=["active"],
        time_filter={"gte": cycle_start_ts, "lte": cycle_end_ts},
        limit=500,
    )
    # REPORTED facts: from the PREVIOUS 24h (not current window).
    # These were reported in earlier cycles — provide as compact context.
    # We use a 24h lookback so the analyst sees recent history.
    reported_lookback = cycle_start_ts - 24 * 3600
    reported_facts = await memory.get_by_lifecycle(
        user_id="trader",
        state=["reported"],
        time_filter={"gte": reported_lookback, "lte": cycle_start_ts},
        limit=100,
    )
    logger.info(
        f"   Facts | {len(new_facts)} NEW (active), "
        f"{len(reported_facts)} REPORTED (24h lookback)"
    )

    # ------------------------------------------------------------------
    # 2. Graph: top entities (replaces get_all full scan)
    # ------------------------------------------------------------------
    try:
        top_entities = await asyncio.to_thread(
            memory.graph.get_top_entities,
            filters={"user_id": "trader"}, limit=30, min_mentions=3
        )
    except Exception as e:
        logger.warning(f"   Graph | get_top_entities failed: {e}")
        top_entities = []
    logger.info(f"   Graph | {len(top_entities)} top entities")

    # ------------------------------------------------------------------
    # 3. Previous conclusions — full 24h + semantic older + adaptive
    # ------------------------------------------------------------------
    previous = await _fetch_previous_conclusions(
        memory, cycle_start_ts, cycle_facts=new_facts
    )
    logger.info(
        f"   Previous conclusions | {len(previous['recent'])} recent (24h), "
        f"{len(previous['older'])} older"
    )

    # ------------------------------------------------------------------
    # 4. Build prompt (lifecycle-aware, token-budgeted)
    # ------------------------------------------------------------------
    prompt = _build_analyst_prompt(
        new_facts, reported_facts, top_entities, previous,
        cycle_start_ts, cycle_end_ts,
    )
    prompt_tokens = _estimate_tokens(prompt)
    logger.info(f"   Prompt | {len(prompt)} chars, ~{prompt_tokens} tokens -> Opus")

    # ------------------------------------------------------------------
    # 5. Prepare report output path (named by cycle end time in MSK)
    # ------------------------------------------------------------------
    from datetime import timedelta
    msk = timezone(timedelta(hours=3))
    end_msk = datetime.fromtimestamp(cycle_end_ts, tz=msk)
    date_dir = end_msk.strftime("%Y-%m-%d")
    time_dir = end_msk.strftime("%H-%M")
    report_dir = _REPORTS_DIR / date_dir / time_dir
    report_dir.mkdir(parents=True, exist_ok=True)
    docx_path = str(report_dir / "report.docx")

    # ------------------------------------------------------------------
    # 5b. Run Opus with MCP memory tools
    # ------------------------------------------------------------------
    logger.info("Opus analyzing | thinking + web research + memory search...")
    report = await _run_opus(prompt, memory, docx_path)
    logger.success(f"Opus done | {len(report)} chars report")

    # Show first few lines of report
    for line in report.split("\n")[:5]:
        if line.strip():
            logger.info(f"   report | {line.strip()[:120]}")

    # ------------------------------------------------------------------
    # 5c. Update world model from analyst output
    # ------------------------------------------------------------------
    try:
        wm = load_world_model()
        wm_update = parse_world_model_update(report)
        if wm_update:
            wm = apply_world_model_update(wm, wm_update, cycle_id)
            save_world_model(wm)
            logger.success(
                f"World model updated | regime={wm.get('market_regime', {}).get('current', '?')}, "
                f"{len(wm.get('active_theses', []))} theses, "
                f"{len(wm.get('active_narratives', []))} narratives"
            )
        else:
            logger.warning("World model | no update block found in report, model unchanged")
    except Exception as e:
        logger.error(f"World model update failed (non-fatal): {e}")

    # Strip the <world_model_update> block from report before sending to user
    report = strip_world_model_block(report)

    # ------------------------------------------------------------------
    # 6. Check if .docx was created
    # ------------------------------------------------------------------
    if os.path.exists(docx_path):
        logger.success(f"DOCX report | {docx_path}")
    else:
        logger.warning(f"DOCX not created | {docx_path}")
        docx_path = None

    # ------------------------------------------------------------------
    # 6b. QA validation (completeness + freshness)
    # ------------------------------------------------------------------
    qa_supplement: str | None = None
    try:
        from src.analyst.qa import validate_report

        # Collect URGENT facts for completeness check
        # ingest.py stores urgency as "urgent" (lowercase) in metadata
        urgent_facts = [
            f for f in new_facts
            if f.get("metadata", {}).get("urgency", "").lower() == "urgent"
            or f.get("urgency", "").lower() == "urgent"
        ]

        # Read previous report text for freshness check
        prev_report_text = _read_previous_report(cycle_end_ts)

        qa_result = await validate_report(
            report_text=report,
            cycle_id=cycle_id,
            memory=memory,
            previous_report=prev_report_text,
            urgent_facts=urgent_facts,
        )

        for w in qa_result.warnings:
            logger.warning(f"QA | {w}")

        if qa_result.supplement:
            qa_supplement = qa_result.supplement
            logger.info(
                f"QA | supplement generated "
                f"({len(qa_result.completeness.get('missing', []))} missing URGENT facts)"
            )
    except Exception as e:
        logger.error(f"QA validation failed (report will be sent as-is): {e}")

    # ------------------------------------------------------------------
    # 7. Save conclusions back to mem0 using ANALYST_EXTRACTION_PROMPT
    # ------------------------------------------------------------------
    logger.info(f"Saving analyst conclusions | cycle {cycle_id}")
    await _save_conclusions(memory, report, cycle_id, memory_lock)

    # ------------------------------------------------------------------
    # 8. Mark facts as reported (AFTER successful report generation)
    # ------------------------------------------------------------------
    report_is_valid = len(report) > 500 and "Analyst cycle failed" not in report
    all_fact_ids = [f["id"] for f in new_facts + reported_facts if "id" in f]
    if all_fact_ids and report_is_valid:
        try:
            updated = await memory.mark_as_reported(all_fact_ids, cycle_id)
            logger.info(f"Marked {updated}/{len(all_fact_ids)} facts as reported for cycle {cycle_id}")
        except Exception as e:
            logger.error(f"Failed to mark facts as reported: {e}")
    elif all_fact_ids and not report_is_valid:
        logger.warning(f"Skipping mark_as_reported: report too short or failed ({len(report)} chars)")

    return report, docx_path, qa_supplement


# ---------------------------------------------------------------------------
# Read previous report (for QA freshness check)
# ---------------------------------------------------------------------------

def _read_previous_report(cycle_end_ts: int) -> str | None:
    """Read the plain text of the most recent previous .docx report.

    Looks in /data/reports/ for the latest report directory BEFORE
    the current cycle_end_ts.  Extracts text from the .docx via zipfile+XML.
    Returns None if no previous report is found or reading fails.
    """
    msk = timezone(timedelta(hours=3))
    current_dt = datetime.fromtimestamp(cycle_end_ts, tz=msk)

    # Collect all report.docx paths with their datetime
    candidates: list[tuple[datetime, Path]] = []
    if not _REPORTS_DIR.exists():
        return None

    for date_dir in _REPORTS_DIR.iterdir():
        if not date_dir.is_dir():
            continue
        for time_dir in date_dir.iterdir():
            if not time_dir.is_dir():
                continue
            docx_path = time_dir / "report.docx"
            if not docx_path.exists():
                continue
            try:
                dt = datetime.strptime(
                    f"{date_dir.name} {time_dir.name}",
                    "%Y-%m-%d %H-%M",
                ).replace(tzinfo=msk)
                if dt < current_dt:
                    candidates.append((dt, docx_path))
            except ValueError:
                continue

    if not candidates:
        return None

    # Pick the most recent one
    candidates.sort(key=lambda x: x[0], reverse=True)
    _, prev_docx = candidates[0]

    return _extract_text_from_docx(prev_docx)


def _extract_text_from_docx(docx_path: Path) -> str | None:
    """Extract plain text from a .docx file using zipfile + XML parsing."""
    try:
        with zipfile.ZipFile(docx_path, "r") as zf:
            with zf.open("word/document.xml") as xml_file:
                tree = ElementTree.parse(xml_file)
        root = tree.getroot()
        # Word namespace
        ns = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
        paragraphs: list[str] = []
        for p in root.iter(f"{{{ns['w']}}}p"):
            texts = [
                node.text
                for node in p.iter(f"{{{ns['w']}}}t")
                if node.text
            ]
            if texts:
                paragraphs.append("".join(texts))
        return "\n".join(paragraphs)
    except Exception as e:
        logger.warning(f"Failed to read previous report {docx_path}: {e}")
        return None


# ---------------------------------------------------------------------------
# Previous conclusions retrieval (with adaptive queries)
# ---------------------------------------------------------------------------

async def _fetch_previous_conclusions(
    memory,
    cycle_start_ts: int,
    cycle_facts: list | None = None,
) -> dict[str, list[str]]:
    """Fetch previous analyst conclusions.

    Recent (last 24h): ALL conclusions -- full context for continuity.
    Older (>24h): semantic search for key themes -- long-term memory.
    Adaptive: dynamic queries extracted from current cycle facts.
    """
    recent = []
    older = []

    # 1. Last 24h -- every conclusion, no losses
    ts_24h_ago = cycle_start_ts - 24 * 3600
    try:
        all_recent = await memory.get_all(
            user_id="trader", agent_id="analyst",
            filters={"timestamp": {"gte": ts_24h_ago}},
            limit=None,
        )
        for mem in all_recent.get("results", []):
            text = mem.get("memory", mem.get("data", ""))
            if text:
                recent.append(text)
    except Exception as e:
        logger.warning(f"Failed to fetch recent analyst conclusions: {e}")

    # 2. Older than 24h -- semantic search for strategic continuity
    seen = set(recent)
    queries = [
        "thesis prediction confirmed invalidated",
        "narrative trend momentum rotation",
        "risk scenario warning tail",
    ]
    for q in queries:
        try:
            search_result = await memory.search(
                q, user_id="trader", agent_id="analyst", limit=10
            )
            for mem in search_result.get("results", []):
                text = mem.get("memory", mem.get("data", ""))
                ts = mem.get("metadata", {}).get("timestamp", 0)
                if text and text not in seen and ts < ts_24h_ago:
                    seen.add(text)
                    older.append(mem)
        except Exception as e:
            logger.warning(f"Older conclusions search failed for '{q}': {e}")

    # 3. Adaptive queries from current cycle facts
    if cycle_facts:
        dynamic_queries = _extract_key_topics(cycle_facts)
        for q in dynamic_queries[:5]:
            try:
                result = await memory.search(
                    q, user_id="trader", agent_id="analyst", limit=5
                )
                for item in result.get("results", []):
                    item_id = item.get("id", "")
                    text = item.get("memory", item.get("data", ""))
                    ts = item.get("metadata", {}).get("timestamp", 0)
                    if text and text not in seen and ts < ts_24h_ago:
                        seen.add(text)
                        older.append(item)
            except Exception as e:
                logger.warning(f"Adaptive conclusions search failed for '{q}': {e}")

    # Convert older items to text if they are dicts
    older_texts = []
    for item in older:
        if isinstance(item, dict):
            older_texts.append(item.get("memory", item.get("data", "")))
        else:
            older_texts.append(str(item))

    return {"recent": recent, "older": older_texts}


def _extract_key_topics(cycle_facts: list) -> list[str]:
    """Extract key topics from current cycle facts for dynamic memory queries.

    Simple heuristic: find $TICKER patterns, capitalized named entities,
    and key crypto-related phrases. No LLM needed.
    """
    sample_texts = [
        f.get("memory", f.get("data", ""))[:200]
        for f in cycle_facts[:20]
    ]
    combined = " ".join(sample_texts)

    topics = []

    # 1. Find $TICKER patterns (e.g., $BTC, $ETH, $SOL)
    tickers = re.findall(r"\$([A-Z]{2,10})", combined)
    for t in set(tickers):
        topics.append(f"{t} price movement analysis")

    # 2. Find capitalized multi-word entities (e.g., "Federal Reserve", "Circle IPO")
    named_entities = re.findall(r"[A-Z][a-z]+(?:\s+[A-Z][a-z]+)+", combined)
    for ne in set(named_entities):
        if len(ne) > 5:  # filter out very short matches
            topics.append(ne)

    # 3. Find standalone all-caps words (likely tickers or acronyms)
    all_caps = re.findall(r"\b([A-Z]{3,8})\b", combined)
    # Filter common English words that happen to be caps
    noise_words = {
        "THE", "AND", "FOR", "BUT", "NOT", "ALL", "ARE", "WAS",
        "HAS", "HAD", "HIS", "HER", "ITS", "OUR", "NEW", "OLD",
        "NOW", "HOW", "WHO", "WHY", "UTC", "USD", "BREAKING",
    }
    for w in set(all_caps):
        if w not in noise_words:
            topics.append(f"{w} developments")

    # 4. Deduplicate and limit
    seen = set()
    unique_topics = []
    for t in topics:
        key = t.lower()
        if key not in seen:
            seen.add(key)
            unique_topics.append(t)

    return unique_topics[:10]


# ---------------------------------------------------------------------------
# Conclusions saving
# ---------------------------------------------------------------------------

async def _save_conclusions(memory, report: str, cycle_id: str, memory_lock=None) -> None:
    """Save analyst conclusions using ANALYST_EXTRACTION_PROMPT.

    Uses memory_lock to prevent concurrent memory.add() calls from
    seeing the temporarily swapped extraction prompt.
    """
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def _noop():
        yield

    async with memory_lock if memory_lock else _noop():
        original_prompt = memory.config.custom_fact_extraction_prompt
        memory.config.custom_fact_extraction_prompt = ANALYST_EXTRACTION_PROMPT
        try:
            await memory.add(
                f"[Analyst Cycle {cycle_id}]\n{report}",
                user_id="trader",
                agent_id="analyst",
                metadata={
                    "source": "analyst",
                    "cycle_id": cycle_id,
                    "timestamp": int(time.time()),
                },
            )
        except Exception as e:
            logger.error(f"Failed to save analyst conclusions: {e}")
        finally:
            memory.config.custom_fact_extraction_prompt = original_prompt


# ---------------------------------------------------------------------------
# Prompt building (lifecycle-aware, token-budgeted)
# ---------------------------------------------------------------------------

def _build_analyst_prompt(
    new_facts: list,
    reported_facts: list,
    top_entities: list,
    previous: dict[str, list[str]],
    cycle_start_ts: int,
    cycle_end_ts: int,
) -> str:
    """Build the prompt for Opus analyst from lifecycle-separated data."""
    start_time = datetime.fromtimestamp(cycle_start_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    end_time = datetime.fromtimestamp(cycle_end_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    # --- Split URGENT from regular NEW facts ---
    urgent_facts = [
        f for f in new_facts
        if f.get("metadata", {}).get("urgency", "").lower() == "urgent"
        or f.get("urgency", "").lower() == "urgent"
    ]
    regular_facts = [f for f in new_facts if f not in urgent_facts]

    # --- Section: URGENT facts ---
    if urgent_facts:
        urgent_section = (
            f"## ⚡ URGENT ({len(urgent_facts)} items)\n"
            f"These were classified as URGENT by the classifier LLM. "
            f"Review each one individually and decide its importance. "
            f"Include the significant ones naturally in the relevant sections of your report. "
            f"If you judge some to be low-priority, still mention them briefly in a "
            f"\"Minor Events\" section at the end — aim for 100% coverage of all intelligence, "
            f"even if briefly.\n\n"
            + _format_facts_by_channel(urgent_facts)
        )
    else:
        urgent_section = ""

    # --- Section: NEW facts by channel ---
    new_section = _format_facts_by_channel(regular_facts)

    # --- Section: REPORTED facts (compact) ---
    reported_section = _format_reported_facts_compact(reported_facts)

    # --- Section: Graph top entities ---
    graph_section = _format_graph_top_entities(top_entities)

    # --- Section: Previous conclusions ---
    recent = previous.get("recent", [])
    older = previous.get("older", [])

    if not recent and not older:
        prev_section = "No previous analyst conclusions available (first cycle)."
    else:
        parts = []
        if recent:
            parts.append(f"### Last 24 hours ({len(recent)} conclusions -- FULL context)")
            for i, c in enumerate(recent, 1):
                parts.append(f"{i}. {c}")
        if older:
            parts.append(f"\n### Older highlights ({len(older)} items -- use search_memory for more)")
            for i, c in enumerate(older, 1):
                parts.append(f"{i}. {c}")
        prev_section = "\n".join(parts)

    # --- Section: Media files ---
    media_section = ""
    media_paths = []
    for mem in new_facts:
        meta = mem.get("metadata", {})
        if meta.get("media_path"):
            media_paths.append(meta["media_path"])
        if meta.get("media_paths"):
            media_paths.extend(meta["media_paths"])
    if media_paths:
        media_section = (
            f"\n\n## Media Files\n"
            f"The following images were attached to messages. "
            f"Use the Read tool to examine any that seem relevant:\n"
            + "\n".join(f"- {p}" for p in media_paths[:20])
        )

    # --- Section: World Model ---
    world_model = load_world_model()
    world_model_section = format_world_model_for_prompt(world_model)
    logger.info(
        f"   World model | regime={world_model.get('market_regime', {}).get('current', '?')}, "
        f"{len(world_model.get('active_theses', []))} theses, "
        f"{len(world_model.get('active_narratives', []))} narratives"
    )

    # --- Assemble full prompt ---
    prompt = f"""\
Analyze the following crypto intelligence collected between {start_time} and {end_time}.

{world_model_section}

{urgent_section}

## Current Cycle Intelligence -- NEW ({len(regular_facts)} items)
{new_section}

## Current Cycle Intelligence -- PREVIOUSLY REPORTED ({len(reported_facts)} items)
These facts were already covered in previous reports. Mention ONLY if there's a new angle.
{reported_section}

## Knowledge Graph -- Top Entities
{graph_section}

## Previous Analyst Conclusions
{prev_section}{media_section}

Produce a comprehensive 6-hour digest following your system prompt instructions.

BEFORE writing your analysis:
1. Use search_memory(scope="analyst") to find your older conclusions on key topics from this cycle
2. Use query_entity for the most active entities in the knowledge graph above
3. Use search_memory(scope="facts") for any topic where you need history beyond the last 24 hours

The Previous Analyst Conclusions section above contains ONLY the last 24 hours. \
Everything older is accessible ONLY through search_memory -- you must call it to access your long-term memory.

If any claims seem significant but unverified, use WebSearch to verify them.
If media files are listed and seem relevant to key developments, use Read to examine them.
{WORLD_MODEL_UPDATE_INSTRUCTION}"""

    # --- Token budget enforcement ---
    original_tokens = _estimate_tokens(prompt)
    if original_tokens > MAX_PROMPT_TOKENS:
        logger.warning(
            f"Prompt exceeds token budget: ~{original_tokens} tokens "
            f"(max {MAX_PROMPT_TOKENS}). Truncating..."
        )
        prompt = _truncate_prompt(
            regular_facts, reported_facts, top_entities, previous,
            cycle_start_ts, cycle_end_ts, media_section,
            world_model_section, WORLD_MODEL_UPDATE_INSTRUCTION, urgent_section,
        )
        final_tokens = _estimate_tokens(prompt)
        logger.warning(f"Prompt truncated from ~{original_tokens} to ~{final_tokens} tokens")

    return prompt


def _truncate_prompt(
    new_facts: list,
    reported_facts: list,
    top_entities: list,
    previous: dict[str, list[str]],
    cycle_start_ts: int,
    cycle_end_ts: int,
    media_section: str,
    world_model_section: str = "",
    world_model_instruction: str = "",
    urgent_section: str = "",
) -> str:
    """Rebuild prompt with progressively truncated sections to fit token budget.

    Truncation priority (least important first):
    1. REPORTED facts
    2. Graph context
    3. NEW facts (last resort)
    """
    start_time = datetime.fromtimestamp(cycle_start_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    end_time = datetime.fromtimestamp(cycle_end_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    recent = previous.get("recent", [])
    older = previous.get("older", [])

    # Build previous section (keep as-is, it's bounded)
    if not recent and not older:
        prev_section = "No previous analyst conclusions available (first cycle)."
    else:
        parts = []
        if recent:
            parts.append(f"### Last 24 hours ({len(recent)} conclusions -- FULL context)")
            for i, c in enumerate(recent, 1):
                parts.append(f"{i}. {c}")
        if older:
            parts.append(f"\n### Older highlights ({len(older)} items -- use search_memory for more)")
            for i, c in enumerate(older, 1):
                parts.append(f"{i}. {c}")
        prev_section = "\n".join(parts)

    # Step 1: Try with truncated REPORTED facts (keep only first 20)
    truncated_reported = reported_facts[:20]
    reported_section = _format_reported_facts_compact(truncated_reported)
    new_section = _format_facts_by_channel(new_facts)
    graph_section = _format_graph_top_entities(top_entities)

    prompt = _assemble_prompt_text(
        start_time, end_time, new_facts, new_section,
        truncated_reported, reported_section, graph_section,
        prev_section, media_section, world_model_section, world_model_instruction, urgent_section,
    )
    if _estimate_tokens(prompt) <= MAX_PROMPT_TOKENS:
        return prompt

    # Step 2: Remove REPORTED facts entirely, truncate graph to 15 entities
    reported_section = "(Truncated to fit token budget. Use search_memory for reported facts.)"
    graph_section = _format_graph_top_entities(top_entities[:15])

    prompt = _assemble_prompt_text(
        start_time, end_time, new_facts, new_section,
        [], reported_section, graph_section,
        prev_section, media_section, world_model_section, world_model_instruction, urgent_section,
    )
    if _estimate_tokens(prompt) <= MAX_PROMPT_TOKENS:
        return prompt

    # Step 3: Truncate NEW facts to first 200
    truncated_new = new_facts[:200]
    new_section = _format_facts_by_channel(truncated_new)
    graph_section = _format_graph_top_entities(top_entities[:10])

    prompt = _assemble_prompt_text(
        start_time, end_time, truncated_new, new_section,
        [], reported_section, graph_section,
        prev_section, media_section, world_model_section, world_model_instruction, urgent_section,
    )
    return prompt


def _assemble_prompt_text(
    start_time: str,
    end_time: str,
    new_facts: list,
    new_section: str,
    reported_facts: list,
    reported_section: str,
    graph_section: str,
    prev_section: str,
    media_section: str,
    world_model_section: str = "",
    world_model_instruction: str = "",
    urgent_section: str = "",
) -> str:
    """Assemble the final prompt string from pre-formatted sections."""
    reported_count = len(reported_facts) if isinstance(reported_facts, list) else 0
    wm_block = f"\n{world_model_section}\n" if world_model_section else ""
    wm_instr = f"\n{world_model_instruction}" if world_model_instruction else ""
    urgent_block = f"\n{urgent_section}\n" if urgent_section else ""
    return f"""\
Analyze the following crypto intelligence collected between {start_time} and {end_time}.
{wm_block}{urgent_block}
## Current Cycle Intelligence -- NEW ({len(new_facts)} items)
{new_section}

## Current Cycle Intelligence -- PREVIOUSLY REPORTED ({reported_count} items)
These facts were already covered in previous reports. Mention ONLY if there's a new angle.
{reported_section}

## Knowledge Graph -- Top Entities
{graph_section}

## Previous Analyst Conclusions
{prev_section}{media_section}

Produce a comprehensive 6-hour digest following your system prompt instructions.

BEFORE writing your analysis:
1. Use search_memory(scope="analyst") to find your older conclusions on key topics from this cycle
2. Use query_entity for the most active entities in the knowledge graph above
3. Use search_memory(scope="facts") for any topic where you need history beyond the last 24 hours

The Previous Analyst Conclusions section above contains ONLY the last 24 hours. \
Everything older is accessible ONLY through search_memory -- you must call it to access your long-term memory.

If any claims seem significant but unverified, use WebSearch to verify them.
If media files are listed and seem relevant to key developments, use Read to examine them.{wm_instr}"""


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def _format_facts_by_channel(results: list[dict]) -> str:
    """Group facts by source channel for better analyst readability."""
    if not results:
        return "No intelligence collected this cycle."

    # Group by channel
    by_channel: dict[str, list[str]] = {}
    uncategorized = []

    for mem in results:
        text = mem.get("memory", mem.get("data", ""))
        if not text:
            continue
        meta = mem.get("metadata", {})
        channels = meta.get("channels", [])
        channel = meta.get("channel", "")

        if channels:
            key = ", ".join(channels[:3])
        elif channel:
            key = channel
        else:
            uncategorized.append(text)
            continue

        by_channel.setdefault(key, []).append(text)

    lines = []
    for ch, facts in sorted(by_channel.items()):
        lines.append(f"### {ch}")
        for i, fact in enumerate(facts, 1):
            lines.append(f"{i}. {fact}")
        lines.append("")

    if uncategorized:
        lines.append("### Other")
        for i, fact in enumerate(uncategorized, 1):
            lines.append(f"{i}. {fact}")

    return "\n".join(lines)


def _format_reported_facts_compact(results: list) -> str:
    """Format reported facts as one-liners for background context."""
    if not results:
        return "(none)"

    lines = []
    for r in results:
        text = r.get("memory", r.get("data", ""))
        if not text:
            continue
        # Truncate to first sentence or 150 characters
        if len(text) > 150:
            # Try to cut at sentence boundary
            cut = text[:150].rsplit(".", 1)
            short = cut[0] + "." if len(cut) > 1 and len(cut[0]) > 30 else text[:150] + "..."
        else:
            short = text

        meta = r.get("metadata", {})
        times = meta.get("times_reported", r.get("times_reported", 0))
        lines.append(f"  [REPORTED x{times}] {short}")

    return "\n".join(lines) or "(none)"


def _format_graph_top_entities(top_entities: list) -> str:
    """Format top entities from graph.get_top_entities() output.

    Input: list of dicts with keys:
        name, entity_type, mentions, lifecycle_state,
        relationships: [{source, relationship, target}]
    """
    if not top_entities:
        return "No graph data available."

    lines = []
    for entity in top_entities:
        name = entity.get("name", "?")
        etype = entity.get("entity_type", "")
        mentions = entity.get("mentions", 0)
        state = entity.get("lifecycle_state", "active")

        type_str = f" [{etype}]" if etype else ""
        state_str = f" ({state})" if state != "active" else ""
        lines.append(f"**{name}**{type_str} -- {mentions} mentions{state_str}")

        rels = entity.get("relationships", [])
        for rel in rels[:10]:
            src = rel.get("source", "?")
            r = rel.get("relationship", "?")
            tgt = rel.get("target", "?")
            lines.append(f"  - {src} --[{r}]--> {tgt}")

        if len(rels) > 10:
            lines.append(f"  - ... and {len(rels) - 10} more relationships")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Opus execution
# ---------------------------------------------------------------------------

async def _run_opus(prompt: str, memory, docx_path: str) -> str:
    """Run Opus with web search, Read tool, MCP memory tools, Bash/Write for docx."""
    # SDK closes stdin after this timeout when MCP servers are present.
    # Default 60s is too short -- Opus needs 20-30 min. Set to 40 min.
    os.environ["CLAUDE_CODE_STREAM_CLOSE_TIMEOUT"] = "2400000"

    mcp_server = create_memory_server(memory)

    # Load docx skill for system prompt
    docx_skill = ""
    docx_skill_path = _SKILLS_DIR / "docx_create.md"
    if docx_skill_path.exists():
        docx_skill = f"\n\n## DOCX Creation Reference\n\n{docx_skill_path.read_text()}"

    system_prompt = ANALYST_SYSTEM_PROMPT + f"""\

## Report File Output

After completing your analysis, you MUST create a .docx report file using the docx npm package.

Steps:
1. Write a Node.js script using Bash tool with a heredoc (cat << 'SCRIPT' > script.js ... SCRIPT). \
Do NOT use the Write tool for the script -- use Bash with heredoc to preserve Cyrillic encoding.
2. The script must use the `docx` package: require('docx'). It is already installed in {_CWD}/node_modules/. \
Do NOT run npm install.
3. Execute it with `cd {_CWD} && node script.js` via Bash. The script must be created in {_CWD}/ as well.
4. Save to: {docx_path}

CRITICAL: The .js script MUST contain actual Russian Cyrillic text (kiriллица), NOT transliteration. \
Write all headings, paragraphs, and table content in Russian using Cyrillic characters.

Make the document professional: use headings, tables for data, bullet lists, bold for emphasis. \
Include a header with "Crypto Intelligence Digest" and a footer with page numbers.
{docx_skill}"""

    options = ClaudeAgentOptions(
        model=MODELS_CONFIG["analyst"],
        tools=["WebSearch", "WebFetch", "Read", "Bash", "Write"],
        mcp_servers={"memory": mcp_server},
        allowed_tools=[
            "WebSearch", "WebFetch", "Read", "Bash", "Write",
            "mcp__memory__search_memory", "mcp__memory__query_entity",
            "mcp__memory__get_cycle_summary",
        ],
        max_turns=30,
        effort="high",
        permission_mode="bypassPermissions",
        system_prompt=system_prompt,
        cwd=_CWD,
        env={"CLAUDE_CODE_MAX_OUTPUT_TOKENS": "64000"},
    )

    report = ""
    try:
        async for message in query(prompt=prompt, options=options):
            if isinstance(message, ResultMessage) and message.result:
                report = message.result
            elif isinstance(message, AssistantMessage):
                for block in message.content:
                    if isinstance(block, TextBlock):
                        report = block.text
    except Exception as e:
        logger.error(f"Opus analyst failed: {e}")
        # Only overwrite if we got nothing useful before the crash
        if len(report) < 200:
            report = f"Analyst cycle failed: {e}"
        else:
            logger.warning(f"Opus crashed but report was already generated ({len(report)} chars), keeping it")

    return report
