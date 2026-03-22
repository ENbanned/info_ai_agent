"""Ingest worker: batches messages and writes to mem0."""

import asyncio
import time
from datetime import datetime, timezone

from loguru import logger

from src.pipeline.classifier import classify
from src.pipeline.preprocessor import ProcessedMessage
from src.tg.bot import send_alert

BATCH_SIZE = 8
BATCH_TIMEOUT = 60  # seconds
MAX_CONCURRENT_CLASSIFY = 5


async def ingest_worker(queue: asyncio.Queue, memory, bot) -> None:
    """Background task: consume messages from queue, classify, batch, and ingest."""
    buffer: list[ProcessedMessage] = []
    first_buffered_at: float | None = None
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_CLASSIFY)

    async def _classify_and_route(msg: ProcessedMessage) -> None:
        nonlocal first_buffered_at
        async with semaphore:
            classification = await classify(msg)

        if classification == "NOISE":
            return

        if classification == "URGENT":
            logger.warning(f"⚡ URGENT → alert + mem0 │ {msg.channel_name}")
            results = await asyncio.gather(
                _ingest_urgent(msg, memory),
                send_alert(bot, msg.text, msg.channel_name),
                return_exceptions=True,
            )
            for r in results:
                if isinstance(r, Exception):
                    logger.error(f"Urgent handler error: {r}")
            return

        # RELEVANT → buffer
        buffer.append(msg)
        if first_buffered_at is None:
            first_buffered_at = time.monotonic()
        logger.debug(f"Buffer ({len(buffer)}/{BATCH_SIZE}) │ {msg.channel_name}")

    pending_tasks: set[asyncio.Task] = set()

    while True:
        # Always use a timeout so we can check buffer for flush
        timeout = BATCH_TIMEOUT
        if buffer and first_buffered_at is not None:
            elapsed = time.monotonic() - first_buffered_at
            timeout = max(0.1, BATCH_TIMEOUT - elapsed)

        try:
            msg: ProcessedMessage = await asyncio.wait_for(queue.get(), timeout=timeout)
        except (asyncio.TimeoutError, TimeoutError):
            if buffer:
                await _flush_batch(buffer, memory)
                buffer.clear()
                first_buffered_at = None
            continue

        # Classify concurrently but check buffer after
        task = asyncio.create_task(_classify_and_route(msg))
        pending_tasks.add(task)
        task.add_done_callback(pending_tasks.discard)

        # Give tasks a moment to complete and fill buffer
        await asyncio.sleep(0)

        if len(buffer) >= BATCH_SIZE:
            await _flush_batch(buffer, memory)
            buffer.clear()
            first_buffered_at = None


async def _flush_batch(batch: list[ProcessedMessage], memory) -> None:
    """Combine batch of messages and add to mem0."""
    channels = list({msg.channel_name for msg in batch})
    logger.info(f"📦 Batch ({len(batch)} msgs) │ {', '.join(channels)}")

    # Show what's going into the batch
    for msg in batch:
        text_clean = msg.text.replace("\n", " ")
        logger.debug(f"   ├─ {msg.channel_name} │ {text_clean}")

    combined = "\n---\n".join(
        f"[{msg.channel_name} | {datetime.fromtimestamp(msg.timestamp, tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}]\n{msg.text}"
        for msg in batch
    )

    media_paths = [msg.media_path for msg in batch if msg.media_path]

    metadata = {
        "source": "telegram_batch",
        "channels": channels,
        "batch_size": len(batch),
        "timestamp": max(msg.timestamp for msg in batch),
        "lifecycle_state": "active",
        "ingested_at": int(time.time()),
    }
    if media_paths:
        metadata["media_paths"] = media_paths

    try:
        result = await memory.add(combined, user_id="trader", metadata=metadata)
        _log_extraction_result(result)
    except Exception as e:
        logger.error(f"Batch ingest failed: {e}")


async def _ingest_urgent(msg: ProcessedMessage, memory) -> None:
    """Add a single urgent message to mem0."""
    # Dedup detection: check if a very similar message was ingested recently
    try:
        dedup_results = await memory.search(
            msg.text[:200], user_id="trader", limit=3
        )
        for mem in dedup_results.get("results", []):
            score = mem.get("score", 0)
            mem_ts = mem.get("metadata", {}).get("timestamp", 0)
            age_seconds = int(time.time()) - mem_ts if mem_ts else float("inf")
            if score > 0.95 and age_seconds < 1800:
                logger.info(
                    f"🔁 Dedup skip │ score={score:.3f} age={age_seconds}s │ {msg.channel_name}"
                )
                return
    except Exception as e:
        # Dedup failure must not block the pipeline
        logger.debug(f"Dedup check failed (continuing): {e}")

    metadata = {
        "source": "telegram",
        "channel": msg.channel_name,
        "urgency": "urgent",
        "timestamp": msg.timestamp,
        "lifecycle_state": "active",
        "ingested_at": int(time.time()),
    }
    if msg.media_path:
        metadata["media_path"] = msg.media_path

    try:
        ts_str = datetime.fromtimestamp(msg.timestamp, tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
        result = await memory.add(
            f"[URGENT | {msg.channel_name} | {ts_str}]\n{msg.text}",
            user_id="trader",
            metadata=metadata,
        )
        _log_extraction_result(result)
    except Exception as e:
        logger.error(f"Urgent ingest failed: {e}")


def _log_extraction_result(result: dict) -> None:
    """Log what mem0 extracted — facts, entities, relationships."""
    if not result:
        return

    # Vector store results (facts)
    memories = result.get("results", [])
    relations = result.get("relations", [])

    fact_count = len(memories)
    rel_count = len(relations) if isinstance(relations, list) else 0

    logger.info(f"🧠 Extracted │ {fact_count} facts, {rel_count} relationships")

    # Show individual facts
    for mem in memories[:10]:
        text = mem.get("memory", mem.get("data", mem.get("text", "")))
        lifecycle = mem.get("metadata", {}).get("lifecycle_state", "")
        state_tag = f" [{lifecycle}]" if lifecycle else ""
        if text:
            logger.info(f"   fact │ • {text[:150]}{state_tag}")

    # Show graph relationships
    if isinstance(relations, list):
        for rel in relations[:10]:
            if isinstance(rel, dict):
                src = rel.get("source", "?")
                r = rel.get("relationship", "?")
                dst = rel.get("target", rel.get("destination", "?"))
                logger.info(f"   graph │ {src} →[{r}]→ {dst}")
