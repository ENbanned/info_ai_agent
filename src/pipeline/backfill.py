import asyncio
from datetime import datetime, timezone

from loguru import logger
from pyrogram import Client

from src.channels import ChannelStore
from src.pipeline.classifier import classify
from src.pipeline.ingest import _flush_batch, _ingest_urgent, BATCH_SIZE
from src.pipeline.preprocessor import ProcessedMessage, process_message

CLASSIFY_CONCURRENCY = 5
INGEST_DELAY = 3
INGEST_TIMEOUT = 180
PROGRESS_EVERY = 10


async def run_backfill(
    user: Client,
    bot: Client,
    store: ChannelStore,
    memory,
    memory_lock: asyncio.Lock,
    cycle_start_ts: int,
    cycle_end_ts: int,
    owner_chat_id: int,
) -> dict:
    cycle_end_dt = datetime.fromtimestamp(cycle_end_ts + 1, tz=timezone.utc)

    all_messages: list[ProcessedMessage] = []
    channels = list(store.channels.values())
    fetch_log: list[str] = []

    for ch in channels:
        try:
            msgs = await _fetch_channel(
                user, ch.id, ch.name, cycle_start_ts, cycle_end_dt, ch.topics
            )
            if msgs:
                all_messages.extend(msgs)
                fetch_log.append(f"  {ch.name}: {len(msgs)}")
            logger.info(f"Backfill fetch │ {ch.name} │ {len(msgs)} msgs")
        except Exception as e:
            logger.error(f"Backfill fetch │ {ch.name} │ {e}")
            fetch_log.append(f"  {ch.name}: error")

    if not all_messages:
        await bot.send_message(owner_chat_id, "No messages found for this cycle.")
        return _summary(0, 0, 0, 0, 0)

    detail = "\n".join(fetch_log[:40])
    await bot.send_message(
        owner_chat_id,
        f"📥 Fetched {len(all_messages)} msgs from {len(channels)} sources\n{detail}",
    )

    await bot.send_message(owner_chat_id, "🔍 Classifying...")

    sem = asyncio.Semaphore(CLASSIFY_CONCURRENCY)
    cls_results: list[str] = ["RELEVANT"] * len(all_messages)

    async def _cls(idx: int, msg: ProcessedMessage) -> None:
        async with sem:
            try:
                cls_results[idx] = await classify(msg)
            except Exception as e:
                logger.error(f"Backfill classify │ {msg.channel_name} │ {e}")

    await asyncio.gather(
        *[asyncio.create_task(_cls(i, m)) for i, m in enumerate(all_messages)]
    )

    urgent = [m for i, m in enumerate(all_messages) if cls_results[i] == "URGENT"]
    relevant = [m for i, m in enumerate(all_messages) if cls_results[i] == "RELEVANT"]
    noise = [m for i, m in enumerate(all_messages) if cls_results[i] == "NOISE"]

    await bot.send_message(
        owner_chat_id,
        f"🔍 Result: 🔴 {len(urgent)} urgent · 🟢 {len(relevant)} relevant · ⚫ {len(noise)} noise",
    )

    to_ingest = len(urgent) + len(relevant)
    if to_ingest == 0:
        await bot.send_message(owner_chat_id, "All noise — nothing to ingest.")
        return _summary(len(all_messages), 0, 0, len(noise), 0)

    total_ops = len(urgent) + (len(relevant) + BATCH_SIZE - 1) // BATCH_SIZE
    await bot.send_message(owner_chat_id, f"💾 Ingesting {to_ingest} messages ({total_ops} ops)...")

    ingested = 0
    failed = 0
    op = 0

    for msg in urgent:
        op += 1
        try:
            await asyncio.wait_for(
                _ingest_urgent(msg, memory, memory_lock),
                timeout=INGEST_TIMEOUT,
            )
            ingested += 1
        except asyncio.TimeoutError:
            failed += 1
            logger.error(f"Backfill timeout │ urgent │ {msg.channel_name}")
        except Exception as e:
            failed += 1
            logger.error(f"Backfill ingest urgent │ {msg.channel_name} │ {e}")
        if op % PROGRESS_EVERY == 0:
            await bot.send_message(
                owner_chat_id, f"💾 {op}/{total_ops} ops ({ingested} ok, {failed} fail)"
            )
        await asyncio.sleep(INGEST_DELAY)

    for i in range(0, len(relevant), BATCH_SIZE):
        batch = relevant[i : i + BATCH_SIZE]
        op += 1
        try:
            await asyncio.wait_for(
                _flush_batch(batch, memory, memory_lock),
                timeout=INGEST_TIMEOUT,
            )
            ingested += len(batch)
        except asyncio.TimeoutError:
            failed += len(batch)
            logger.error(f"Backfill timeout │ batch {op}")
        except Exception as e:
            failed += len(batch)
            logger.error(f"Backfill ingest batch │ {e}")
        if op % PROGRESS_EVERY == 0:
            await bot.send_message(
                owner_chat_id, f"💾 {op}/{total_ops} ops ({ingested} ok, {failed} fail)"
            )
        await asyncio.sleep(INGEST_DELAY)

    return _summary(len(all_messages), len(urgent), len(relevant), len(noise), ingested)


async def _fetch_channel(
    user: Client,
    channel_id: int,
    channel_name: str,
    cycle_start_ts: int,
    cycle_end_dt: datetime,
    topics: dict[str, int],
) -> list[ProcessedMessage]:
    messages: list[ProcessedMessage] = []
    tracked = {v: k for k, v in topics.items()} if topics else None

    async for msg in user.get_chat_history(channel_id, offset_date=cycle_end_dt):
        if msg.date is None:
            continue
        if int(msg.date.timestamp()) < cycle_start_ts:
            break

        if tracked is not None:
            tid = getattr(msg, "message_thread_id", None)
            if tid not in tracked:
                continue
            topic_name = tracked[tid]
        else:
            topic_name = None

        processed = await process_message(msg, channel_name, topic_name)
        if processed:
            messages.append(processed)

    return messages


def _summary(fetched: int, urgent: int, relevant: int, noise: int, ingested: int) -> dict:
    return {
        "fetched": fetched,
        "urgent": urgent,
        "relevant": relevant,
        "noise": noise,
        "ingested": ingested,
    }
