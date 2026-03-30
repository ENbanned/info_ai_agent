import asyncio
from datetime import datetime, timedelta, timezone

from loguru import logger
from pyrogram import Client, filters
from pyrogram.types import Message

from src.channels import ChannelStore
from src.config import BOT_CONFIG
from src.pipeline.backfill import run_backfill

OWNER = BOT_CONFIG["owner_chat_id"]
MSK = timezone(timedelta(hours=3))
CYCLE_HOURS = {0, 6, 12, 18}


def register_backfill_handler(
    bot: Client,
    user: Client,
    store: ChannelStore,
    memory,
    memory_lock: asyncio.Lock,
) -> None:
    _active: dict[str, asyncio.Task] = {}

    @bot.on_message(filters.command("backfill") & filters.user(OWNER) & filters.private)
    async def handle_backfill(_client: Client, message: Message) -> None:
        args = message.text.split(maxsplit=1)
        if len(args) < 2:
            await message.reply_text(
                "Usage: /backfill DD.MM.YYYY HH:MM\n"
                "Example: /backfill 26.03.2026 00:00\n\n"
                "Valid cycle hours: 00, 06, 12, 18"
            )
            return

        raw = args[1].strip()
        try:
            cycle_end_msk = datetime.strptime(raw, "%d.%m.%Y %H:%M").replace(tzinfo=MSK)
        except ValueError:
            await message.reply_text("Invalid format. Use: /backfill DD.MM.YYYY HH:MM")
            return

        if cycle_end_msk.hour not in CYCLE_HOURS or cycle_end_msk.minute != 0:
            await message.reply_text(
                f"Invalid cycle. Valid hours: {sorted(CYCLE_HOURS)} (XX:00)"
            )
            return

        now = datetime.now(tz=MSK)
        if cycle_end_msk > now:
            await message.reply_text("This cycle hasn't ended yet.")
            return

        cycle_key = cycle_end_msk.strftime("%Y%m%d_%H%M")
        if cycle_key in _active and not _active[cycle_key].done():
            await message.reply_text("Backfill for this cycle is already running.")
            return

        cycle_end_ts = int(cycle_end_msk.timestamp())
        cycle_start_ts = cycle_end_ts - 6 * 3600
        cycle_label = cycle_end_msk.strftime("%Y-%m-%d %H:%M MSK")

        await message.reply_text(f"🔄 Backfill started: {cycle_label}")

        async def _run() -> None:
            try:
                summary = await run_backfill(
                    user, bot, store, memory, memory_lock,
                    cycle_start_ts, cycle_end_ts, OWNER,
                )
                await bot.send_message(
                    OWNER,
                    f"✅ Backfill done: {cycle_label}\n\n"
                    f"📥 Fetched: {summary['fetched']}\n"
                    f"🔴 Urgent: {summary['urgent']}\n"
                    f"🟢 Relevant: {summary['relevant']}\n"
                    f"⚫ Noise: {summary['noise']}\n"
                    f"💾 Ingested: {summary['ingested']}",
                )
                logger.success(f"Backfill done │ {cycle_label} │ {summary}")
            except Exception as e:
                logger.error(f"Backfill failed │ {cycle_label} │ {e}")
                try:
                    await bot.send_message(OWNER, f"❌ Backfill failed: {e}")
                except Exception:
                    pass

        _active[cycle_key] = asyncio.create_task(_run())
