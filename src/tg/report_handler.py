import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path

from loguru import logger
from pyrogram import Client, filters
from pyrogram.types import Message

from src.config import BOT_CONFIG
from src.tg.bot import send_report, send_report_file
from src.analyst.analyst import run_cycle

OWNER = BOT_CONFIG["owner_chat_id"]
MSK = timezone(timedelta(hours=3))
CYCLE_HOURS = {0, 6, 12, 18}
_REPORTS_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "reports"


def register_report_handler(bot: Client, memory, memory_lock: asyncio.Lock) -> None:

    @bot.on_message(filters.command("report") & filters.user(OWNER) & filters.private)
    async def handle_report(_client: Client, message: Message) -> None:
        args = message.text.split(maxsplit=1)
        if len(args) < 2:
            await message.reply_text(
                "Usage: /report DD.MM.YYYY HH:MM\n"
                "Example: /report 26.03.2026 00:00"
            )
            return

        raw = args[1].strip()
        try:
            cycle_end_msk = datetime.strptime(raw, "%d.%m.%Y %H:%M").replace(tzinfo=MSK)
        except ValueError:
            await message.reply_text("Invalid format. Use: /report DD.MM.YYYY HH:MM")
            return

        if cycle_end_msk.hour not in CYCLE_HOURS or cycle_end_msk.minute != 0:
            await message.reply_text(
                f"Invalid cycle time. Valid hours: {sorted(CYCLE_HOURS)} (XX:00)"
            )
            return

        now = datetime.now(tz=MSK)
        if cycle_end_msk > now + timedelta(hours=1):
            await message.reply_text("This cycle hasn't happened yet.")
            return

        date_dir = cycle_end_msk.strftime("%Y-%m-%d")
        time_dir = cycle_end_msk.strftime("%H-%M")
        docx_path = _REPORTS_DIR / date_dir / time_dir / "report.docx"
        cycle_label = cycle_end_msk.strftime("%Y-%m-%d %H:%M MSK")

        if docx_path.exists():
            await message.reply_text(f"Found existing report, resending...")
            await send_report_file(bot, str(docx_path), cycle_label)
            return

        await message.reply_text(f"Report not found, generating for {cycle_label}...")

        cycle_end_ts = int(cycle_end_msk.timestamp())
        cycle_start_ts = cycle_end_ts - 6 * 3600

        try:
            report, docx_out, qa_supplement = await run_cycle(
                memory, cycle_start_ts, cycle_end_ts, memory_lock=memory_lock
            )
        except Exception as e:
            logger.error(f"/report generation failed: {e}")
            await message.reply_text(f"Generation failed: {e}")
            return

        if not report:
            await message.reply_text("Empty report generated.")
            return

        if docx_out:
            await send_report_file(bot, docx_out, cycle_label)
        else:
            await send_report(bot, report)

        await message.reply_text("Done")
