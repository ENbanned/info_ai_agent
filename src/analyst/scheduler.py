"""Scheduler: runs the analyst at fixed Moscow time slots (00:00, 06:00, 12:00, 18:00)."""

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path

from loguru import logger
from pyrogram import Client

from src.analyst.analyst import run_cycle
from src.config import BOT_CONFIG
from src.tg.bot import send_alert, send_report, send_report_file

MSK = timezone(timedelta(hours=3))
CYCLE_HOURS = [0, 6, 12, 18]
_REPORTS_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "reports"


def _next_cycle_time() -> datetime:
    """Calculate the next fixed cycle time in MSK."""
    now = datetime.now(tz=MSK)
    for hour in CYCLE_HOURS:
        candidate = now.replace(hour=hour, minute=0, second=0, microsecond=0)
        if candidate > now:
            return candidate
    tomorrow = now + timedelta(days=1)
    return tomorrow.replace(hour=0, minute=0, second=0, microsecond=0)


def _prev_cycle_time(current: datetime) -> datetime:
    """Get the start of the current cycle (6h before the next one)."""
    return current - timedelta(hours=6)


def _get_expected_cycles_since(system_start_ts: float) -> set[str]:
    """Get set of expected cycle labels (YYYY-MM-DD/HH-MM) since system start."""
    expected = set()
    start_dt = datetime.fromtimestamp(system_start_ts, tz=MSK)
    # Round up to next cycle boundary
    for hour in CYCLE_HOURS:
        candidate = start_dt.replace(hour=hour, minute=0, second=0, microsecond=0)
        if candidate >= start_dt:
            start_dt = candidate
            break
    else:
        start_dt = (start_dt + timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )

    now = datetime.now(tz=MSK)
    current = start_dt
    while current < now:
        label = current.strftime("%Y-%m-%d/%H-%M")
        expected.add(label)
        # Find next cycle time
        found_next = False
        for hour in CYCLE_HOURS:
            candidate = current.replace(hour=hour, minute=0, second=0, microsecond=0)
            if candidate > current:
                current = candidate
                found_next = True
                break
        if not found_next:
            current = (current + timedelta(days=1)).replace(
                hour=CYCLE_HOURS[0], minute=0, second=0, microsecond=0
            )

    return expected


def _get_existing_reports() -> set[str]:
    """Get set of existing report labels (YYYY-MM-DD/HH-MM) from reports dir."""
    existing = set()
    if not _REPORTS_DIR.exists():
        return existing

    for date_dir in _REPORTS_DIR.iterdir():
        if not date_dir.is_dir():
            continue
        for time_dir in date_dir.iterdir():
            if not time_dir.is_dir():
                continue
            # Check if report.docx exists in this directory
            if (time_dir / "report.docx").exists():
                label = f"{date_dir.name}/{time_dir.name}"
                existing.add(label)

    return existing


async def analyst_loop(memory, bot: Client, system_start_ts: float, memory_lock=None) -> None:
    """Run the analyst at fixed MSK time slots: 00:00, 06:00, 12:00, 18:00."""
    logger.info(f"Analyst scheduler | MSK slots: {CYCLE_HOURS}")

    # ------------------------------------------------------------------
    # Missed cycle detection on startup
    # ------------------------------------------------------------------
    try:
        expected = _get_expected_cycles_since(system_start_ts)
        existing = _get_existing_reports()
        missed = expected - existing
        if missed:
            missed_sorted = sorted(missed)
            logger.warning(
                f"Detected {len(missed)} missed cycle(s) since system start: "
                f"{missed_sorted[:10]}{'...' if len(missed_sorted) > 10 else ''}"
            )
        else:
            logger.info("No missed cycles detected since system start")
    except Exception as e:
        logger.warning(f"Missed cycle detection failed: {e}")

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------
    while True:
        next_time = _next_cycle_time()
        now = datetime.now(tz=MSK)
        wait_seconds = (next_time - now).total_seconds()
        wait_min = wait_seconds / 60

        logger.info(f"Next analyst | {next_time.strftime('%H:%M')} MSK (in {wait_min:.0f} min)")
        await asyncio.sleep(wait_seconds)

        cycle_start_ts = int(_prev_cycle_time(next_time).timestamp())
        cycle_end_ts = int(next_time.timestamp())
        cycle_label = next_time.strftime("%Y-%m-%d %H:%M MSK")
        cycle_id = next_time.strftime("%Y%m%d_%H%M")
        logger.info(f"Analyst cycle starting | {cycle_label}")

        # ---- Retry with exponential backoff (3 attempts) ----
        report = None
        docx_path = None
        qa_supplement = None
        for attempt in range(3):
            try:
                report, docx_path, qa_supplement = await run_cycle(memory, cycle_start_ts, cycle_end_ts, memory_lock=memory_lock)
                break  # success
            except Exception as e:
                if attempt < 2:
                    wait = 60 * (2 ** attempt)  # 60s, 120s
                    logger.warning(
                        f"Analyst cycle failed (attempt {attempt + 1}/3), "
                        f"retrying in {wait}s: {e}"
                    )
                    await asyncio.sleep(wait)
                else:
                    logger.error(f"Analyst cycle failed after 3 attempts: {e}")
                    try:
                        await send_alert(
                            bot,
                            f"Analyst cycle {cycle_id} FAILED after 3 attempts: {e}",
                            "SYSTEM",
                        )
                    except Exception:
                        logger.error("Failed to send failure notification")

        # ---- Send report if we got one ----
        if report:
            try:
                if docx_path:
                    await send_report_file(bot, docx_path, cycle_label)
                else:
                    # Fallback: send text if docx failed
                    await send_report(bot, report)
                logger.success("Analyst cycle done | report sent")

                # QA supplement logged but NOT sent to Telegram —
                # URGENT facts are now natively in the analyst prompt
                if qa_supplement:
                    logger.info(f"QA supplement generated ({len(qa_supplement)} chars) — logged only, not sent")
            except Exception as e:
                logger.error(f"Failed to send report: {e}")
                try:
                    await bot.send_message(
                        BOT_CONFIG["owner_chat_id"],
                        f"**Analyst cycle report generated but sending failed:**\n```\n{e}\n```",
                    )
                except Exception:
                    logger.error("Failed to send error notification")

        await asyncio.sleep(60)
