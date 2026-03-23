from pathlib import Path

from loguru import logger
from pyrogram import Client, enums
from html import escape as _esc

from src.config import TG_CONFIG, BOT_CONFIG
from src.tg.formatter import markdown_to_telegram_html, split_html_message

SESSIONS_DIR = str(Path(__file__).resolve().parent.parent.parent / "data" / "sessions")


def get_bot_client() -> Client:
    return Client(
        name="bot_session",
        workdir=SESSIONS_DIR,
        api_id=TG_CONFIG["api_id"],
        api_hash=TG_CONFIG["api_hash"],
        bot_token=BOT_CONFIG["token"],
    )


async def send_alert(bot: Client, text: str, channel_name: str) -> None:
    html_body = markdown_to_telegram_html(text[:4000])
    formatted_html = f"<b>\U0001f6a8 URGENT</b> | {_esc(channel_name)}\n\n{html_body}"
    try:
        await bot.send_message(
            chat_id=BOT_CONFIG["owner_chat_id"],
            text=formatted_html,
            parse_mode=enums.ParseMode.HTML,
        )
        logger.success(f"📤 Alert sent │ {channel_name}")
    except Exception as e:
        logger.warning(f"📤 Alert HTML failed │ {channel_name} │ {e}, falling back to plain")
        try:
            plain = f"\U0001f6a8 URGENT | {channel_name}\n\n{text[:4000]}"
            await bot.send_message(chat_id=BOT_CONFIG["owner_chat_id"], text=plain)
            logger.success(f"📤 Alert sent (plain) │ {channel_name}")
        except Exception as e2:
            logger.error(f"📤 Alert failed │ {channel_name} │ {e2}")


async def send_report_file(bot: Client, file_path: str, cycle_label: str) -> None:
    try:
        await bot.send_document(
            chat_id=BOT_CONFIG["owner_chat_id"],
            document=file_path,
            caption=f"📊 6H DIGEST — {cycle_label}",
        )
        logger.success(f"📤 Report file sent │ {file_path}")
    except Exception as e:
        logger.error(f"📤 Report file failed │ {e}")


async def send_report(bot: Client, report: str) -> None:
    chunks = _split_message(report, max_len=4000)
    logger.info(f"📤 Sending report │ {len(report)} chars, {len(chunks)} message(s)")
    for i, chunk in enumerate(chunks):
        if len(chunks) > 1:
            header = f"**📊 6H DIGEST** ({i + 1}/{len(chunks)})\n\n"
        else:
            header = "**📊 6H DIGEST**\n\n"
        try:
            await bot.send_message(chat_id=BOT_CONFIG["owner_chat_id"], text=header + chunk)
        except Exception as e:
            logger.error(f"📤 Report chunk {i+1} failed: {e}")


def _split_message(text: str, max_len: int = 4000) -> list[str]:
    if len(text) <= max_len:
        return [text]

    chunks = []
    while text:
        if len(text) <= max_len:
            chunks.append(text)
            break
        split_at = text.rfind("\n", 0, max_len)
        if split_at < max_len // 2:
            split_at = max_len
        chunks.append(text[:split_at])
        text = text[split_at:].lstrip("\n")
    return chunks
