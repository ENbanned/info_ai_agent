import asyncio
import json
import os
from pathlib import Path

from loguru import logger
from pyrogram import Client, ContinuePropagation, filters, enums
from pyrogram.types import Message
from claude_agent_sdk import query, ClaudeAgentOptions, ResultMessage, AssistantMessage, TextBlock

from src.config import BOT_CONFIG, MODELS_CONFIG
from src.pipeline.prompts import ASK_SYSTEM_PROMPT
from src.analyst.memory_tools import create_memory_server
from src.tg.formatter import markdown_to_telegram_html, split_html_message

_SESSIONS_PATH = Path(__file__).resolve().parent.parent.parent / "data" / "ask_sessions.json"
_MEDIA_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "media"
_MAX_SESSIONS = 500
_COLLECT_TIMEOUT = 0.5


def _load_sessions() -> dict[str, str]:
    if _SESSIONS_PATH.exists():
        try:
            data = json.loads(_SESSIONS_PATH.read_text())
            return {str(k): v for k, v in data.items()}
        except Exception:
            pass
    return {}


def _save_sessions(sessions: dict[str, str]) -> None:
    try:
        _SESSIONS_PATH.write_text(json.dumps(sessions))
    except Exception as e:
        logger.warning(f"Failed to persist sessions: {e}")


_sessions: dict[str, str] = _load_sessions()


def _set_session(msg_id: int, session_id: str) -> None:
    _sessions[str(msg_id)] = session_id
    if len(_sessions) > _MAX_SESSIONS:
        oldest = next(iter(_sessions))
        _sessions.pop(oldest)
    _save_sessions(_sessions)


def _get_session(msg_id: int) -> str | None:
    return _sessions.get(str(msg_id))


def _build_options(memory, resume_id: str | None = None) -> ClaudeAgentOptions:
    mcp_server = create_memory_server(memory)

    options = ClaudeAgentOptions(
        model=MODELS_CONFIG["analyst"],
        tools=["WebSearch", "WebFetch", "Read"],
        mcp_servers={"memory": mcp_server},
        allowed_tools=[
            "WebSearch", "WebFetch", "Read",
            "mcp__memory__search_memory", "mcp__memory__query_entity",
        ],
        max_turns=20,
        effort="high",
        permission_mode="bypassPermissions",
        system_prompt=ASK_SYSTEM_PROMPT,
    )

    if resume_id:
        options.resume = resume_id

    return options


async def _download_single_photo(message: Message) -> str | None:
    if not message.photo:
        return None
    filename = f"ask_{message.chat.id}_{message.id}.jpg"
    dest = _MEDIA_DIR / filename
    try:
        await message.download(file_name=str(dest))
        logger.debug(f"Photo saved │ {dest}")
        return str(dest)
    except Exception as e:
        logger.error(f"Photo download failed: {e}")
        return None


async def _download_photos(message: Message) -> list[str]:
    if message.media_group_id:
        try:
            group_msgs = await message.get_media_group()
        except Exception as e:
            logger.warning(f"get_media_group failed: {e}, falling back to single photo")
            group_msgs = [message]
    else:
        group_msgs = [message] if message.photo else []

    paths = []
    for msg in group_msgs:
        if msg.photo:
            path = await _download_single_photo(msg)
            if path:
                paths.append(path)
    return paths


def _build_prompt(text: str, image_paths: list[str]) -> str:
    parts = []
    if image_paths:
        if len(image_paths) == 1:
            parts.append(f"Use the Read tool to examine this image: {image_paths[0]}")
        else:
            joined = "\n".join(image_paths)
            parts.append(f"Use the Read tool to examine these {len(image_paths)} images:\n{joined}")
    if text:
        parts.append(text)
    return "\n\n".join(parts)


async def _run_ask_query(question: str, memory, resume_id: str | None = None) -> tuple[str, str | None]:
    os.environ["CLAUDE_CODE_STREAM_CLOSE_TIMEOUT"] = "2400000"

    options = _build_options(memory, resume_id)

    answer = ""
    session_id = None
    try:
        async for message in query(prompt=question, options=options):
            if isinstance(message, ResultMessage):
                if message.result:
                    answer = message.result
                if message.session_id:
                    session_id = message.session_id
            elif isinstance(message, AssistantMessage):
                for block in message.content:
                    if isinstance(block, TextBlock):
                        answer = block.text
    except Exception as e:
        logger.error(f"/ask Opus failed: {e}")
        if len(answer) < 50:
            answer = f"Ошибка при обработке запроса: {e}"
        else:
            logger.warning(f"/ask Opus crashed but partial answer exists ({len(answer)} chars), keeping it")

    return answer, session_id


def _split_plain(text: str, max_len: int = 4000) -> list[str]:
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


async def _send_answer(message, answer: str, session_id: str | None) -> None:
    html_answer = markdown_to_telegram_html(answer)
    chunks = split_html_message(html_answer, max_len=4000)

    last_sent_id = None
    for i, chunk in enumerate(chunks):
        try:
            sent = await message.reply_text(chunk, parse_mode=enums.ParseMode.HTML)
            last_sent_id = sent.id
        except Exception as e:
            logger.warning(f"/ask HTML send failed chunk {i+1}: {e}, falling back to plain text")
            try:
                plain_chunks = _split_plain(answer)
                plain_chunk = plain_chunks[i] if i < len(plain_chunks) else chunk
                sent = await message.reply_text(plain_chunk)
                last_sent_id = sent.id
            except Exception as e2:
                logger.error(f"/ask reply chunk {i+1} failed entirely: {e2}")

    if last_sent_id and session_id:
        _set_session(last_sent_id, session_id)
        logger.debug(f"/ask session mapped │ msg {last_sent_id} → {session_id[:12]}...")


def register_ask_handler(bot: Client, memory) -> None:
    owner_filter = filters.user(BOT_CONFIG["owner_chat_id"]) & filters.private

    # chat_id -> {text, images, orig_message, resume_session_id, task}
    _pending: dict[int, dict] = {}

    async def _flush(chat_id: int) -> None:
        await asyncio.sleep(_COLLECT_TIMEOUT)
        pending = _pending.pop(chat_id, None)
        if not pending:
            return

        text = pending["text"]
        image_paths = pending["images"]
        orig_message = pending["orig_message"]
        resume_session_id = pending.get("resume_session_id")

        if not text and not image_paths:
            return

        prompt = _build_prompt(text, image_paths)
        n = len(image_paths)
        img_tag = f" 📷x{n}" if n > 1 else (" 📷" if n == 1 else "")
        label = "/ask follow-up" if resume_session_id else "/ask"
        logger.info(f"🔎 {label}{img_tag} │ {text[:100]}")
        thinking_msg = await orig_message.reply_text("🔍 Думаю...")

        answer, new_session_id = await _run_ask_query(prompt, memory, resume_id=resume_session_id)
        effective_session = new_session_id or resume_session_id

        try:
            await thinking_msg.delete()
        except Exception:
            pass

        await _send_answer(orig_message, answer, effective_session)
        logger.success(
            f"🔎 {label} done │ {len(answer)} chars │ "
            f"session {effective_session[:12] if effective_session else 'none'}..."
        )

    def _reschedule(chat_id: int) -> None:
        existing = _pending.get(chat_id, {}).get("task")
        if existing:
            existing.cancel()
        _pending[chat_id]["task"] = asyncio.create_task(_flush(chat_id))

    @bot.on_message(filters.command("ask") & owner_filter)
    async def handle_ask(client: Client, message: Message) -> None:
        text = message.text.partition(" ")[2].strip() if message.text else ""
        caption = message.caption.partition(" ")[2].strip() if message.caption else ""
        question_text = text or caption

        image_paths = await _download_photos(message)

        if not question_text and not image_paths:
            await message.reply_text("Формат: /ask <вопрос> (можно приложить фото)")
            return

        chat_id = message.chat.id
        existing = _pending.get(chat_id, {}).get("task")
        if existing:
            existing.cancel()

        _pending[chat_id] = {
            "text": question_text,
            "images": image_paths,
            "orig_message": message,
            "resume_session_id": None,
            "task": None,
        }
        _reschedule(chat_id)

    _non_cmd = filters.create(lambda _, __, m: bool(m.text) and not m.text.startswith("/"))

    @bot.on_message(~filters.reply & _non_cmd & owner_filter)
    async def handle_text_continuation(client: Client, message: Message) -> None:
        chat_id = message.chat.id
        if chat_id not in _pending:
            return
        pending = _pending[chat_id]
        pending["text"] = (pending["text"] + "\n" + message.text).strip() if pending["text"] else message.text
        _reschedule(chat_id)

    @bot.on_message(filters.reply & owner_filter & ~filters.command("ask"))
    async def handle_reply(client: Client, message: Message) -> None:
        if message.text and message.text.startswith("/"):
            raise ContinuePropagation
        reply_to = message.reply_to_message
        if not reply_to or reply_to.from_user is None:
            return

        bot_me = await client.get_me()
        if reply_to.from_user.id != bot_me.id:
            return

        session_id = _get_session(reply_to.id)
        if not session_id:
            return

        question_text = message.text.strip() if message.text else ""
        if not question_text:
            question_text = message.caption.strip() if message.caption else ""

        image_paths = await _download_photos(message)

        if not question_text and not image_paths:
            return

        chat_id = message.chat.id

        # If a pending entry already exists for the same session (e.g. another
        # part of a Telegram-split long reply just landed), merge into it.
        existing = _pending.get(chat_id)
        if existing and existing.get("resume_session_id") == session_id:
            if question_text:
                existing["text"] = (
                    existing["text"] + "\n" + question_text
                ).strip() if existing["text"] else question_text
            if image_paths:
                existing["images"].extend(image_paths)
            _reschedule(chat_id)
            return

        # Different/no pending — replace it.
        if existing:
            existing_task = existing.get("task")
            if existing_task:
                existing_task.cancel()

        _pending[chat_id] = {
            "text": question_text,
            "images": image_paths,
            "orig_message": message,
            "resume_session_id": session_id,
            "task": None,
        }
        _reschedule(chat_id)
