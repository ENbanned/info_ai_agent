import os

from loguru import logger
from pyrogram import Client, filters, enums
from claude_agent_sdk import query, ClaudeAgentOptions, ResultMessage, AssistantMessage, TextBlock

from src.config import BOT_CONFIG, MODELS_CONFIG
from src.pipeline.prompts import ASK_SYSTEM_PROMPT
from src.analyst.memory_tools import create_memory_server
from src.tg.formatter import markdown_to_telegram_html, split_html_message

_sessions: dict[int, str] = {}


def _build_options(memory, resume_id: str | None = None) -> ClaudeAgentOptions:
    mcp_server = create_memory_server(memory)

    options = ClaudeAgentOptions(
        model=MODELS_CONFIG["analyst"],
        tools=["WebSearch", "WebFetch"],
        mcp_servers={"memory": mcp_server},
        allowed_tools=[
            "WebSearch", "WebFetch",
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
        _sessions[last_sent_id] = session_id
        if len(_sessions) > 500:
            _sessions.pop(next(iter(_sessions)))
        logger.debug(f"/ask session mapped │ msg {last_sent_id} → {session_id[:12]}...")


def register_ask_handler(bot: Client, memory) -> None:
    owner_filter = filters.user(BOT_CONFIG["owner_chat_id"]) & filters.private

    @bot.on_message(filters.command("ask") & owner_filter)
    async def handle_ask(client: Client, message):
        question = message.text.partition(" ")[2].strip() if message.text else ""
        if not question:
            await message.reply_text("Формат: /ask <вопрос>")
            return

        logger.info(f"🔎 /ask │ {question[:100]}")
        thinking_msg = await message.reply_text("🔍 Думаю...")

        answer, session_id = await _run_ask_query(question, memory)

        try:
            await thinking_msg.delete()
        except Exception:
            pass

        await _send_answer(message, answer, session_id)
        logger.success(f"🔎 /ask done │ {len(answer)} chars │ session {session_id[:12] if session_id else 'none'}...")


    @bot.on_message(filters.reply & owner_filter & ~filters.command("ask"))
    async def handle_reply(client: Client, message):
        reply_to = message.reply_to_message
        if not reply_to or reply_to.from_user is None:
            return

        bot_me = await client.get_me()
        if reply_to.from_user.id != bot_me.id:
            return

        session_id = _sessions.get(reply_to.id)
        if not session_id:
            return

        question = message.text.strip() if message.text else ""
        if not question:
            return

        logger.info(f"🔎 /ask follow-up │ {question[:100]} │ session {session_id[:12]}...")
        thinking_msg = await message.reply_text("🔍 Думаю...")

        answer, new_session_id = await _run_ask_query(question, memory, resume_id=session_id)
        effective_session = new_session_id or session_id

        try:
            await thinking_msg.delete()
        except Exception:
            pass

        await _send_answer(message, answer, effective_session)
        logger.success(f"🔎 /ask follow-up done │ {len(answer)} chars")
