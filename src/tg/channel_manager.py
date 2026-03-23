from loguru import logger
from pyrogram import Client, filters, enums
from pyrogram.types import Message

from src.config import BOT_CONFIG
from src.channels import ChannelStore, parse_tg_link, resolve_channel

OWNER = BOT_CONFIG["owner_chat_id"]


def register_channel_manager(bot: Client, user: Client, store: ChannelStore) -> None:

    _pending_topic: dict[int, dict] = {}

    @bot.on_message(filters.command("add") & filters.user(OWNER) & filters.private)
    async def handle_add(_client: Client, message: Message) -> None:
        args = message.text.split(maxsplit=1)
        if len(args) < 2:
            await message.reply_text("Usage: /add &lt;t.me link&gt;", parse_mode=enums.ParseMode.HTML)
            return

        link = args[1].strip()
        parsed = parse_tg_link(link)
        if not parsed:
            await message.reply_text("Invalid link. Supported:\n<code>t.me/username</code>\n<code>t.me/c/ID/msg</code>\n<code>t.me/c/ID/topic/msg</code>", parse_mode=enums.ParseMode.HTML)
            return

        await message.reply_text("Resolving channel...")

        resolved = await resolve_channel(user, parsed)
        if not resolved:
            await message.reply_text("Could not resolve channel. Make sure the user account is a member.")
            return

        existing = store.get(resolved.id)

        if existing and parsed.thread_id:
            if parsed.thread_id in existing.topics.values():
                await message.reply_text(f"Topic already tracked in <b>{existing.name}</b>", parse_mode=enums.ParseMode.HTML)
                return
            _pending_topic[OWNER] = {
                "channel_id": existing.id,
                "thread_id": parsed.thread_id,
                "new_channel": None,
            }
            await message.reply_text(
                f"Channel <b>{existing.name}</b> already tracked.\n"
                f"Send topic name for thread <code>{parsed.thread_id}</code>:",
                parse_mode=enums.ParseMode.HTML,
            )
            return

        if existing and not parsed.thread_id:
            await message.reply_text(f"Already tracked: <b>{existing.name}</b>", parse_mode=enums.ParseMode.HTML)
            return

        if parsed.thread_id:
            _pending_topic[OWNER] = {
                "channel_id": resolved.id,
                "thread_id": parsed.thread_id,
                "new_channel": resolved,
            }
            await message.reply_text(
                f"Adding <b>{resolved.name}</b>\n"
                f"Send topic name for thread <code>{parsed.thread_id}</code>:",
                parse_mode=enums.ParseMode.HTML,
            )
            return

        store.add(resolved)
        at = f" · @{resolved.username}" if resolved.username else ""
        await message.reply_text(f"✅ Added: <b>{resolved.name}</b>{at}", parse_mode=enums.ParseMode.HTML)
        logger.info(f"Channel added: {resolved.name} [{resolved.id}]")

    @bot.on_message(filters.command("remove") & filters.user(OWNER) & filters.private)
    async def handle_remove(_client: Client, message: Message) -> None:
        args = message.text.split(maxsplit=1)
        if len(args) < 2:
            await message.reply_text("Usage: /remove &lt;id | @username | name | link&gt;", parse_mode=enums.ParseMode.HTML)
            return

        ch = store.find(args[1].strip())
        if not ch:
            await message.reply_text("Channel not found.")
            return

        removed = store.remove(ch.id)
        await message.reply_text(f"🗑 Removed: <b>{removed.name}</b> [<code>{removed.id}</code>]", parse_mode=enums.ParseMode.HTML)
        logger.info(f"Channel removed: {removed.name} [{removed.id}]")

    @bot.on_message(filters.command("channels") & filters.user(OWNER) & filters.private)
    async def handle_channels(_client: Client, message: Message) -> None:
        channels = list(store.channels.values())
        if not channels:
            await message.reply_text("No channels configured.")
            return

        active = [ch for ch in channels if not ch.paused]
        paused = [ch for ch in channels if ch.paused]

        lines = [f"<b>📡 Channels: {len(active)} active, {len(paused)} paused</b>\n"]

        for ch in active:
            parts = [f"▸ {ch.name}"]
            if ch.username:
                parts.append(f"@{ch.username}")
            if ch.topics:
                topic_names = ", ".join(ch.topics.keys())
                parts.append(f"📌 {topic_names}")
            lines.append(" · ".join(parts))

        if paused:
            lines.append("")
            lines.append("<b>⏸ Paused:</b>")
            for ch in paused:
                lines.append(f"  {ch.name} [<code>{ch.id}</code>]")

        text = "\n".join(lines)

        if len(text) > 4000:
            mid = len(lines) // 2
            part1 = "\n".join(lines[:mid])
            part2 = "\n".join(lines[mid:])
            await message.reply_text(part1, parse_mode=enums.ParseMode.HTML)
            await message.reply_text(part2, parse_mode=enums.ParseMode.HTML)
        else:
            await message.reply_text(text, parse_mode=enums.ParseMode.HTML)

    @bot.on_message(filters.command("pause") & filters.user(OWNER) & filters.private)
    async def handle_pause(_client: Client, message: Message) -> None:
        args = message.text.split(maxsplit=1)
        if len(args) < 2:
            await message.reply_text("Usage: /pause &lt;id | @username | name&gt;", parse_mode=enums.ParseMode.HTML)
            return

        ch = store.find(args[1].strip())
        if not ch:
            await message.reply_text("Channel not found.")
            return

        if store.pause(ch.id):
            await message.reply_text(f"⏸ Paused: <b>{ch.name}</b>", parse_mode=enums.ParseMode.HTML)
            logger.info(f"Channel paused: {ch.name} [{ch.id}]")
        else:
            await message.reply_text(f"Already paused: <b>{ch.name}</b>", parse_mode=enums.ParseMode.HTML)

    @bot.on_message(filters.command("resume") & filters.user(OWNER) & filters.private)
    async def handle_resume(_client: Client, message: Message) -> None:
        args = message.text.split(maxsplit=1)
        if len(args) < 2:
            await message.reply_text("Usage: /resume &lt;id | @username | name&gt;", parse_mode=enums.ParseMode.HTML)
            return

        ch = store.find(args[1].strip())
        if not ch:
            await message.reply_text("Channel not found.")
            return

        if store.resume(ch.id):
            await message.reply_text(f"▶️ Resumed: <b>{ch.name}</b>", parse_mode=enums.ParseMode.HTML)
            logger.info(f"Channel resumed: {ch.name} [{ch.id}]")
        else:
            await message.reply_text(f"Already active: <b>{ch.name}</b>", parse_mode=enums.ParseMode.HTML)

    @bot.on_message(filters.user(OWNER) & filters.private, group=-1)
    async def handle_pending_topic(_client: Client, message: Message) -> None:
        if OWNER not in _pending_topic:
            message.continue_propagation()
            return

        if message.text and message.text.startswith("/"):
            _pending_topic.pop(OWNER, None)
            message.continue_propagation()
            return

        topic_name = (message.text or "").strip()
        if not topic_name:
            await message.reply_text("Send a text name for the topic, or /cancel.")
            message.stop_propagation()
            return

        pending = _pending_topic.pop(OWNER)
        channel_id = pending["channel_id"]
        thread_id = pending["thread_id"]
        new_channel = pending["new_channel"]

        if new_channel:
            new_channel.topics = {topic_name: thread_id}
            store.add(new_channel)
            at = f" · @{new_channel.username}" if new_channel.username else ""
            await message.reply_text(
                f"✅ Added: <b>{new_channel.name}</b>{at}\n"
                f"📌 Topic: {topic_name} (thread {thread_id})",
                parse_mode=enums.ParseMode.HTML,
            )
            logger.info(f"Channel added: {new_channel.name} [{new_channel.id}] with topic {topic_name}")
        else:
            store.add_topic(channel_id, topic_name, thread_id)
            ch = store.get(channel_id)
            await message.reply_text(
                f"✅ Topic added to <b>{ch.name}</b>\n"
                f"📌 {topic_name} (thread {thread_id})",
                parse_mode=enums.ParseMode.HTML,
            )
            logger.info(f"Topic added: {topic_name} [{thread_id}] to {ch.name}")

        message.stop_propagation()
