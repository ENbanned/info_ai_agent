import asyncio

from loguru import logger
from pyrogram import Client, filters
from pyrogram.handlers import MessageHandler
from pyrogram.types import Message

from src.channels import ChannelStore
from src.pipeline.preprocessor import process_message


def register_listener(client: Client, queue: asyncio.Queue, store: ChannelStore) -> None:
    channel_filter = store.build_channel_filter()
    topic_map = store.build_topic_map()
    name_map = store.build_name_map()
    topic_name_map = store.build_topic_name_map()
    store.attach_listener(channel_filter, topic_map, name_map, topic_name_map)

    async def on_message(_client: Client, message: Message) -> None:
        chat_id = message.chat.id

        if chat_id in topic_map:
            thread_id = message.message_thread_id
            if thread_id not in topic_map[chat_id]:
                return

        channel_name = name_map.get(chat_id, "unknown")

        topic_name = None
        if chat_id in topic_name_map and message.message_thread_id:
            topic_name = topic_name_map[chat_id].get(message.message_thread_id)

        processed = await process_message(message, channel_name, topic_name)
        if processed is None:
            return

        topic_tag = f" │ {topic_name}" if topic_name else ""
        media_tag = " 📎" if processed.has_media else ""
        text_clean = processed.text.replace("\n", " ")
        logger.info(f"📥 {channel_name}{topic_tag}{media_tag} │ {text_clean}")

        await queue.put(processed)

    client.add_handler(MessageHandler(on_message, channel_filter), group=0)
    logger.success(
        f"Listener: {len(name_map)} channels, {len(topic_map)} with topic filters"
    )
