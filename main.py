"""Crypto News Intelligence System — Orchestrator."""

import asyncio
import time

from pyrogram import idle

from src.log import setup_logging, logger
from src.config import MEM0_CONFIG
from src.channels import ChannelStore
from src.tg.create_session import get_client
from src.tg.bot import get_bot_client
from src.tg.listener import register_listener
from src.pipeline.ingest import ingest_worker
from src.analyst.scheduler import analyst_loop
from src.tg.ask_handler import register_ask_handler
from src.tg.channel_manager import register_channel_manager

setup_logging()


async def main():
    system_start = time.time()

    # 1. Channel store (loads from data/channels.json or migrates from config.json)
    store = ChannelStore()

    # 2. Initialize mem0
    from mem0 import AsyncMemory

    logger.info("Initializing mem0...")
    memory = await AsyncMemory.from_config(config_dict=MEM0_CONFIG)
    logger.success("mem0 ready │ Qdrant + Neo4j")

    # 3. Start TG bot
    logger.info("Starting bot client...")
    bot = get_bot_client()
    await bot.start()
    bot_me = await bot.get_me()
    logger.success(f"Bot online │ @{bot_me.username}")

    # 4. Start TG user client
    logger.info("Starting user client...")
    user = get_client()
    await user.start()
    user_me = await user.get_me()
    logger.success(f"User client │ @{user_me.username}")

    # 5. Register handlers on bot
    register_ask_handler(bot, memory)
    register_channel_manager(bot, user, store)

    # 6. Pipeline
    queue: asyncio.Queue = asyncio.Queue(maxsize=1000)
    register_listener(user, queue, store)

    # 7. Background tasks
    ingest_task = asyncio.create_task(ingest_worker(queue, memory, bot))
    analyst_task = asyncio.create_task(analyst_loop(memory, bot, system_start))

    logger.success("🟢 System running │ listening + analyst scheduled")

    # 8. Keep alive
    try:
        await idle()
    finally:
        logger.info("Shutting down...")
        ingest_task.cancel()
        analyst_task.cancel()
        for task in [ingest_task, analyst_task]:
            try:
                await task
            except asyncio.CancelledError:
                pass
        await bot.stop()
        await user.stop()
        logger.info("Shutdown complete.")


asyncio.run(main())
