import asyncio
import time
from pathlib import Path

from pyrogram import idle
from mem0 import AsyncMemory

_DATA = Path(__file__).parent / "data"
for _d in ("sessions", "logs", "media", "reports", "skills", "analyst_workdir", "classifier_workdir"):
    (_DATA / _d).mkdir(parents=True, exist_ok=True)

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
from src.tg.report_handler import register_report_handler
from src.tg.backfill_handler import register_backfill_handler

setup_logging()


async def main():
    system_start = time.time()

    store = ChannelStore()

    logger.info("Initializing mem0...")
    memory = await AsyncMemory.from_config(config_dict=MEM0_CONFIG)
    logger.success("mem0 ready │ Qdrant + Neo4j")

    logger.info("Starting bot client...")
    bot = get_bot_client()
    await bot.start()
    bot_me = await bot.get_me()
    logger.success(f"Bot online │ @{bot_me.username}")

    logger.info("Starting user client...")
    user = get_client()
    await user.start()
    user_me = await user.get_me()
    logger.success(f"User client │ @{user_me.username}")

    memory_lock = asyncio.Lock()

    register_ask_handler(bot, memory)
    register_channel_manager(bot, user, store)
    register_report_handler(bot, memory, memory_lock)
    register_backfill_handler(bot, user, store, memory, memory_lock)

    queue: asyncio.Queue = asyncio.Queue(maxsize=1000)
    register_listener(user, queue, store)
    ingest_task = asyncio.create_task(ingest_worker(queue, memory, bot, memory_lock))
    analyst_task = asyncio.create_task(analyst_loop(memory, bot, system_start, memory_lock))

    logger.success("🟢 System running │ listening + analyst scheduled")

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
