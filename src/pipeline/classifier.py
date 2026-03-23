import os
from typing import Literal

from loguru import logger
from claude_agent_sdk import query, ClaudeAgentOptions, ResultMessage, AssistantMessage, TextBlock

from src.config import MODELS_CONFIG
from src.pipeline.prompts import CLASSIFICATION_PROMPT
from src.pipeline.preprocessor import ProcessedMessage

_CWD = "/tmp/crypto-classifier"
os.makedirs(_CWD, exist_ok=True)

Classification = Literal["URGENT", "RELEVANT", "NOISE"]

_ICONS = {"URGENT": "🔴", "RELEVANT": "🟢", "NOISE": "⚫"}


async def classify(msg: ProcessedMessage) -> Classification:
    prompt = CLASSIFICATION_PROMPT.format(
        channel_name=msg.channel_name,
        message_text=msg.text[:2000],
    )

    options = ClaudeAgentOptions(
        model=MODELS_CONFIG["classifier"],
        max_turns=1,
        permission_mode="bypassPermissions",
        tools=[],
        cwd=_CWD,
    )

    result_text = ""
    try:
        async for message in query(prompt=prompt, options=options):
            if isinstance(message, ResultMessage) and message.result:
                result_text = message.result
            elif isinstance(message, AssistantMessage):
                for block in message.content:
                    if isinstance(block, TextBlock):
                        result_text = block.text
                        break
    except Exception as e:
        logger.warning(f"Classification failed, defaulting to RELEVANT: {e}")
        return "RELEVANT"

    text_upper = result_text.strip().upper()
    if "URGENT" in text_upper:
        result: Classification = "URGENT"
    elif "NOISE" in text_upper:
        result = "NOISE"
    else:
        result = "RELEVANT"

    icon = _ICONS[result]
    logger.info(f"{icon} {result} │ {msg.channel_name}")

    return result
