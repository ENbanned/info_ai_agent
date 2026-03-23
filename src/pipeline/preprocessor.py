from dataclasses import dataclass
from pathlib import Path

from pyrogram.types import Message

MEDIA_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "media"
MEDIA_DIR.mkdir(exist_ok=True)


@dataclass
class ProcessedMessage:
    text: str
    channel_name: str
    channel_id: int
    message_id: int
    timestamp: int
    has_media: bool
    media_path: str | None
    topic_name: str | None


async def process_message(message: Message, channel_name: str, topic_name: str | None = None) -> ProcessedMessage | None:
    parts: list[str] = []

    text = message.text or message.caption or ""
    if text:
        parts.append(text)

    if message.forward_origin:
        origin = message.forward_origin
        if hasattr(origin, "sender_chat") and origin.sender_chat:
            parts.insert(0, f"[Forwarded from: {origin.sender_chat.title}]")
        elif hasattr(origin, "sender_user") and origin.sender_user:
            user = origin.sender_user
            name = user.first_name or ""
            if user.last_name:
                name += f" {user.last_name}"
            parts.insert(0, f"[Forwarded from: {name}]")
        elif hasattr(origin, "sender_user_name") and origin.sender_user_name:
            parts.insert(0, f"[Forwarded from: {origin.sender_user_name}]")

    if message.web_page:
        wp = message.web_page
        wp_parts = []
        if wp.title:
            wp_parts.append(wp.title)
        if wp.description:
            wp_parts.append(wp.description)
        if wp_parts:
            parts.append(f"[Link preview: {' — '.join(wp_parts)}]")

    media_path = None
    has_media = False
    if message.photo:
        has_media = True
        filename = f"{message.chat.id}_{message.id}.jpg"
        dest = MEDIA_DIR / filename
        try:
            await message.download(file_name=str(dest))
            media_path = str(dest)
        except Exception:
            pass

    combined = "\n".join(parts).strip()
    if not combined and not has_media:
        return None

    return ProcessedMessage(
        text=combined,
        channel_name=channel_name,
        channel_id=message.chat.id,
        message_id=message.id,
        timestamp=int(message.date.timestamp()) if message.date else 0,
        has_media=has_media,
        media_path=media_path,
        topic_name=topic_name,
    )
