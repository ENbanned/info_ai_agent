"""Dynamic channel management with persistence and live filter mutation."""

import json
import re
from dataclasses import dataclass, field, asdict
from pathlib import Path

from loguru import logger
from pyrogram import Client, filters


DATA_PATH = Path(__file__).resolve().parent.parent / "data" / "channels.json"
CONFIG_PATH = Path(__file__).resolve().parent.parent / "config.json"


@dataclass
class Channel:
    id: int
    name: str
    username: str | None = None
    topics: dict[str, int] = field(default_factory=dict)
    paused: bool = False


@dataclass
class ParsedLink:
    username: str | None = None
    channel_id: int | None = None
    thread_id: int | None = None
    message_id: int | None = None


def parse_tg_link(url: str) -> ParsedLink | None:
    """Parse a t.me link into components.

    Supported formats:
      t.me/username              -> public channel
      t.me/username/123          -> public, message
      t.me/username/45/123       -> public, topic + message
      t.me/c/ID/123             -> private, message
      t.me/c/ID/45/123          -> private, topic + message
    """
    url = url.strip()
    m = re.match(r"(?:https?://)?(?:www\.)?t\.me/(.+)", url)
    if not m:
        return None

    path = m.group(1).strip("/")
    parts = path.split("/")
    if not parts:
        return None

    if parts[0] == "c":
        # Private channel: t.me/c/ID/...
        if len(parts) < 3:
            return None
        try:
            raw_id = int(parts[1])
        except ValueError:
            return None
        channel_id = int(f"-100{raw_id}")

        if len(parts) == 3:
            try:
                return ParsedLink(channel_id=channel_id, message_id=int(parts[2]))
            except ValueError:
                return None
        elif len(parts) >= 4:
            try:
                return ParsedLink(
                    channel_id=channel_id,
                    thread_id=int(parts[2]),
                    message_id=int(parts[3]),
                )
            except ValueError:
                return None
    else:
        # Public channel: t.me/username/...
        username = parts[0]
        if len(parts) == 1:
            return ParsedLink(username=username)
        elif len(parts) == 2:
            try:
                return ParsedLink(username=username, message_id=int(parts[1]))
            except ValueError:
                return None
        elif len(parts) >= 3:
            try:
                return ParsedLink(
                    username=username,
                    thread_id=int(parts[1]),
                    message_id=int(parts[2]),
                )
            except ValueError:
                return None

    return None


async def resolve_channel(user_client: Client, parsed: ParsedLink) -> Channel | None:
    """Resolve a ParsedLink to a Channel via the user client."""
    try:
        if parsed.username:
            chat = await user_client.get_chat(parsed.username)
        elif parsed.channel_id:
            chat = await user_client.get_chat(parsed.channel_id)
        else:
            return None
    except Exception as e:
        logger.warning(f"Failed to resolve channel: {e}")
        return None

    topics = {}
    if parsed.thread_id:
        topics[f"Topic_{parsed.thread_id}"] = parsed.thread_id

    return Channel(
        id=chat.id,
        name=chat.title or chat.first_name or str(chat.id),
        username=chat.username,
        topics=topics,
        paused=False,
    )


class ChannelStore:
    """Persistent channel store with live filter/map mutation."""

    def __init__(self):
        self._channels: dict[int, Channel] = {}
        self._filter: filters.chat | None = None
        self._topic_map: dict[int, set[int]] | None = None
        self._name_map: dict[int, str] | None = None
        self._topic_name_map: dict[int, dict[int, str]] | None = None
        self._load()

    # ---- persistence ----

    def _load(self):
        if DATA_PATH.exists():
            try:
                data = json.loads(DATA_PATH.read_text())
                for entry in data:
                    ch = Channel(**entry)
                    self._channels[ch.id] = ch
                logger.info(f"Loaded {len(self._channels)} channels from {DATA_PATH.name}")
                return
            except (json.JSONDecodeError, TypeError, KeyError) as e:
                logger.warning(f"Corrupted {DATA_PATH.name}, re-migrating: {e}")
        self._migrate()

    def _migrate(self):
        """Auto-migrate from config.json sources on first run."""
        try:
            with open(CONFIG_PATH) as f:
                config = json.load(f)
        except FileNotFoundError:
            logger.warning("config.json not found, starting with empty channel list")
            return

        sources = config.get("sources", {})

        for ch in sources.get("topic_channels", []):
            username = ch.get("username", "")
            if username:
                username = username.lstrip("@")
            channel = Channel(
                id=ch["id"],
                name=ch["name"],
                username=username or None,
                topics=ch.get("topics", {}),
                paused=False,
            )
            self._channels[channel.id] = channel

        for ch in sources.get("channels", []):
            username = ch.get("username", "")
            if username:
                username = username.lstrip("@")
            channel = Channel(
                id=ch["id"],
                name=ch["name"],
                username=username or None,
                topics={},
                paused=False,
            )
            self._channels[channel.id] = channel

        self._save()
        logger.success(f"Migrated {len(self._channels)} channels from config.json")

    def _save(self):
        DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
        data = [asdict(ch) for ch in self._channels.values()]
        DATA_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2))

    # ---- builder methods (called once at startup) ----

    def build_channel_filter(self) -> filters.chat:
        active_ids = [ch.id for ch in self._channels.values() if not ch.paused]
        if active_ids:
            return filters.chat(active_ids)
        cf = filters.chat(0)
        cf.clear()
        return cf

    def build_topic_map(self) -> dict[int, set[int]]:
        result: dict[int, set[int]] = {}
        for ch in self._channels.values():
            if ch.topics and not ch.paused:
                result[ch.id] = set(ch.topics.values())
        return result

    def build_name_map(self) -> dict[int, str]:
        return {ch.id: ch.name for ch in self._channels.values()}

    def build_topic_name_map(self) -> dict[int, dict[int, str]]:
        result: dict[int, dict[int, str]] = {}
        for ch in self._channels.values():
            if ch.topics:
                result[ch.id] = {v: k for k, v in ch.topics.items()}
        return result

    def attach_listener(self, channel_filter, topic_map, name_map, topic_name_map):
        """Store references to the mutable objects used by the listener handler."""
        self._filter = channel_filter
        self._topic_map = topic_map
        self._name_map = name_map
        self._topic_name_map = topic_name_map

    # ---- lookups ----

    @property
    def channels(self) -> dict[int, Channel]:
        return self._channels

    def get(self, channel_id: int) -> Channel | None:
        return self._channels.get(channel_id)

    def find_by_name(self, name: str) -> Channel | None:
        name_lower = name.lower()
        for ch in self._channels.values():
            if ch.name.lower() == name_lower:
                return ch
        return None

    def find_by_username(self, username: str) -> Channel | None:
        username = username.lstrip("@").lower()
        for ch in self._channels.values():
            if ch.username and ch.username.lower() == username:
                return ch
        return None

    def find(self, query: str) -> Channel | None:
        """Find a channel by ID, username, link, or name."""
        query = query.strip()

        # Try as channel ID
        try:
            return self._channels.get(int(query))
        except ValueError:
            pass

        # Try as t.me link
        if "t.me/" in query:
            parsed = parse_tg_link(query)
            if parsed:
                if parsed.channel_id:
                    return self._channels.get(parsed.channel_id)
                if parsed.username:
                    return self.find_by_username(parsed.username)

        # Try as username
        if query.startswith("@"):
            return self.find_by_username(query)

        # Try as name
        return self.find_by_name(query)

    # ---- live mutations ----

    def add(self, channel: Channel) -> None:
        self._channels[channel.id] = channel
        self._save()
        if self._filter is not None and not channel.paused:
            self._filter.add(channel.id)
            self._name_map[channel.id] = channel.name
            if channel.topics:
                self._topic_map[channel.id] = set(channel.topics.values())
                self._topic_name_map[channel.id] = {v: k for k, v in channel.topics.items()}

    def remove(self, channel_id: int) -> Channel | None:
        ch = self._channels.pop(channel_id, None)
        if ch is None:
            return None
        self._save()
        if self._filter is not None:
            self._filter.discard(channel_id)
            self._name_map.pop(channel_id, None)
            self._topic_map.pop(channel_id, None)
            self._topic_name_map.pop(channel_id, None)
        return ch

    def pause(self, channel_id: int) -> bool:
        ch = self._channels.get(channel_id)
        if not ch or ch.paused:
            return False
        ch.paused = True
        self._save()
        if self._filter is not None:
            self._filter.discard(channel_id)
            self._topic_map.pop(channel_id, None)
        return True

    def resume(self, channel_id: int) -> bool:
        ch = self._channels.get(channel_id)
        if not ch or not ch.paused:
            return False
        ch.paused = False
        self._save()
        if self._filter is not None:
            self._filter.add(channel_id)
            self._name_map[channel_id] = ch.name
            if ch.topics:
                self._topic_map[channel_id] = set(ch.topics.values())
                if channel_id not in self._topic_name_map:
                    self._topic_name_map[channel_id] = {v: k for k, v in ch.topics.items()}
        return True

    def add_topic(self, channel_id: int, name: str, thread_id: int) -> bool:
        ch = self._channels.get(channel_id)
        if not ch:
            return False
        ch.topics[name] = thread_id
        self._save()
        if self._filter is not None:
            if channel_id in self._topic_map:
                self._topic_map[channel_id].add(thread_id)
            else:
                self._topic_map[channel_id] = {thread_id}
            if channel_id in self._topic_name_map:
                self._topic_name_map[channel_id][thread_id] = name
            else:
                self._topic_name_map[channel_id] = {thread_id: name}
        return True
