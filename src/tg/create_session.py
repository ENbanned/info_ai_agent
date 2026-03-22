from pathlib import Path

from pyrogram import Client, enums

from src.config import TG_CONFIG

SESSIONS_DIR = str(Path(__file__).resolve().parent.parent.parent / "data" / "sessions")


def get_client() -> Client:
    return Client(
        name=TG_CONFIG["session_name"],
        workdir=SESSIONS_DIR,
        api_id=TG_CONFIG["api_id"],
        api_hash=TG_CONFIG["api_hash"],
        device_model=TG_CONFIG["device_model"],
        system_version=TG_CONFIG["system_version"],
        app_version=TG_CONFIG["app_version"],
        lang_pack=TG_CONFIG["lang_pack"],
        lang_code=TG_CONFIG["lang_code"],
        system_lang_code=TG_CONFIG["system_lang_code"],
        client_platform=enums.ClientPlatform[TG_CONFIG["client_platform"]],
    )
