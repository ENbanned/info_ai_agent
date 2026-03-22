from typing import Optional

from mem0.configs.llms.base import BaseLlmConfig


class ClaudeCodeConfig(BaseLlmConfig):
    """Configuration for Claude Code LLM provider (via claude-agent-sdk)."""

    def __init__(
        self,
        model: Optional[str] = "sonnet",
        temperature: float = 0.1,
        api_key: Optional[str] = None,
        max_tokens: int = 4096,
        top_p: float = 0.1,
        top_k: int = 1,
        enable_vision: bool = False,
        vision_details: Optional[str] = "auto",
        http_client_proxies: Optional[dict] = None,
        # Claude Code specific
        permission_mode: str = "acceptEdits",
        max_turns: int = 1,
        cwd: Optional[str] = None,
    ):
        super().__init__(
            model=model,
            temperature=temperature,
            api_key=api_key,
            max_tokens=max_tokens,
            top_p=top_p,
            top_k=top_k,
            enable_vision=enable_vision,
            vision_details=vision_details,
            http_client_proxies=http_client_proxies,
        )
        self.permission_mode = permission_mode
        self.max_turns = max_turns
        self.cwd = cwd
