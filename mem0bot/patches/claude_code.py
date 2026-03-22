import asyncio
import concurrent.futures
import json
import logging
import os
from typing import Any, Dict, List, Optional, Union

try:
    from claude_agent_sdk import (
        AssistantMessage,
        ClaudeAgentOptions,
        ResultMessage,
        TextBlock,
        query,
    )
except ImportError:
    raise ImportError(
        "The 'claude-agent-sdk' library is required. Install with: uv add claude-agent-sdk"
    )

from mem0.configs.llms.base import BaseLlmConfig
from mem0.configs.llms.claude_code import ClaudeCodeConfig
from mem0.llms.base import LLMBase

logger = logging.getLogger(__name__)

_SDK_CWD = "/tmp/mem0-claude-code"


class ClaudeCodeLLM(LLMBase):
    """LLM provider using Claude Code (claude-agent-sdk).

    Text/JSON: tools=[], max_turns=1, plain text generation.
    Tool calling: tools=[], max_turns=1, output_format enforces JSON schema.
    """

    def __init__(self, config: Optional[Union[BaseLlmConfig, ClaudeCodeConfig, Dict]] = None):
        if config is None:
            config = ClaudeCodeConfig()
        elif isinstance(config, dict):
            config = ClaudeCodeConfig(**config)
        elif isinstance(config, BaseLlmConfig) and not isinstance(config, ClaudeCodeConfig):
            config = ClaudeCodeConfig(
                model=config.model,
                temperature=config.temperature,
                api_key=config.api_key,
                max_tokens=config.max_tokens,
                top_p=config.top_p,
                top_k=config.top_k,
                enable_vision=config.enable_vision,
                vision_details=config.vision_details,
            )

        super().__init__(config)
        if not self.config.model:
            self.config.model = "sonnet"

        os.environ.pop("CLAUDECODE", None)
        os.makedirs(_SDK_CWD, exist_ok=True)

    # ------------------------------------------------------------------
    # Sync entry point
    # ------------------------------------------------------------------

    def generate_response(
        self,
        messages: List[Dict[str, str]],
        response_format=None,
        tools: Optional[List[Dict]] = None,
        tool_choice: str = "auto",
        **kwargs,
    ):
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        coro = self.async_generate_response(
            messages, response_format=response_format,
            tools=tools, tool_choice=tool_choice, **kwargs,
        )

        if loop and loop.is_running():
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                return pool.submit(asyncio.run, coro).result()
        return asyncio.run(coro)

    # ------------------------------------------------------------------
    # Native async entry point
    # ------------------------------------------------------------------

    async def async_generate_response(
        self,
        messages: List[Dict[str, str]],
        response_format=None,
        tools: Optional[List[Dict]] = None,
        tool_choice: str = "auto",
        **kwargs,
    ):
        system, prompt = self._split_messages(messages)

        if tools:
            return await self._call_with_tools(system, prompt, tools)
        return await self._call_text(system, prompt, response_format)

    # ------------------------------------------------------------------
    # Text/JSON path (no tools)
    # ------------------------------------------------------------------

    async def _call_text(self, system: str, prompt: str, response_format=None) -> str:
        effective_system = system
        if response_format and response_format.get("type") == "json_object":
            suffix = "\n\nRespond with valid JSON only. No markdown fences, no extra text."
            effective_system = (system + suffix) if system else suffix.strip()

        options = ClaudeAgentOptions(
            model=self.config.model,
            max_turns=1,
            permission_mode=getattr(self.config, "permission_mode", "acceptEdits"),
            tools=[],
            cwd=_SDK_CWD,
        )
        if effective_system:
            options.system_prompt = effective_system

        return await self._run_query(prompt, options)

    # ------------------------------------------------------------------
    # Tool calling path — prompt-based + output_format schema enforcement
    # ------------------------------------------------------------------

    async def _call_with_tools(
        self, system: str, prompt: str, tools: List[Dict]
    ) -> Dict[str, Any]:
        tool_instructions = self._build_tool_instructions(tools)
        effective_system = f"{system}\n\n{tool_instructions}" if system else tool_instructions

        options = ClaudeAgentOptions(
            model=self.config.model,
            max_turns=1,
            permission_mode=getattr(self.config, "permission_mode", "acceptEdits"),
            tools=[],
            cwd=_SDK_CWD,
        )
        options.system_prompt = effective_system

        text = await self._run_query(prompt, options)
        return self._parse_tool_response(text, tools)

    # ------------------------------------------------------------------
    # Core query runner
    # ------------------------------------------------------------------

    async def _run_query(self, prompt: str, options: ClaudeAgentOptions) -> str:
        first_text = ""
        result_text = ""
        async for message in query(prompt=prompt, options=options):
            if isinstance(message, ResultMessage):
                if message.result:
                    result_text = message.result
                if message.structured_output is not None:
                    return json.dumps(message.structured_output)
            elif isinstance(message, AssistantMessage):
                for block in message.content:
                    if isinstance(block, TextBlock) and not first_text:
                        first_text = block.text
        # Prefer first TextBlock (contains JSON before any "reconsideration")
        return first_text or result_text

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _split_messages(messages: List[Dict[str, str]]) -> tuple[str, str]:
        system = ""
        user_parts = []
        for msg in messages:
            if msg["role"] == "system":
                system = msg["content"]
            else:
                user_parts.append(msg["content"])
        return system, "\n".join(user_parts)

    @staticmethod
    def _build_tool_instructions(tools: List[Dict]) -> str:
        parts = [
            "You must call one of the tools below.",
            "Respond with a JSON object containing a tool_calls array.",
            "",
            "Available tools:",
        ]
        for t in tools:
            func = t.get("function", t)
            name = func.get("name", "unknown")
            desc = func.get("description", "")
            params = json.dumps(func.get("parameters", {}))
            parts.append(f"  - {name}: {desc}")
            parts.append(f"    Parameters: {params}")
        parts.append("")
        parts.append("If nothing to extract, return: {\"tool_calls\": []}")
        return "\n".join(parts)

    @staticmethod
    def _parse_tool_response(text: str, tools: List[Dict]) -> Dict[str, Any]:
        text = text.strip()

        # Strip markdown fences
        if text.startswith("```"):
            lines = text.split("\n")
            json_lines = []
            inside = False
            for line in lines:
                if line.startswith("```") and not inside:
                    inside = True
                elif line.startswith("```") and inside:
                    break
                elif inside:
                    json_lines.append(line)
            text = "\n".join(json_lines).strip()

        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            parsed = _extract_first_json_object(text)
            if parsed is None:
                logger.error(f"No JSON in tool response: {text[:300]}")
                return {"tool_calls": []}

        if "tool_calls" in parsed:
            normalized = []
            for tc in parsed["tool_calls"]:
                tc = _normalize_tool_call(tc, tools)
                if tc:
                    normalized.append(tc)
            return {"tool_calls": normalized}

        # Response is raw arguments (no tool_calls wrapper)
        if tools and len(tools) == 1:
            func = tools[0].get("function", tools[0])
            name = func.get("name", "unknown")
            return {"tool_calls": [{"name": name, "arguments": parsed}]}

        # Try: tool name used as top-level key, e.g. {"extract_entities": {...}}
        if tools:
            for t in tools:
                func = t.get("function", t)
                name = func.get("name", "unknown")
                if name in parsed:
                    return {"tool_calls": [{"name": name, "arguments": parsed[name]}]}

        return {"tool_calls": []}


def _extract_first_json_object(text: str) -> dict | None:
    """Extract the first complete JSON object from text using brace balancing."""
    start = text.find("{")
    if start < 0:
        return None
    depth = 0
    in_string = False
    escape = False
    for i in range(start, len(text)):
        c = text[i]
        if escape:
            escape = False
            continue
        if c == "\\":
            escape = True
            continue
        if c == '"' and not escape:
            in_string = not in_string
            continue
        if in_string:
            continue
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                try:
                    return json.loads(text[start : i + 1])
                except json.JSONDecodeError:
                    return None
    return None


def _normalize_tool_call(tc: dict, tools: list) -> dict | None:
    """Normalize a tool call dict to {"name": ..., "arguments": ...}."""
    # Standard format
    name = tc.get("name") or tc.get("tool_name") or tc.get("tool")

    # Tool name as key: {"extract_entities": {"entities": [...]}}
    if not name and tools:
        for t in tools:
            func = t.get("function", t)
            fname = func.get("name", "")
            if fname in tc:
                return {"name": fname, "arguments": tc[fname]}

    if not name:
        return None

    args = tc.get("arguments") or tc.get("parameters") or tc.get("input") or {}
    return {"name": name, "arguments": args}
