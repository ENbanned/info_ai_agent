import os
from typing import Dict, List, Optional, Union

try:
    import anthropic
except ImportError:
    raise ImportError("The 'anthropic' library is required. Please install it using 'pip install anthropic'.")

from mem0.configs.llms.anthropic import AnthropicConfig
from mem0.configs.llms.base import BaseLlmConfig
from mem0.llms.base import LLMBase


def _convert_openai_tools_to_anthropic(tools):
    if not tools:
        return tools
    converted = []
    for tool in tools:
        if tool.get("type") == "function" and "function" in tool:
            func = tool["function"]
            anthropic_tool = {
                "name": func["name"],
                "description": func.get("description", ""),
                "input_schema": func.get("parameters", {"type": "object", "properties": {}}),
            }
            converted.append(anthropic_tool)
        else:
            converted.append(tool)
    return converted


def _convert_tool_choice_to_anthropic(tool_choice):
    if isinstance(tool_choice, dict):
        return tool_choice
    if isinstance(tool_choice, str):
        mapping = {
            "auto": {"type": "auto"},
            "required": {"type": "any"},
            "any": {"type": "any"},
        }
        return mapping.get(tool_choice, {"type": "auto"})
    return {"type": "auto"}


def _parse_anthropic_response(response):
    content = response.content
    tool_calls = []
    text_parts = []

    for block in content:
        if block.type == "tool_use":
            tool_calls.append({
                "name": block.name,
                "arguments": block.input,
            })
        elif block.type == "text":
            text_parts.append(block.text)

    if tool_calls:
        result = {"tool_calls": tool_calls}
        if text_parts:
            result["content"] = "\n".join(text_parts)
        return result

    return "\n".join(text_parts) if text_parts else ""


class AnthropicLLM(LLMBase):
    def __init__(self, config: Optional[Union[BaseLlmConfig, AnthropicConfig, Dict]] = None):
        if config is None:
            config = AnthropicConfig()
        elif isinstance(config, dict):
            config = AnthropicConfig(**config)
        elif isinstance(config, BaseLlmConfig) and not isinstance(config, AnthropicConfig):
            config = AnthropicConfig(
                model=config.model,
                temperature=config.temperature,
                api_key=config.api_key,
                max_tokens=config.max_tokens,
                top_p=config.top_p,
                top_k=config.top_k,
                enable_vision=config.enable_vision,
                vision_details=config.vision_details,
                http_client_proxies=config.http_client,
            )

        super().__init__(config)

        if not self.config.model:
            self.config.model = "claude-sonnet-4-6"

        api_key = self.config.api_key or os.getenv("ANTHROPIC_API_KEY")
        self.client = anthropic.Anthropic(api_key=api_key)

    def generate_response(
        self,
        messages: List[Dict[str, str]],
        response_format=None,
        tools: Optional[List[Dict]] = None,
        tool_choice: str = "auto",
        **kwargs,
    ):
        system_message = ""
        filtered_messages = []
        for message in messages:
            if message["role"] == "system":
                system_message = message["content"]
            else:
                filtered_messages.append(message)

        params = {
            "model": self.config.model,
            "messages": filtered_messages,
            "max_tokens": self.config.max_tokens,
            "temperature": self.config.temperature,
        }

        if system_message:
            params["system"] = system_message

        if tools:
            params["tools"] = _convert_openai_tools_to_anthropic(tools)
            params["tool_choice"] = _convert_tool_choice_to_anthropic(tool_choice)

        response = self.client.messages.create(**params)
        result = _parse_anthropic_response(response)

        # When tools were requested, always return dict so callers can use .get()
        if tools and isinstance(result, str):
            return {"tool_calls": [], "content": result}

        return result
