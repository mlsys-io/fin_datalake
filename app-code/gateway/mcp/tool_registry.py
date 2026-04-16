from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable

from mcp.types import Tool


def _identity(arguments: dict[str, Any] | None) -> dict[str, Any]:
    return dict(arguments or {})


@dataclass(frozen=True)
class GatewayMcpTool:
    name: str
    description: str
    input_schema: dict[str, Any]
    domain: str
    action: str
    argument_mapper: Callable[[dict[str, Any]], dict[str, Any]] = field(
        default=_identity,
        repr=False,
        compare=False,
    )

    def to_tool(self) -> Tool:
        return Tool(
            name=self.name,
            description=self.description,
            inputSchema=self.input_schema,
        )

    def map_arguments(self, arguments: dict[str, Any] | None) -> dict[str, Any]:
        return self.argument_mapper(dict(arguments or {}))


class GatewayMcpToolRegistry:
    def __init__(self, tools: Iterable[GatewayMcpTool] | None = None):
        self._tools: dict[str, GatewayMcpTool] = {}
        for tool in tools or []:
            self.register(tool)

    def register(self, tool: GatewayMcpTool) -> None:
        if tool.name in self._tools:
            raise ValueError(f"Duplicate MCP tool registered: {tool.name}")
        self._tools[tool.name] = tool

    def register_many(self, tools: Iterable[GatewayMcpTool]) -> None:
        for tool in tools:
            self.register(tool)

    def list_tools(self) -> list[Tool]:
        return [tool.to_tool() for tool in sorted(self._tools.values(), key=lambda item: item.name)]

    def get(self, name: str) -> GatewayMcpTool | None:
        return self._tools.get(name)

    def names(self) -> list[str]:
        return sorted(self._tools.keys())


def format_mcp_result(data: Any) -> str:
    if isinstance(data, str):
        return data
    return json.dumps(data, indent=2, default=str, ensure_ascii=False)
