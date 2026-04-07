"""
Tests for the AgentAdapter.

Focuses on durable catalog fallback and live runtime synchronization behavior.
"""

import pytest
from unittest.mock import AsyncMock, patch

from gateway.adapters.agent import AgentAdapter
from gateway.models.intent import UserIntent
from gateway.models.user import User


def make_intent(action: str, **params) -> UserIntent:
    return UserIntent(
        domain="agent",
        action=action,
        parameters=params,
        user_id="test",
        roles=["Admin"],
    )


ADMIN = User(username="admin", hashed_password="x", role_names=["Admin"])


class _MockSession:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


@pytest.mark.asyncio
async def test_list_agents_falls_back_to_catalog_when_hub_unavailable():
    adapter = AgentAdapter()
    intent = make_intent("list")

    catalog_agents = [
        {
            "name": "SupportAgent",
            "capabilities": ["chat.support.respond"],
            "capability_specs": [{"id": "chat.support.respond", "description": "Support agent"}],
            "metadata": {"class": "SupportAgent"},
            "registered_at": None,
            "last_seen_at": None,
            "alive": False,
            "source": "catalog",
        }
    ]

    with patch.object(adapter, "_fetch_agents_from_hub", AsyncMock(side_effect=RuntimeError("ray down"))), \
         patch("gateway.adapters.agent.AsyncSessionLocal", return_value=_MockSession()), \
         patch("gateway.adapters.agent.crud.list_agent_definitions", AsyncMock(return_value=catalog_agents)), \
         patch("gateway.adapters.agent.crud.upsert_agent_definition", AsyncMock()) as mock_upsert:
        result = await adapter.execute(ADMIN, intent)

    assert result["available"] is False
    assert result["agents"] == catalog_agents
    assert "AgentHub unavailable" in result["detail"]
    mock_upsert.assert_not_awaited()


@pytest.mark.asyncio
async def test_list_agents_syncs_live_agents_into_catalog():
    adapter = AgentAdapter()
    intent = make_intent("list")

    live_agents = [
        {
            "name": "SupportAgent",
            "capabilities": ["chat.support.respond"],
            "capability_specs": [{"id": "chat.support.respond", "description": "Live support agent"}],
            "metadata": {"class": "SupportAgent", "app_name": "SupportAgent"},
            "registered_at": "2026-04-02T10:00:00+00:00",
            "alive": True,
        }
    ]
    catalog_agents = [
        {
            "name": "SupportAgent",
            "capabilities": ["chat.support.respond"],
            "capability_specs": [{"id": "chat.support.respond", "description": "Catalog support agent"}],
            "metadata": {"class": "SupportAgent"},
            "registered_at": "2026-04-02T10:00:00+00:00",
            "last_seen_at": "2026-04-02T10:05:00+00:00",
            "alive": False,
            "source": "catalog",
        }
    ]

    with patch.object(adapter, "_fetch_agents_from_hub", AsyncMock(return_value=live_agents)), \
         patch("gateway.adapters.agent.AsyncSessionLocal", return_value=_MockSession()), \
         patch("gateway.adapters.agent.crud.list_agent_definitions", AsyncMock(return_value=catalog_agents)), \
         patch("gateway.adapters.agent.crud.upsert_agent_definition", AsyncMock()) as mock_upsert:
        result = await adapter.execute(ADMIN, intent)

    assert result["available"] is True
    assert result["detail"] is None
    assert len(result["agents"]) == 1
    assert result["agents"][0]["name"] == "SupportAgent"
    assert result["agents"][0]["alive"] is True
    assert result["agents"][0]["source"] == "catalog+runtime"
    mock_upsert.assert_awaited_once()
