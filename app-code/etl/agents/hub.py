"""
AgentHub - Central service discovery registry for all agents.

The Hub remains the control-plane registry for agent discovery, but now supports
both legacy string capabilities and richer typed capability specifications.
"""

from typing import Dict, List, Any
from dataclasses import dataclass, field
from datetime import datetime
from collections import defaultdict
import ray


def _normalize_text_list(values: Any) -> List[str]:
    if values is None:
        return []
    if isinstance(values, str):
        values = [values]
    result = []
    for value in values:
        text = str(value).strip()
        if text:
            result.append(text.lower())
    return result


def _normalize_capability_spec(entry: Any) -> Dict[str, Any]:
    if isinstance(entry, str):
        capability_id = entry.strip()
        return {
            "id": capability_id,
            "description": capability_id,
            "tags": [],
            "aliases": [],
            "input_type": None,
            "output_type": None,
            "interaction_mode": "invoke",
        }

    if not isinstance(entry, dict):
        raise TypeError(f"Unsupported capability entry: {type(entry)}")

    capability_id = str(entry.get("id") or entry.get("name") or "").strip()
    if not capability_id:
        raise ValueError("Capability spec requires a non-empty 'id'.")

    return {
        "id": capability_id,
        "description": str(entry.get("description") or capability_id),
        "tags": _normalize_text_list(entry.get("tags")),
        "aliases": _normalize_text_list(entry.get("aliases")),
        "input_type": entry.get("input_type"),
        "output_type": entry.get("output_type"),
        "interaction_mode": str(entry.get("interaction_mode") or "invoke"),
    }


def _normalize_capability_specs(capabilities: List[Any]) -> List[Dict[str, Any]]:
    normalized = []
    seen = set()
    for entry in capabilities or []:
        spec = _normalize_capability_spec(entry)
        key = spec["id"].lower()
        if key in seen:
            continue
        seen.add(key)
        normalized.append(spec)
    return normalized


@dataclass
class AgentInfo:
    """Metadata about a registered agent."""

    name: str
    capabilities: List[str]
    capability_specs: List[Dict[str, Any]]
    registered_at: datetime = field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = field(default_factory=dict)


@ray.remote
class AgentHub:
    """
    Central discovery registry for the multi-agent system.

    Handles registration plus typed capability discovery. Interaction is still
    performed through Ray Serve handles or HTTP.
    """

    def __init__(self):
        from etl.utils.logging import setup_logging
        from loguru import logger

        setup_logging(component="hub")

        self._agents: Dict[str, AgentInfo] = {}
        self._capability_index: Dict[str, List[str]] = defaultdict(list)
        self._stats = {
            "total_registrations": 0,
        }
        logger.info("AgentHub initialized")

    def register(
        self,
        name: str,
        capabilities: List[Any],
        metadata: Dict = None,
    ) -> bool:
        """
        Register an agent with legacy string capabilities or richer capability specs.
        """
        from loguru import logger

        if name in self._agents:
            self._remove_from_capability_index(name)
            logger.info(f"Agent '{name}' re-registering (updating capabilities)")

        capability_specs = _normalize_capability_specs(capabilities)
        legacy_capabilities = [spec["id"] for spec in capability_specs]

        self._agents[name] = AgentInfo(
            name=name,
            capabilities=legacy_capabilities,
            capability_specs=capability_specs,
            metadata=metadata or {},
        )

        for spec in capability_specs:
            for token in [spec["id"], *spec.get("aliases", [])]:
                token_key = str(token).strip().lower()
                if token_key and name not in self._capability_index[token_key]:
                    self._capability_index[token_key].append(name)

        logger.info(f"Registered '{name}' with capabilities: {legacy_capabilities}")
        self._stats["total_registrations"] += 1
        return True

    def unregister(self, name: str) -> bool:
        if name not in self._agents:
            return False

        self._remove_from_capability_index(name)
        del self._agents[name]

        from loguru import logger

        logger.info(f"Unregistered '{name}'")
        return True

    def _remove_from_capability_index(self, name: str):
        for _, agents in self._capability_index.items():
            if name in agents:
                agents.remove(name)

    def find_by_capability(self, capability: str) -> List[str]:
        """Find all agents matching a capability id or alias."""
        return list(self._capability_index.get(str(capability).strip().lower(), []))

    def query_agents(
        self,
        capability: str = None,
        required_tags: List[str] = None,
        interaction_mode: str = None,
        input_type: str = None,
        output_type: str = None,
        alive_only: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        Query agents using exact-match constraints over typed capability specs.
        """
        required_tags_normalized = set(_normalize_text_list(required_tags))
        capability_key = str(capability).strip().lower() if capability else None
        interaction_mode_key = str(interaction_mode).strip().lower() if interaction_mode else None
        input_type_key = str(input_type).strip().lower() if input_type else None
        output_type_key = str(output_type).strip().lower() if output_type else None

        matches = []
        for info in self._agents.values():
            alive = self._is_alive(info.name)
            if alive_only and not alive:
                continue

            matching_specs = []
            for spec in info.capability_specs:
                candidate_tokens = {spec["id"].lower(), *spec.get("aliases", [])}
                if capability_key and capability_key not in candidate_tokens:
                    continue
                if interaction_mode_key and str(spec.get("interaction_mode") or "").lower() != interaction_mode_key:
                    continue
                if input_type_key and str(spec.get("input_type") or "").lower() != input_type_key:
                    continue
                if output_type_key and str(spec.get("output_type") or "").lower() != output_type_key:
                    continue
                if required_tags_normalized and not required_tags_normalized.issubset(set(spec.get("tags", []))):
                    continue
                matching_specs.append(spec)

            if capability_key and not matching_specs:
                continue
            if required_tags_normalized and not matching_specs:
                continue
            if interaction_mode_key and not matching_specs:
                continue
            if input_type_key and not matching_specs:
                continue
            if output_type_key and not matching_specs:
                continue

            matches.append(
                {
                    "name": info.name,
                    "capabilities": info.capabilities,
                    "capability_specs": matching_specs or info.capability_specs,
                    "registered_at": info.registered_at.isoformat(),
                    "metadata": info.metadata,
                    "alive": alive,
                }
            )

        return matches

    def list_agents(self) -> List[Dict]:
        return [
            {
                "name": info.name,
                "capabilities": info.capabilities,
                "capability_specs": info.capability_specs,
                "registered_at": info.registered_at.isoformat(),
                "metadata": info.metadata,
                "alive": self._is_alive(info.name),
            }
            for info in self._agents.values()
        ]

    def get_capabilities(self) -> List[str]:
        return [
            cap
            for cap, agents in self._capability_index.items()
            if agents
        ]

    def call(self, name: str, payload: Any) -> Any:
        from etl.runtime import resolve_serve_response

        handle = self._get_handle(name)
        if handle is None:
            raise ValueError(f"Agent '{name}' is not registered or not alive")
        return resolve_serve_response(handle.invoke.remote(payload))

    def call_by_capability(
        self,
        capability: str,
        payload: Any,
        required_tags: List[str] = None,
        interaction_mode: str = None,
        input_type: str = None,
        output_type: str = None,
    ) -> Any:
        matches = self.query_agents(
            capability=capability,
            required_tags=required_tags,
            interaction_mode=interaction_mode,
            input_type=input_type,
            output_type=output_type,
            alive_only=True,
        )
        if not matches:
            raise ValueError(f"No agent found with capability: {capability}")
        return self.call(matches[0]["name"], payload)

    def notify(self, name: str, event: Dict[str, Any]) -> bool:
        handle = self._get_handle(name)
        if handle is None:
            return False
        from etl.runtime import resolve_serve_response

        resolve_serve_response(handle.handle_event.remote(event))
        return True

    def notify_capability(
        self,
        capability: str,
        event: Dict[str, Any],
        required_tags: List[str] = None,
        interaction_mode: str = None,
        input_type: str = None,
        output_type: str = None,
    ) -> int:
        matches = self.query_agents(
            capability=capability,
            required_tags=required_tags,
            interaction_mode=interaction_mode,
            input_type=input_type,
            output_type=output_type,
            alive_only=True,
        )
        delivered = 0
        for match in matches:
            if self.notify(match["name"], event):
                delivered += 1
        return delivered

    def is_alive(self, name: str) -> bool:
        return self._is_alive(name)

    def health_check(self) -> Dict[str, bool]:
        return {name: self._is_alive(name) for name in self._agents}

    def get_stats(self) -> Dict:
        return {
            "registered_agents": len(self._agents),
            "capabilities": len([c for c, a in self._capability_index.items() if a]),
            **self._stats,
        }

    def _get_handle(self, name: str):
        import ray.serve as serve
        try:
            return serve.get_app_handle(name)
        except Exception:
            self.unregister(name)
            return None

    def _is_alive(self, name: str) -> bool:
        import ray.serve as serve
        try:
            serve.get_app_handle(name)
            return True
        except (ValueError, KeyError, RuntimeError):
            return False


def get_hub() -> ray.actor.ActorHandle:
    try:
        return ray.get_actor("AgentHub")
    except ValueError:
        try:
            return AgentHub.options(name="AgentHub", lifetime="detached").remote()
        except ValueError:
            return ray.get_actor("AgentHub")
