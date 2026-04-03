"""
Base Agent class for request-driven AI agents deployed with Ray Serve.
"""
from typing import Any, Dict, List, Optional
from abc import ABC, abstractmethod

from etl.agents.mixins import ConversationManagerMixin


class BaseAgent(ABC, ConversationManagerMixin):
    """
    The base runtime container for Ray Serve-deployed agents.

    Agents are request-driven services. The primary execution path is `invoke()`
    and the asynchronous event path is `handle_event()`.
    """

    CAPABILITIES: list = []
    CAPABILITY_SPECS: list = []

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.name = self.__class__.__name__
        self.app_name: Optional[str] = None
        self.config = config or {}
        self.executor = None

    @classmethod
    def deploy(
        cls,
        name: Optional[str] = None,
        num_replicas: int = 1,
        num_cpus: float = 0.5,
        config: Optional[Dict[str, Any]] = None,
        **serve_options,
    ):
        import ray
        import ray.serve as serve
        from loguru import logger
        from etl.runtime import ensure_ray, resolve_serve_response

        ensure_ray()

        actor_name = name or cls.__name__

        deployment_cls = serve.deployment(
            name=actor_name,
            num_replicas=num_replicas,
            ray_actor_options={"num_cpus": num_cpus},
            **serve_options,
        )(cls)

        handle = serve.run(deployment_cls.bind(config=config), name=actor_name)
        logger.info(f"Deployed Agent '{actor_name}' to Ray Serve")

        resolve_serve_response(handle.set_app_name.remote(actor_name))
        resolve_serve_response(handle.setup.remote())

        return handle

    @classmethod
    def connect(cls, name: str):
        from etl.runtime import ensure_ray
        import ray.serve as serve

        ensure_ray()
        return serve.get_app_handle(name)

    def set_app_name(self, app_name: str):
        self.app_name = app_name

    def setup(self):
        if self.executor is not None:
            return

        from etl.utils.logging import setup_logging
        from loguru import logger

        setup_logging(component="agent")
        logger.info(f"[{self.name}] Setting up agent runtime...")
        self.executor = self.build_executor()

        if self.get_capability_specs():
            self._register_with_hub()

        logger.info(f"[{self.name}] Ready.")

    def _register_with_hub(self):
        import ray
        from loguru import logger

        registered_name = self.app_name or self.name

        try:
            from etl.agents.hub import get_hub

            hub = get_hub()
            ray.get(
                hub.register.remote(
                    name=registered_name,
                    capabilities=self.get_capability_specs(),
                    metadata={
                        "class": self.__class__.__name__,
                        "app_name": registered_name,
                        "interaction_modes": self.get_interaction_modes(),
                    },
                )
            )
            logger.info(
                f"[{self.name}] Registered as '{registered_name}' in AgentHub "
                f"(capabilities: {self.get_capability_specs()})"
            )
        except Exception as e:
            logger.warning(f"[{self.name}] Failed to register with AgentHub: {e}")

    @classmethod
    def get_capability_specs(cls) -> List[Dict[str, Any]]:
        if getattr(cls, "CAPABILITY_SPECS", None):
            return list(cls.CAPABILITY_SPECS)
        return list(getattr(cls, "CAPABILITIES", []))

    @classmethod
    def get_interaction_modes(cls) -> List[str]:
        modes = []
        for entry in cls.get_capability_specs():
            if isinstance(entry, dict):
                mode = str(entry.get("interaction_mode") or "").strip()
                if mode and mode not in modes:
                    modes.append(mode)
        if not modes and getattr(cls, "CAPABILITIES", None):
            modes.append("invoke")
        return modes

    def shutdown(self):
        """
        Remove the Ray Serve deployment and run final cleanup.
        """
        from etl.runtime import ensure_ray
        import ray.serve as serve

        ensure_ray()
        serve.delete(self.app_name or self.name)
        self.on_stop()

    def on_stop(self):
        """
        Lifecycle cleanup hook for agent-specific resource release.
        """
        self._deregister_from_hub()

    def _deregister_from_hub(self):
        import ray
        from loguru import logger

        registered_name = self.app_name or self.name
        try:
            from etl.agents.hub import get_hub

            hub = get_hub()
            ray.get(hub.unregister.remote(registered_name))
            logger.info(f"[{self.name}] Deregistered '{registered_name}' from AgentHub")
        except Exception:
            pass

    @abstractmethod
    def build_executor(self) -> Any:
        pass

    def invoke(self, payload: Any, session_id: Optional[str] = None) -> Any:
        """
        Primary synchronous request/response interaction for agents.
        """
        from loguru import logger

        if not self.executor:
            self.setup()

        input_data = payload
        history = None

        if session_id is not None:
            history = self.load_conversation_state(session_id)
            user_entry = self._coerce_conversation_entry(payload)
            if user_entry is not None:
                history.append(user_entry)
                history = self.trim_history(history)
                input_data = history

        try:
            if hasattr(self.executor, "invoke"):
                result = self.executor.invoke(input_data)
            elif callable(self.executor):
                result = self.executor(input_data)
            else:
                raise TypeError(f"Executor {type(self.executor)} is not callable and has no 'invoke' method.")

            if session_id is not None and history is not None:
                history.append({"role": "assistant", "content": self._extract_response_text(result)})
                self.save_conversation_state(session_id, history)

            return result
        except Exception as e:
            logger.error(f"[{self.name}] Error during invoke(): {e}")
            raise

    def chat(self, message: str, session_id: Optional[str] = None) -> Any:
        """
        Convenience wrapper for chat-capable agents.
        """
        return self.invoke(message, session_id=session_id)

    def ask(self, payload: Any, session_id: Optional[str] = None) -> Any:
        """
        Compatibility alias used by demos and higher-level orchestration code.
        """
        return self.invoke(payload, session_id=session_id)

    def handle_event(self, event: Dict[str, Any]):
        """
        Handle an asynchronous event envelope.
        """
        from loguru import logger

        try:
            event_type = str(event.get("type") or event.get("topic") or "").strip()
            payload = event.get("payload", {})

            if event_type:
                handler_name = f"handle_{event_type.replace('-', '_').replace('.', '_')}"
                handler = getattr(self, handler_name, None)
                if handler and callable(handler):
                    logger.debug(f"[{self.name}] Routing event to {handler_name}")
                    return handler(payload, event)

            return self.on_event(event)
        except Exception as e:
            logger.error(f"[{self.name}] Error processing event: {e}")
            raise

    def on_event(self, event: Dict[str, Any]):
        from loguru import logger

        logger.info(f"[{self.name}] Received event: {event.get('type') or event.get('topic')} from {event.get('sender')}")
        return {"accepted": True}

    def delegate(
        self,
        capability: str,
        payload: Any,
        retry_on_failure: bool = True,
        max_retries: int = 3,
        required_tags: Optional[List[str]] = None,
        interaction_mode: Optional[str] = None,
        input_type: Optional[str] = None,
        output_type: Optional[str] = None,
        selection_strategy: str = "random",
    ) -> Any:
        import ray
        import random
        from loguru import logger
        from etl.agents.hub import get_hub

        logger.info(f"[{self.name}] Delegating capability '{capability}'")
        hub = get_hub()

        candidates = ray.get(
            hub.query_agents.remote(
                capability=capability,
                required_tags=required_tags,
                interaction_mode=interaction_mode,
                input_type=input_type,
                output_type=output_type,
                alive_only=True,
            )
        )

        if not candidates:
            raise ValueError(f"No agent found with capability: {capability}")

        self_name = self.app_name or self.name
        agent_names = [candidate["name"] for candidate in candidates if candidate["name"] != self_name]
        if not agent_names:
            raise ValueError(f"No peer agent available for capability: {capability}")

        if selection_strategy == "first":
            ordered_targets = agent_names
        else:
            ordered_targets = list(agent_names)
            random.shuffle(ordered_targets)

        attempts = min(len(ordered_targets), max_retries) if retry_on_failure else 1
        last_error = None
        for i in range(attempts):
            target_name = ordered_targets[i]
            try:
                return self.delegate_to(target_name, payload)
            except Exception as e:
                logger.warning(f"[{self.name}] Delegation to {target_name} failed: {e}")
                last_error = e

        raise RuntimeError(f"All agents for capability '{capability}' failed. Last error: {last_error}")

    def delegate_to(self, agent_name: str, payload: Any) -> Any:
        import ray.serve as serve
        from etl.runtime import resolve_serve_response

        handle = serve.get_app_handle(agent_name)
        return resolve_serve_response(handle.invoke.remote(payload))

    def _coerce_conversation_entry(self, payload: Any) -> Optional[Dict[str, str]]:
        if isinstance(payload, str):
            return {"role": "user", "content": payload}
        if isinstance(payload, dict):
            if "content" in payload:
                return {"role": "user", "content": str(payload["content"])}
            if "message" in payload:
                return {"role": "user", "content": str(payload["message"])}
            if "question" in payload:
                return {"role": "user", "content": str(payload["question"])}
        return None

    def _extract_response_text(self, result: Any) -> str:
        if hasattr(result, "content"):
            return str(result.content)
        if isinstance(result, dict):
            if "content" in result:
                return str(result["content"])
            if "message" in result:
                return str(result["message"])
        return str(result)
