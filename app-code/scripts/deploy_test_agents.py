"""
Deploy dummy agents for testing typed capability discovery and direct delegation.

Usage:
    cd app-code
    uv run python scripts/deploy_test_agents.py
"""

from __future__ import annotations

import os
import time

import ray

from agents.dummy_agents import ForecastAgent, RouterAgent, SentimentModelAgent, SupportAgent
from etl.agents.hub import get_hub
from etl.runtime import ensure_ray


def main():
    ray_address = os.getenv("RAY_ADDRESS", "auto")
    print(f"Connecting to Ray at: {ray_address}")
    ensure_ray(address=ray_address)

    deployments = [
        ("SupportAgent", SupportAgent),
        ("SentimentModel-1", SentimentModelAgent),
        ("ForecastModel-1", ForecastAgent),
        ("RouterAgent", RouterAgent),
    ]

    handles = {}
    for name, agent_cls in deployments:
        print(f"Deploying {name}...")
        handles[name] = agent_cls.deploy(name=name)

    time.sleep(2)

    hub = get_hub()
    agents = ray.get(hub.list_agents.remote())

    print("\nRegistered agents:")
    for agent in agents:
        print(f"- {agent['name']} | alive={agent['alive']} | capabilities={agent['capabilities']}")
        for spec in agent.get("capability_specs", []):
            print(
                f"    -> {spec['id']} | mode={spec.get('interaction_mode')} "
                f"| input={spec.get('input_type')} | output={spec.get('output_type')} | tags={spec.get('tags')}"
            )

    print("\nSmoke tests:")
    router = handles["RouterAgent"]
    direct_result = ray.get(router.invoke.remote({"action": "direct_support", "message": "Gateway returned 503"}))
    sentiment_result = ray.get(router.invoke.remote({"action": "score_sentiment", "text": "Bitcoin rally beats expectations"}))
    forecast_result = ray.get(router.invoke.remote({"action": "forecast", "values": [100, 103, 106]}))

    print(f"- Direct delegation: {direct_result}")
    print(f"- Capability delegation: {sentiment_result}")
    print(f"- Forecast delegation: {forecast_result}")

    print("\nGateway examples:")
    print('- Chat-capable agent: {"domain":"agent","action":"chat","parameters":{"agent_name":"SupportAgent","message":"hello"}}')
    print('- Generic invoke: {"domain":"agent","action":"invoke","parameters":{"agent_name":"SentimentModel-1","payload":"market rally gains strength"}}')


if __name__ == "__main__":
    main()
