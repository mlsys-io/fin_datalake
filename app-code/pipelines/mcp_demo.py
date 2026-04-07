import requests
import json
import os
from loguru import logger

def run_mcp_demo(gateway_url: str = "http://localhost:8000"):
    """
    Demonstrates the Model Context Protocol (MCP) by acting as 
    an external LLM client that leverages tools exposed by the Gateway.
    """
    logger.info("=== 🌉 MODEL CONTEXT PROTOCOL (MCP) DEMO ===")
    logger.info(f"Target Gateway: {gateway_url}")
    
    # In a real environment, the MCP server is typically run via stdio.
    # But because our system exposes the tools over the unified Gateway REST /intent
    # endpoint, we can simulate an MCP LLM's network call simply by invoking the intent router.
    # (The MCP server internally wraps this exact same router logic).
    
    # 1. First Tool Context: Sub-Agent Discovery
    logger.info("\n[Client LLM request] -> Call tool: list_agents()")
    
    payload_list = {
        "action": "list_agents",
        "domain": "agent",
        "parameters": {}
    }
    
    try:
        response = requests.post(f"{gateway_url}/api/v1/intent", json=payload_list, headers={"Authorization": "Bearer test-admin-key"})
        response.raise_for_status()
        agents = response.json().get("data", {})
        logger.success(f"[Gateway tools response] -> Active Agents: {list(agents.keys())}")
    except Exception as e:
        logger.error(f"Failed to list agents. Is the Gateway running? {e}")
        return

    # 2. Second Tool Context: Trading Strategy Extraction
    logger.info("\n[Client LLM request] -> Call tool: chat_agent(StrategyAgent)")
    
    payload_chat = {
        "action": "chat_agent",
        "domain": "agent",
        "parameters": {
            "agent_name": "strategy",
            # A blank payload forces the StrategyAgent to pull the latest ContextStore signal
            "payload": {"fetch_latest": True}
        }
    }
    
    logger.info("Routing through Gateway to Ray Serve...")
    try:
        response = requests.post(f"{gateway_url}/api/v1/intent", json=payload_chat, headers={"Authorization": "Bearer test-admin-key"}, timeout=15.0)
        signal = response.json().get("data", {})
        if "action" in signal:
            logger.success(f"[Gateway tools response] -> Strategy Signal: {signal.get('action')} (Conf: {signal.get('confidence')})")
        else:
             logger.warning(f"[Gateway tools response] -> No active signal returned. Execute 'market_pulse_demo.py' first.")
    except Exception as e:
        logger.error(f"Agent chat tool failed: {e}")

    # 3. Third Tool Context: Zero-Copy Data Lake Access
    logger.info("\n[Client LLM request] -> Call tool: query_data(Delta Lake)")
    
    payload_query = {
        "action": "query_data",
        "domain": "data",
        "parameters": {
            "sql_query": "SELECT * FROM demo_bronze_news LIMIT 2",
            "format": "json"
        }
    }
    
    try:
        response = requests.post(f"{gateway_url}/api/v1/intent", json=payload_query, headers={"Authorization": "Bearer test-admin-key"})
        data = response.json().get("data", {})
        if data:
             logger.success(f"[Gateway tools response] -> Successfully read {len(data)} rows from Delta Lake.")
             print(json.dumps(data, indent=2))
        else:
             logger.warning("No data returned. Run the ingest pipeline first.")
    except requests.exceptions.HTTPError:
        # SQL query on an unconfigured Delta table might fail
        logger.warning("Data Query tool executed. Ensure Delta Lake is populated and hive metastore is running for full SQL support.")
    except Exception as e:
         logger.error(f"Data query tool failed: {e}")
            
    logger.info("\n=== MCP Demo Complete ===")
    logger.info("Proof Value: Any external LLM can dynamically discover agents, trigger analysis, and query raw data through the exact same JSON-RPC interface.")

if __name__ == "__main__":
    run_mcp_demo()
