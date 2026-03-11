from typing import Any
import os

from langchain.agents import initialize_agent, AgentType, Tool
from langchain_openai import ChatOpenAI

from etl.agents import LangChainAgent
from etl.agents.tools import TimescaleTool
from etl.io.sinks.timescaledb import TimescaleDBSink

class MarketAnalystAgent(LangChainAgent):
    """
    An Agent that can analyze market data stored in TimescaleDB.
    """
    
    def build_executor(self):
        # 1. Config Database Connection
        # In prod, fetch these from secrets/env vars
        db_config = TimescaleDBSink(
            host=os.getenv("DB_HOST", "localhost"),
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASS", "password"),
            database=os.getenv("DB_NAME", "financial_db"),
            table_name="market_data"
        )
        
        # 2. Initialize Tool
        ts_tool = TimescaleTool(db_config)
        
        tools = [
            Tool(
                name="QueryMarketData",
                func=ts_tool.run,
                description="Useful for getting price history. Input should be a valid SQL query string."
            )
        ]
        
        # 3. Initialize LLM
        # Assumes OPENAI_API_KEY is set in environment
        llm = ChatOpenAI(temperature=0, model_name="gpt-4")
        
        # 4. Create Executor
        return initialize_agent(
            tools=tools, 
            llm=llm, 
            agent=AgentType.CHAT_ZERO_SHOT_REACT_DESCRIPTION, 
            verbose=True,
            handle_parsing_errors=True
        )

# Example Usage helper
def deploy_analyst():
    """Deploy as a named Ray Actor — returns SyncHandle."""
    return MarketAnalystAgent.deploy(name="MarketAnalyst", num_cpus=1)

