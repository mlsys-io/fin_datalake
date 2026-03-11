import pytest
from unittest.mock import MagicMock, patch
from etl.agents.base import BaseAgent
from etl.agents.langchain_adapter import LangChainAgent
from etl.agents.tools import TimescaleTool, MilvusTool

# --- BaseAgent Tests ---

class MockAgent(BaseAgent):
    """Concrete impl for testing."""
    def build_executor(self):
        # Return a simple mock callable
        mock = MagicMock()
        mock.return_value = "Success"
        mock.invoke.return_value = "Success" # Support both protocols
        return mock

def test_base_agent_ask_callable():
    """Test standard call protocol."""
    agent = MockAgent()
    # Force setup mock logic to be a simple function
    agent.executor = MagicMock(return_value="CallableResult")
    # Remove invoke attribute to force callable path
    del agent.executor.invoke 
    
    result = agent.ask("input")
    assert result == "CallableResult"
    agent.executor.assert_called_with("input")

def test_base_agent_ask_invoke_protocol():
    """Test Runnable protocol (LangChain style)."""
    agent = MockAgent()
    # Initial setup happens lazy
    assert agent.executor is None
    
    # helper invoke mock
    mock_ex = MagicMock()
    mock_ex.invoke.return_value = "InvokeResult"
    
    with patch.object(agent, 'build_executor', return_value=mock_ex):
        result = agent.ask("input2")
        assert result == "InvokeResult"
        mock_ex.invoke.assert_called_with("input2")

# --- LangChainAgent Tests ---

class MockLCAgent(LangChainAgent):
    def build_executor(self):
        m = MagicMock()
        m.invoke.return_value = {"output": "AgentAnswer"}
        return m

def test_langchain_agent_pass_through():
    """Test that it passes input dict/str correctly."""
    agent = MockLCAgent()
    
    # Test Dict
    res = agent.ask({"q": "hi"})
    assert res == {"output": "AgentAnswer"}
    
    # Test Str
    res2 = agent.ask("raw string")
    assert res2 == {"output": "AgentAnswer"}

# --- Tool Tests ---

def test_timescale_tool():
    """Test SQL tool wrapper logic (MOCKED DB)."""
    mock_conf = MagicMock()
    mock_conf.host = "h"
    tool = TimescaleTool(mock_conf)
    
    with patch("psycopg2.connect") as mock_conn:
        mock_cursor = MagicMock()
        mock_conn.return_value.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [{"col": 1}]
        
        res = tool.run("SELECT *")
        assert res == [{"col": 1}]
        mock_cursor.execute.assert_called_with("SELECT *")

def test_milvus_tool_search():
    """Test Vector Search logic (MOCKED DB)."""
    mock_conf = MagicMock()
    mock_conf.uri = "u"
    
    # Dummy embedding func
    def fake_embed(text):
        return [0.1, 0.2]
        
    tool = MilvusTool(mock_conf, embedding_func=fake_embed)
    
    with patch("pymilvus.connections.connect"), \
         patch("pymilvus.Collection") as MockColl:
         
        mock_c_instance = MockColl.return_value
        # Mock search result structure
        mock_hit = MagicMock()
        mock_hit.id = 123
        mock_hit.distance = 0.5
        mock_hit.entity.get.return_value = "content stuff"
        
        mock_c_instance.search.return_value = [[mock_hit]]
        
        results = tool.search("query")
        
        assert len(results) == 1
        assert results[0]["id"] == 123
        assert results[0]["entity"]["content"] == "content stuff"
