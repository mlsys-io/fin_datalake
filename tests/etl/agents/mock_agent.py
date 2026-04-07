from etl.agents.base import BaseAgent

class MockAgent(BaseAgent):
    def build_executor(self):
        # A mock executor that just returns the payload capitalized
        def executor(payload):
            return f"MOCK_{payload.upper()}"
        return executor
