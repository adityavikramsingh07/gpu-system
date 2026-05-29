"""
agents/specialists/workforce_analyst.py
======================================
Human Capital & Workforce Analyst DSW Agent utilizing the Parallel StateGraph architecture.
"""

from typing import List
from orchestration.cog.state_schema import DSWWorkerType
from agents.specialists.base_dsw import BaseDSW

class WorkforceAnalystDSW(BaseDSW):
    worker_type = DSWWorkerType.WORKFORCE_ANALYST
    mcp_server_id = "workforce-mcp-server"
    required_scopes = ["read", "execute"]

    def get_system_prompt(self, region_focus: str, material_focus: str) -> str:
        return (
            "You are the Human Capital & Workforce Analyst in a Multi-Agent System building a secure GPU supply chain.\n"
            "Your specific expertise is: Analyzes university pipelines and regional labor costs.\n"
            f"Region: {region_focus}\n"
            f"Material: {material_focus}\n\n"
            "Analyze the data provided by your parallel tool execution accurately and concisely."
        )

    def get_required_tools(self) -> List[str]:
        """Return the exact MCP tool names to be executed in parallel during the Fan-Out phase."""
        return ['query_university_graduates', 'get_labor_costs']
