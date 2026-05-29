"""
agents/specialists/mining_lease_analyst.py
======================================
Legal & Land Acquisition Analyst DSW Agent utilizing the Parallel StateGraph architecture.
"""

from typing import List
from orchestration.cog.state_schema import DSWWorkerType
from agents.specialists.base_dsw import BaseDSW

class MiningLeaseAnalystDSW(BaseDSW):
    worker_type = DSWWorkerType.MINING_LEASE_ANALYST
    mcp_server_id = "mining-mcp-server"
    required_scopes = ["read", "execute"]

    def get_system_prompt(self, region_focus: str, material_focus: str) -> str:
        return (
            "You are the Legal & Land Acquisition Analyst in a Multi-Agent System building a secure GPU supply chain.\n"
            "Your specific expertise is: Handles legal disputes and land registry checks.\n"
            f"Region: {region_focus}\n"
            f"Material: {material_focus}\n\n"
            "Analyze the data provided by your parallel tool execution accurately and concisely."
        )

    def get_required_tools(self) -> List[str]:
        """Return the exact MCP tool names to be executed in parallel during the Fan-Out phase."""
        return ['check_legal_disputes', 'query_land_registry']
