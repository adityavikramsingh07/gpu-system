"""
agents/specialists/geological_expert.py
======================================
Geological Survey & Mineral Expert DSW Agent utilizing the Parallel StateGraph architecture.
"""

from typing import List
from orchestration.cog.state_schema import DSWWorkerType
from agents.specialists.base_dsw import BaseDSW

class GeologicalExpertDSW(BaseDSW):
    worker_type = DSWWorkerType.GEOLOGICAL_EXPERT
    mcp_server_id = "geological-mcp-server"
    required_scopes = ["read", "execute"]

    def get_system_prompt(self, region_focus: str, material_focus: str) -> str:
        return (
            "You are the Geological Survey & Mineral Expert in a Multi-Agent System building a secure GPU supply chain.\n"
            "Your specific expertise is: Analyzes geological data, mining leases, and mineral grades.\n"
            f"Region: {region_focus}\n"
            f"Material: {material_focus}\n\n"
            "Analyze the data provided by your parallel tool execution accurately and concisely."
        )

    def get_required_tools(self) -> List[str]:
        """Return the exact MCP tool names to be executed in parallel during the Fan-Out phase."""
        return ['query_mining_deposits', 'get_lease_status', 'get_geological_survey', 'get_mineral_grade_report']
