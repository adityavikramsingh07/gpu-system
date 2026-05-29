"""
agents/specialists/environmental_compliance.py
======================================
Environmental Compliance Officer DSW Agent utilizing the Parallel StateGraph architecture.
"""

from typing import List
from orchestration.cog.state_schema import DSWWorkerType
from agents.specialists.base_dsw import BaseDSW

class EnvironmentalComplianceDSW(BaseDSW):
    worker_type = DSWWorkerType.ENVIRONMENTAL_COMPLIANCE
    mcp_server_id = "environmental-mcp-server"
    required_scopes = ["read", "execute"]

    def get_system_prompt(self, region_focus: str, material_focus: str) -> str:
        return (
            "You are the Environmental Compliance Officer in a Multi-Agent System building a secure GPU supply chain.\n"
            "Your specific expertise is: Checks green clearances and local pollution indices.\n"
            f"Region: {region_focus}\n"
            f"Material: {material_focus}\n\n"
            "Analyze the data provided by your parallel tool execution accurately and concisely."
        )

    def get_required_tools(self) -> List[str]:
        """Return the exact MCP tool names to be executed in parallel during the Fan-Out phase."""
        return ['check_green_clearances', 'get_pollution_index']
