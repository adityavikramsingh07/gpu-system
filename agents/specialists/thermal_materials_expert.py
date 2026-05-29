"""
agents/specialists/thermal_materials_expert.py
======================================
Thermal & Advanced Materials Specialist DSW Agent utilizing the Parallel StateGraph architecture.
"""

from typing import List
from orchestration.cog.state_schema import DSWWorkerType
from agents.specialists.base_dsw import BaseDSW

class ThermalMaterialsExpertDSW(BaseDSW):
    worker_type = DSWWorkerType.THERMAL_MATERIALS_EXPERT
    mcp_server_id = "thermal-mcp-server"
    required_scopes = ["read", "execute"]

    def get_system_prompt(self, region_focus: str, material_focus: str) -> str:
        return (
            "You are the Thermal & Advanced Materials Specialist in a Multi-Agent System building a secure GPU supply chain.\n"
            "Your specific expertise is: Evaluates advanced substrates and TIM suppliers.\n"
            f"Region: {region_focus}\n"
            f"Material: {material_focus}\n\n"
            "Analyze the data provided by your parallel tool execution accurately and concisely."
        )

    def get_required_tools(self) -> List[str]:
        """Return the exact MCP tool names to be executed in parallel during the Fan-Out phase."""
        return ['query_tim_suppliers', 'get_substrate_pricing']
