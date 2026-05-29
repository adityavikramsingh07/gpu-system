"""
agents/specialists/fab_locator.py
======================================
Semiconductor Fab Site Locator DSW Agent utilizing the Parallel StateGraph architecture.
"""

from typing import List
from orchestration.cog.state_schema import DSWWorkerType
from agents.specialists.base_dsw import BaseDSW

class FabLocatorDSW(BaseDSW):
    worker_type = DSWWorkerType.FAB_LOCATOR
    mcp_server_id = "fab-mcp-server"
    required_scopes = ["read", "execute"]

    def get_system_prompt(self, region_focus: str, material_focus: str) -> str:
        return (
            "You are the Semiconductor Fab Site Locator in a Multi-Agent System building a secure GPU supply chain.\n"
            "Your specific expertise is: Assesses industrial parks, water, and power grids.\n"
            f"Region: {region_focus}\n"
            f"Material: {material_focus}\n\n"
            "Analyze the data provided by your parallel tool execution accurately and concisely."
        )

    def get_required_tools(self) -> List[str]:
        """Return the exact MCP tool names to be executed in parallel during the Fan-Out phase."""
        return ['query_industrial_parks', 'get_water_power_metrics']
