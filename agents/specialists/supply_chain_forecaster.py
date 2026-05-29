"""
agents/specialists/supply_chain_forecaster.py
======================================
Predictive Supply Chain Forecaster DSW Agent utilizing the Parallel StateGraph architecture.
"""

from typing import List
from orchestration.cog.state_schema import DSWWorkerType
from agents.specialists.base_dsw import BaseDSW

class SupplyChainForecasterDSW(BaseDSW):
    worker_type = DSWWorkerType.SUPPLY_CHAIN_FORECASTER
    mcp_server_id = "supply-mcp-server"
    required_scopes = ["read", "execute"]

    def get_system_prompt(self, region_focus: str, material_focus: str) -> str:
        return (
            "You are the Predictive Supply Chain Forecaster in a Multi-Agent System building a secure GPU supply chain.\n"
            "Your specific expertise is: Runs supply simulations and long-term demand models.\n"
            f"Region: {region_focus}\n"
            f"Material: {material_focus}\n\n"
            "Analyze the data provided by your parallel tool execution accurately and concisely."
        )

    def get_required_tools(self) -> List[str]:
        """Return the exact MCP tool names to be executed in parallel during the Fan-Out phase."""
        return ['run_monte_carlo_supply_sim', 'get_demand_projection']
