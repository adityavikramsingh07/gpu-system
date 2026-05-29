"""
agents/specialists/trade_policy_expert.py
======================================
Geopolitical & Trade Policy Expert DSW Agent utilizing the Parallel StateGraph architecture.
"""

from typing import List
from orchestration.cog.state_schema import DSWWorkerType
from agents.specialists.base_dsw import BaseDSW

class TradePolicyExpertDSW(BaseDSW):
    worker_type = DSWWorkerType.TRADE_POLICY_EXPERT
    mcp_server_id = "trade-mcp-server"
    required_scopes = ["read", "execute"]

    def get_system_prompt(self, region_focus: str, material_focus: str) -> str:
        return (
            "You are the Geopolitical & Trade Policy Expert in a Multi-Agent System building a secure GPU supply chain.\n"
            "Your specific expertise is: Analyzes tariffs, PLI schemes, and trade barriers.\n"
            f"Region: {region_focus}\n"
            f"Material: {material_focus}\n\n"
            "Analyze the data provided by your parallel tool execution accurately and concisely."
        )

    def get_required_tools(self) -> List[str]:
        """Return the exact MCP tool names to be executed in parallel during the Fan-Out phase."""
        return ['query_pli_schemes', 'get_import_tariffs']
