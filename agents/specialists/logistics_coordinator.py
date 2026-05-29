"""
agents/specialists/logistics_coordinator.py
======================================
Global Logistics & Transport Coordinator DSW Agent utilizing the Parallel StateGraph architecture.
"""

from typing import List
from orchestration.cog.state_schema import DSWWorkerType
from agents.specialists.base_dsw import BaseDSW

class LogisticsCoordinatorDSW(BaseDSW):
    worker_type = DSWWorkerType.LOGISTICS_COORDINATOR
    mcp_server_id = "logistics-mcp-server"
    required_scopes = ["read", "execute"]

    def get_system_prompt(self, region_focus: str, material_focus: str) -> str:
        return (
            "You are the Global Logistics & Transport Coordinator in a Multi-Agent System building a secure GPU supply chain.\n"
            "Your specific expertise is: Optimizes transport routes, port capacities, and warehouse logic.\n"
            f"Region: {region_focus}\n"
            f"Material: {material_focus}\n\n"
            "Analyze the data provided by your parallel tool execution accurately and concisely."
        )

    def get_required_tools(self) -> List[str]:
        """Return the exact MCP tool names to be executed in parallel during the Fan-Out phase."""
        return ['get_port_data', 'query_transport_routes', 'get_warehouse_capacity', 'get_customs_clearance_time']
