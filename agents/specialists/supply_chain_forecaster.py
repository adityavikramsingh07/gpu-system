"""
agents/specialists/supply_chain_forecaster.py
=========================================
Supply Chain Forecaster DSW

Domain: Forecast supply chain bottlenecks and demand projections.
MCP Server: forecast-mcp-server
MCP Tools: run_monte_carlo_supply_sim, get_demand_projection
"""

from typing import Any, Dict, List, Tuple
from orchestration.cog.state_schema import DSWWorkerType, MCPToolCall
from core_services.svb import EphemeralToken
from agents.specialists.base_dsw import BaseDSW, DSW_BASE_SYSTEM_PROMPT

class SupplyChainForecasterDSW(BaseDSW):
    worker_type    = DSWWorkerType.SUPPLY_CHAIN_FORECASTER
    mcp_server_id  = "forecast-mcp-server"
    required_scopes = ['RUN_SIMULATION', 'READ_DEMAND']
    available_tools = ['run_monte_carlo_supply_sim', 'get_demand_projection']

    SYSTEM_PROMPT = DSW_BASE_SYSTEM_PROMPT.format(
        worker_role   = "Supply Chain Forecaster",
        mission       = "Forecast supply chain bottlenecks and demand projections.",
        region_focus  = "{region_focus}",
        material_focus = "{material_focus}",
        mcp_server_id = "forecast-mcp-server",
        available_tools = "run_monte_carlo_supply_sim, get_demand_projection",
    )

    def execute_core(
        self,
        token:          EphemeralToken,
        query:          str,
        region_focus:   str,
        material_focus: str,
        span:           Any,
    ) -> Tuple[Dict[str, Any], List[MCPToolCall], float, List[str]]:
        
        mcp_calls = []
        sources = []
        result_data = {"summary": f"Supply Chain Forecaster assessment completed for {material_focus} in {region_focus}."}
        
        for tool in self.available_tools:
            span.add_event(f"Calling {tool}")
            res, call = self.call_mcp_tool(
                tool_name=tool,
                params={"region": region_focus, "material": material_focus, "query": query},
                token=token,
                span=span
            )
            mcp_calls.append(call)
            sources.append(f"{tool} API")
            result_data[tool] = res.get("data", "No data")

        confidence = 0.85
        span.set_attribute("confidence", confidence)
        return result_data, mcp_calls, confidence, sources
