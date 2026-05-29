"""
agents/specialists/fab_locator.py
=========================================
Fab Locator Expert DSW

Domain: Identify suitable locations for semiconductor fabs based on utilities.
MCP Server: fab-mcp-server
MCP Tools: query_industrial_parks, get_water_power_metrics
"""

from typing import Any, Dict, List, Tuple
from orchestration.cog.state_schema import DSWWorkerType, MCPToolCall
from core_services.svb import EphemeralToken
from agents.specialists.base_dsw import BaseDSW, DSW_BASE_SYSTEM_PROMPT

class FabLocatorDSW(BaseDSW):
    worker_type    = DSWWorkerType.FAB_LOCATOR
    mcp_server_id  = "fab-mcp-server"
    required_scopes = ['READ_INDUSTRIAL_PARKS', 'READ_UTILITIES']
    available_tools = ['query_industrial_parks', 'get_water_power_metrics']

    SYSTEM_PROMPT = DSW_BASE_SYSTEM_PROMPT.format(
        worker_role   = "Fab Locator Expert",
        mission       = "Identify suitable locations for semiconductor fabs based on utilities.",
        region_focus  = "{region_focus}",
        material_focus = "{material_focus}",
        mcp_server_id = "fab-mcp-server",
        available_tools = "query_industrial_parks, get_water_power_metrics",
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
        result_data = {"summary": f"Fab Locator Expert assessment completed for {material_focus} in {region_focus}."}
        
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
