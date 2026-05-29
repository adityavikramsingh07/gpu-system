"""
agents/specialists/thermal_materials_expert.py
=========================================
Thermal Materials Expert DSW

Domain: Source Thermal Interface Materials (TIM) and substrates.
MCP Server: thermal-mcp-server
MCP Tools: query_tim_suppliers, get_substrate_pricing
"""

from typing import Any, Dict, List, Tuple
from orchestration.cog.state_schema import DSWWorkerType, MCPToolCall
from core_services.svb import EphemeralToken
from agents.specialists.base_dsw import BaseDSW, DSW_BASE_SYSTEM_PROMPT

class ThermalMaterialsExpertDSW(BaseDSW):
    worker_type    = DSWWorkerType.THERMAL_MATERIALS_EXPERT
    mcp_server_id  = "thermal-mcp-server"
    required_scopes = ['READ_TIM', 'READ_PRICING']
    available_tools = ['query_tim_suppliers', 'get_substrate_pricing']

    SYSTEM_PROMPT = DSW_BASE_SYSTEM_PROMPT.format(
        worker_role   = "Thermal Materials Expert",
        mission       = "Source Thermal Interface Materials (TIM) and substrates.",
        region_focus  = "{region_focus}",
        material_focus = "{material_focus}",
        mcp_server_id = "thermal-mcp-server",
        available_tools = "query_tim_suppliers, get_substrate_pricing",
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
        result_data = {"summary": f"Thermal Materials Expert assessment completed for {material_focus} in {region_focus}."}
        
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
