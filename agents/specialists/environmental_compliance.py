"""
agents/specialists/environmental_compliance.py
=========================================
Environmental Compliance Officer DSW

Domain: Verify environmental green clearances and pollution metrics.
MCP Server: env-mcp-server
MCP Tools: check_green_clearances, get_pollution_index
"""

from typing import Any, Dict, List, Tuple
from orchestration.cog.state_schema import DSWWorkerType, MCPToolCall
from core_services.svb import EphemeralToken
from agents.specialists.base_dsw import BaseDSW, DSW_BASE_SYSTEM_PROMPT

class EnvironmentalComplianceDSW(BaseDSW):
    worker_type    = DSWWorkerType.ENVIRONMENTAL_COMPLIANCE
    mcp_server_id  = "env-mcp-server"
    required_scopes = ['READ_CLEARANCES', 'READ_POLLUTION']
    available_tools = ['check_green_clearances', 'get_pollution_index']

    SYSTEM_PROMPT = DSW_BASE_SYSTEM_PROMPT.format(
        worker_role   = "Environmental Compliance Officer",
        mission       = "Verify environmental green clearances and pollution metrics.",
        region_focus  = "{region_focus}",
        material_focus = "{material_focus}",
        mcp_server_id = "env-mcp-server",
        available_tools = "check_green_clearances, get_pollution_index",
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
        result_data = {"summary": f"Environmental Compliance Officer assessment completed for {material_focus} in {region_focus}."}
        
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
