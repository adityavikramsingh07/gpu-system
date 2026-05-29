"""
agents/specialists/trade_policy_expert.py
=========================================
Trade Policy Expert DSW

Domain: Analyze PLI schemes, tariffs, and government incentives.
MCP Server: trade-mcp-server
MCP Tools: query_pli_schemes, get_import_tariffs
"""

from typing import Any, Dict, List, Tuple
from orchestration.cog.state_schema import DSWWorkerType, MCPToolCall
from core_services.svb import EphemeralToken
from agents.specialists.base_dsw import BaseDSW, DSW_BASE_SYSTEM_PROMPT

class TradePolicyExpertDSW(BaseDSW):
    worker_type    = DSWWorkerType.TRADE_POLICY_EXPERT
    mcp_server_id  = "trade-mcp-server"
    required_scopes = ['READ_PLI', 'READ_TARIFFS']
    available_tools = ['query_pli_schemes', 'get_import_tariffs']

    SYSTEM_PROMPT = DSW_BASE_SYSTEM_PROMPT.format(
        worker_role   = "Trade Policy Expert",
        mission       = "Analyze PLI schemes, tariffs, and government incentives.",
        region_focus  = "{region_focus}",
        material_focus = "{material_focus}",
        mcp_server_id = "trade-mcp-server",
        available_tools = "query_pli_schemes, get_import_tariffs",
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
        result_data = {"summary": f"Trade Policy Expert assessment completed for {material_focus} in {region_focus}."}
        
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
