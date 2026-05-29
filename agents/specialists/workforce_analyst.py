"""
agents/specialists/workforce_analyst.py
=========================================
Workforce & Labor Analyst DSW

Domain: Analyze skilled labor availability and workforce costs.
MCP Server: workforce-mcp-server
MCP Tools: query_university_graduates, get_labor_costs
"""

from typing import Any, Dict, List, Tuple
from orchestration.cog.state_schema import DSWWorkerType, MCPToolCall
from core_services.svb import EphemeralToken
from agents.specialists.base_dsw import BaseDSW, DSW_BASE_SYSTEM_PROMPT

class WorkforceAnalystDSW(BaseDSW):
    worker_type    = DSWWorkerType.WORKFORCE_ANALYST
    mcp_server_id  = "workforce-mcp-server"
    required_scopes = ['READ_GRADUATES', 'READ_LABOR_COSTS']
    available_tools = ['query_university_graduates', 'get_labor_costs']

    SYSTEM_PROMPT = DSW_BASE_SYSTEM_PROMPT.format(
        worker_role   = "Workforce & Labor Analyst",
        mission       = "Analyze skilled labor availability and workforce costs.",
        region_focus  = "{region_focus}",
        material_focus = "{material_focus}",
        mcp_server_id = "workforce-mcp-server",
        available_tools = "query_university_graduates, get_labor_costs",
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
        result_data = {"summary": f"Workforce & Labor Analyst assessment completed for {material_focus} in {region_focus}."}
        
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
