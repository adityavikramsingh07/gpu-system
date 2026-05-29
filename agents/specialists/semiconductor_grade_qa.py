"""
agents/specialists/semiconductor_grade_qa.py
=========================================
Semiconductor Grade QA Specialist DSW

Domain: Verify material purity compliance (e.g. SEMI F49) and QA.
MCP Server: qa-mcp-server
MCP Tools: verify_semi_f49_compliance, get_iso_certifications
"""

from typing import Any, Dict, List, Tuple
from orchestration.cog.state_schema import DSWWorkerType, MCPToolCall
from core_services.svb import EphemeralToken
from agents.specialists.base_dsw import BaseDSW, DSW_BASE_SYSTEM_PROMPT

class SemiconductorGradeQADSW(BaseDSW):
    worker_type    = DSWWorkerType.SEMICONDUCTOR_GRADE_QA
    mcp_server_id  = "qa-mcp-server"
    required_scopes = ['READ_COMPLIANCE', 'READ_ISO']
    available_tools = ['verify_semi_f49_compliance', 'get_iso_certifications']

    SYSTEM_PROMPT = DSW_BASE_SYSTEM_PROMPT.format(
        worker_role   = "Semiconductor Grade QA Specialist",
        mission       = "Verify material purity compliance (e.g. SEMI F49) and QA.",
        region_focus  = "{region_focus}",
        material_focus = "{material_focus}",
        mcp_server_id = "qa-mcp-server",
        available_tools = "verify_semi_f49_compliance, get_iso_certifications",
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
        result_data = {"summary": f"Semiconductor Grade QA Specialist assessment completed for {material_focus} in {region_focus}."}
        
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
