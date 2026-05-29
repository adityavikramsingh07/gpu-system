"""
agents/specialists/mining_lease_analyst.py
=========================================
Mining Lease Legal Analyst DSW

Domain: Analyze legal disputes and land registry for mining leases.
MCP Server: legal-mcp-server
MCP Tools: check_legal_disputes, query_land_registry
"""

from typing import Any, Dict, List, Tuple
from orchestration.cog.state_schema import DSWWorkerType, MCPToolCall
from core_services.svb import EphemeralToken
from agents.specialists.base_dsw import BaseDSW, DSW_BASE_SYSTEM_PROMPT

class MiningLeaseAnalystDSW(BaseDSW):
    worker_type    = DSWWorkerType.MINING_LEASE_ANALYST
    mcp_server_id  = "legal-mcp-server"
    required_scopes = ['READ_LEGAL', 'READ_REGISTRY']
    available_tools = ['check_legal_disputes', 'query_land_registry']

    SYSTEM_PROMPT = DSW_BASE_SYSTEM_PROMPT.format(
        worker_role   = "Mining Lease Legal Analyst",
        mission       = "Analyze legal disputes and land registry for mining leases.",
        region_focus  = "{region_focus}",
        material_focus = "{material_focus}",
        mcp_server_id = "legal-mcp-server",
        available_tools = "check_legal_disputes, query_land_registry",
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
        result_data = {"summary": f"Mining Lease Legal Analyst assessment completed for {material_focus} in {region_focus}."}
        
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
