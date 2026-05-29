"""
agents/specialists/chemical_infra_analyst.py
=========================================
Chemical Infrastructure Analyst DSW

Domain: Map chemical processing plants and precursor availability.
MCP Server: chemical-mcp-server
MCP Tools: map_processing_plants, get_chemical_precursor_supply
"""

from typing import Any, Dict, List, Tuple
from orchestration.cog.state_schema import DSWWorkerType, MCPToolCall
from core_services.svb import EphemeralToken
from agents.specialists.base_dsw import BaseDSW, DSW_BASE_SYSTEM_PROMPT

class ChemicalInfraAnalystDSW(BaseDSW):
    worker_type    = DSWWorkerType.CHEMICAL_INFRA_ANALYST
    mcp_server_id  = "chemical-mcp-server"
    required_scopes = ['READ_CHEM_PLANTS', 'READ_PRECURSORS']
    available_tools = ['map_processing_plants', 'get_chemical_precursor_supply']

    SYSTEM_PROMPT = DSW_BASE_SYSTEM_PROMPT.format(
        worker_role   = "Chemical Infrastructure Analyst",
        mission       = "Map chemical processing plants and precursor availability.",
        region_focus  = "{region_focus}",
        material_focus = "{material_focus}",
        mcp_server_id = "chemical-mcp-server",
        available_tools = "map_processing_plants, get_chemical_precursor_supply",
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
        result_data = {"summary": f"Chemical Infrastructure Analyst assessment completed for {material_focus} in {region_focus}."}
        
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
