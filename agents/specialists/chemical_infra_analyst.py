"""
agents/specialists/chemical_infra_analyst.py
=========================================
Chemical Infrastructure Analyst DSW (LangGraph Autonomous Agent)

Domain: Map chemical processing plants and evaluate precursor availability for raw material refinement.
MCP Server: chemical-mcp-server
MCP Tools: map_processing_plants, get_chemical_precursor_supply
"""

from typing import Any, List
from pydantic import BaseModel, Field

from orchestration.cog.state_schema import DSWWorkerType, MCPToolCall
from core_services.svb import EphemeralToken
from agents.specialists.base_dsw import BaseDSW

class MCPToolSchema(BaseModel):
    query: str = Field(description="The specific context or parameters for this tool execution.")
    region: str = Field(description="The geographic region focus.", default="")
    material: str = Field(description="The target material focus.", default="")

class ChemicalInfraAnalystDSW(BaseDSW):
    worker_type    = DSWWorkerType.CHEMICAL_INFRA_ANALYST
    mcp_server_id  = "chemical-mcp-server"
    required_scopes = ['READ_CHEM_PLANTS', 'READ_PRECURSORS']

    def get_system_prompt(self, region_focus: str, material_focus: str) -> str:
        return f"""You are a highly intelligent Chemical Infrastructure Analyst specializing in the GPU semiconductor supply chain.
Your mission: Map chemical processing plants and evaluate precursor availability for raw material refinement.

Focus Area:
- Region: {region_focus}
- Material: {material_focus}

You have access to a set of specialized tools that connect to the `chemical-mcp-server`.
You must use these tools to gather real-world data, analyze the results, and formulate a comprehensive answer.

Guidelines:
1. Always utilize your tools to verify data before answering. Do not hallucinate.
2. If a tool times out or fails, note it in your summary and try to proceed with partial data if possible.
3. Be specific and quantitative in your final response.
"""

    def get_dynamic_tools(self, token: EphemeralToken, span: Any, mcp_calls: List[MCPToolCall]) -> List[Any]:
        tools = []
        tools.append(self.create_mcp_tool(
            tool_name="map_processing_plants",
            description="Execute the map_processing_plants capability on the chemical-mcp-server.",
            params_schema=MCPToolSchema,
            token=token,
            span=span,
            mcp_calls=mcp_calls
        ))
        tools.append(self.create_mcp_tool(
            tool_name="get_chemical_precursor_supply",
            description="Execute the get_chemical_precursor_supply capability on the chemical-mcp-server.",
            params_schema=MCPToolSchema,
            token=token,
            span=span,
            mcp_calls=mcp_calls
        ))

        return tools
