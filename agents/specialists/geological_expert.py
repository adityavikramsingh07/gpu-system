"""
agents/specialists/geological_expert.py
=========================================
Geological Expert DSW (LangGraph Autonomous Agent)

Domain: Map raw material mining deposits, HPQ seam surveys, rare earth reserves, and geological formations suitable for semiconductor-grade mineral extraction.
MCP Server: geological-mcp-server
MCP Tools: query_mining_deposits, get_lease_status, get_geological_survey, get_mineral_grade_report
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

class GeologicalExpertDSW(BaseDSW):
    worker_type    = DSWWorkerType.GEOLOGICAL_EXPERT
    mcp_server_id  = "geological-mcp-server"
    required_scopes = ['READ_MINING_DEPOSITS', 'READ_LEASE_STATUS', 'READ_GEO_SURVEY']

    def get_system_prompt(self, region_focus: str, material_focus: str) -> str:
        return f"""You are a highly intelligent Geological Expert specializing in the GPU semiconductor supply chain.
Your mission: Map raw material mining deposits, HPQ seam surveys, rare earth reserves, and geological formations suitable for semiconductor-grade mineral extraction.

Focus Area:
- Region: {region_focus}
- Material: {material_focus}

You have access to a set of specialized tools that connect to the `geological-mcp-server`.
You must use these tools to gather real-world data, analyze the results, and formulate a comprehensive answer.

Guidelines:
1. Always utilize your tools to verify data before answering. Do not hallucinate.
2. If a tool times out or fails, note it in your summary and try to proceed with partial data if possible.
3. Be specific and quantitative in your final response.
"""

    def get_dynamic_tools(self, token: EphemeralToken, span: Any, mcp_calls: List[MCPToolCall]) -> List[Any]:
        tools = []
        tools.append(self.create_mcp_tool(
            tool_name="query_mining_deposits",
            description="Execute the query_mining_deposits capability on the geological-mcp-server.",
            params_schema=MCPToolSchema,
            token=token,
            span=span,
            mcp_calls=mcp_calls
        ))
        tools.append(self.create_mcp_tool(
            tool_name="get_lease_status",
            description="Execute the get_lease_status capability on the geological-mcp-server.",
            params_schema=MCPToolSchema,
            token=token,
            span=span,
            mcp_calls=mcp_calls
        ))
        tools.append(self.create_mcp_tool(
            tool_name="get_geological_survey",
            description="Execute the get_geological_survey capability on the geological-mcp-server.",
            params_schema=MCPToolSchema,
            token=token,
            span=span,
            mcp_calls=mcp_calls
        ))
        tools.append(self.create_mcp_tool(
            tool_name="get_mineral_grade_report",
            description="Execute the get_mineral_grade_report capability on the geological-mcp-server.",
            params_schema=MCPToolSchema,
            token=token,
            span=span,
            mcp_calls=mcp_calls
        ))

        return tools
