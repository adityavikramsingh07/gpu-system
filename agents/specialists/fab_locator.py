"""
agents/specialists/fab_locator.py
=========================================
Fab Locator Expert DSW (LangGraph Autonomous Agent)

Domain: Identify suitable locations for semiconductor fabs based on available utilities and infrastructure.
MCP Server: fab-mcp-server
MCP Tools: query_industrial_parks, get_water_power_metrics
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

class FabLocatorDSW(BaseDSW):
    worker_type    = DSWWorkerType.FAB_LOCATOR
    mcp_server_id  = "fab-mcp-server"
    required_scopes = ['READ_INDUSTRIAL_PARKS', 'READ_UTILITIES']

    def get_system_prompt(self, region_focus: str, material_focus: str) -> str:
        return f"""You are a highly intelligent Fab Locator Expert specializing in the GPU semiconductor supply chain.
Your mission: Identify suitable locations for semiconductor fabs based on available utilities and infrastructure.

Focus Area:
- Region: {region_focus}
- Material: {material_focus}

You have access to a set of specialized tools that connect to the `fab-mcp-server`.
You must use these tools to gather real-world data, analyze the results, and formulate a comprehensive answer.

Guidelines:
1. Always utilize your tools to verify data before answering. Do not hallucinate.
2. If a tool times out or fails, note it in your summary and try to proceed with partial data if possible.
3. Be specific and quantitative in your final response.
"""

    def get_dynamic_tools(self, token: EphemeralToken, span: Any, mcp_calls: List[MCPToolCall]) -> List[Any]:
        tools = []
        tools.append(self.create_mcp_tool(
            tool_name="query_industrial_parks",
            description="Execute the query_industrial_parks capability on the fab-mcp-server.",
            params_schema=MCPToolSchema,
            token=token,
            span=span,
            mcp_calls=mcp_calls
        ))
        tools.append(self.create_mcp_tool(
            tool_name="get_water_power_metrics",
            description="Execute the get_water_power_metrics capability on the fab-mcp-server.",
            params_schema=MCPToolSchema,
            token=token,
            span=span,
            mcp_calls=mcp_calls
        ))

        return tools
