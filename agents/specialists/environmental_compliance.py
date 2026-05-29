"""
agents/specialists/environmental_compliance.py
=========================================
Environmental Compliance Officer DSW (LangGraph Autonomous Agent)

Domain: Verify environmental green clearances and monitor local pollution index metrics.
MCP Server: env-mcp-server
MCP Tools: check_green_clearances, get_pollution_index
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

class EnvironmentalComplianceDSW(BaseDSW):
    worker_type    = DSWWorkerType.ENVIRONMENTAL_COMPLIANCE
    mcp_server_id  = "env-mcp-server"
    required_scopes = ['READ_CLEARANCES', 'READ_POLLUTION']

    def get_system_prompt(self, region_focus: str, material_focus: str) -> str:
        return f"""You are a highly intelligent Environmental Compliance Officer specializing in the GPU semiconductor supply chain.
Your mission: Verify environmental green clearances and monitor local pollution index metrics.

Focus Area:
- Region: {region_focus}
- Material: {material_focus}

You have access to a set of specialized tools that connect to the `env-mcp-server`.
You must use these tools to gather real-world data, analyze the results, and formulate a comprehensive answer.

Guidelines:
1. Always utilize your tools to verify data before answering. Do not hallucinate.
2. If a tool times out or fails, note it in your summary and try to proceed with partial data if possible.
3. Be specific and quantitative in your final response.
"""

    def get_dynamic_tools(self, token: EphemeralToken, span: Any, mcp_calls: List[MCPToolCall]) -> List[Any]:
        tools = []
        tools.append(self.create_mcp_tool(
            tool_name="check_green_clearances",
            description="Execute the check_green_clearances capability on the env-mcp-server.",
            params_schema=MCPToolSchema,
            token=token,
            span=span,
            mcp_calls=mcp_calls
        ))
        tools.append(self.create_mcp_tool(
            tool_name="get_pollution_index",
            description="Execute the get_pollution_index capability on the env-mcp-server.",
            params_schema=MCPToolSchema,
            token=token,
            span=span,
            mcp_calls=mcp_calls
        ))

        return tools
