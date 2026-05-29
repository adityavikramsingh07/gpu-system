"""
agents/specialists/thermal_materials_expert.py
=========================================
Thermal Materials Expert DSW (LangGraph Autonomous Agent)

Domain: Source Thermal Interface Materials (TIM) and evaluate advanced substrate pricing.
MCP Server: thermal-mcp-server
MCP Tools: query_tim_suppliers, get_substrate_pricing
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

class ThermalMaterialsExpertDSW(BaseDSW):
    worker_type    = DSWWorkerType.THERMAL_MATERIALS_EXPERT
    mcp_server_id  = "thermal-mcp-server"
    required_scopes = ['READ_TIM', 'READ_PRICING']

    def get_system_prompt(self, region_focus: str, material_focus: str) -> str:
        return f"""You are a highly intelligent Thermal Materials Expert specializing in the GPU semiconductor supply chain.
Your mission: Source Thermal Interface Materials (TIM) and evaluate advanced substrate pricing.

Focus Area:
- Region: {region_focus}
- Material: {material_focus}

You have access to a set of specialized tools that connect to the `thermal-mcp-server`.
You must use these tools to gather real-world data, analyze the results, and formulate a comprehensive answer.

Guidelines:
1. Always utilize your tools to verify data before answering. Do not hallucinate.
2. If a tool times out or fails, note it in your summary and try to proceed with partial data if possible.
3. Be specific and quantitative in your final response.
"""

    def get_dynamic_tools(self, token: EphemeralToken, span: Any, mcp_calls: List[MCPToolCall]) -> List[Any]:
        tools = []
        tools.append(self.create_mcp_tool(
            tool_name="query_tim_suppliers",
            description="Execute the query_tim_suppliers capability on the thermal-mcp-server.",
            params_schema=MCPToolSchema,
            token=token,
            span=span,
            mcp_calls=mcp_calls
        ))
        tools.append(self.create_mcp_tool(
            tool_name="get_substrate_pricing",
            description="Execute the get_substrate_pricing capability on the thermal-mcp-server.",
            params_schema=MCPToolSchema,
            token=token,
            span=span,
            mcp_calls=mcp_calls
        ))

        return tools
