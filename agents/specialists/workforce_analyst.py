"""
agents/specialists/workforce_analyst.py
=========================================
Workforce & Labor Analyst DSW (LangGraph Autonomous Agent)

Domain: Analyze skilled labor availability and workforce costs in target geographic regions.
MCP Server: workforce-mcp-server
MCP Tools: query_university_graduates, get_labor_costs
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

class WorkforceAnalystDSW(BaseDSW):
    worker_type    = DSWWorkerType.WORKFORCE_ANALYST
    mcp_server_id  = "workforce-mcp-server"
    required_scopes = ['READ_GRADUATES', 'READ_LABOR_COSTS']

    def get_system_prompt(self, region_focus: str, material_focus: str) -> str:
        return f"""You are a highly intelligent Workforce & Labor Analyst specializing in the GPU semiconductor supply chain.
Your mission: Analyze skilled labor availability and workforce costs in target geographic regions.

Focus Area:
- Region: {region_focus}
- Material: {material_focus}

You have access to a set of specialized tools that connect to the `workforce-mcp-server`.
You must use these tools to gather real-world data, analyze the results, and formulate a comprehensive answer.

Guidelines:
1. Always utilize your tools to verify data before answering. Do not hallucinate.
2. If a tool times out or fails, note it in your summary and try to proceed with partial data if possible.
3. Be specific and quantitative in your final response.
"""

    def get_dynamic_tools(self, token: EphemeralToken, span: Any, mcp_calls: List[MCPToolCall]) -> List[Any]:
        tools = []
        tools.append(self.create_mcp_tool(
            tool_name="query_university_graduates",
            description="Execute the query_university_graduates capability on the workforce-mcp-server.",
            params_schema=MCPToolSchema,
            token=token,
            span=span,
            mcp_calls=mcp_calls
        ))
        tools.append(self.create_mcp_tool(
            tool_name="get_labor_costs",
            description="Execute the get_labor_costs capability on the workforce-mcp-server.",
            params_schema=MCPToolSchema,
            token=token,
            span=span,
            mcp_calls=mcp_calls
        ))

        return tools
