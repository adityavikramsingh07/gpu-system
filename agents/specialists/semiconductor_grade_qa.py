"""
agents/specialists/semiconductor_grade_qa.py
=========================================
Semiconductor Grade QA Specialist DSW (LangGraph Autonomous Agent)

Domain: Verify raw material purity compliance (e.g., SEMI F49) and QA ISO certifications.
MCP Server: qa-mcp-server
MCP Tools: verify_semi_f49_compliance, get_iso_certifications
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

class SemiconductorGradeQADSW(BaseDSW):
    worker_type    = DSWWorkerType.SEMICONDUCTOR_GRADE_QA
    mcp_server_id  = "qa-mcp-server"
    required_scopes = ['READ_COMPLIANCE', 'READ_ISO']

    def get_system_prompt(self, region_focus: str, material_focus: str) -> str:
        return f"""You are a highly intelligent Semiconductor Grade QA Specialist specializing in the GPU semiconductor supply chain.
Your mission: Verify raw material purity compliance (e.g., SEMI F49) and QA ISO certifications.

Focus Area:
- Region: {region_focus}
- Material: {material_focus}

You have access to a set of specialized tools that connect to the `qa-mcp-server`.
You must use these tools to gather real-world data, analyze the results, and formulate a comprehensive answer.

Guidelines:
1. Always utilize your tools to verify data before answering. Do not hallucinate.
2. If a tool times out or fails, note it in your summary and try to proceed with partial data if possible.
3. Be specific and quantitative in your final response.
"""

    def get_dynamic_tools(self, token: EphemeralToken, span: Any, mcp_calls: List[MCPToolCall]) -> List[Any]:
        tools = []
        tools.append(self.create_mcp_tool(
            tool_name="verify_semi_f49_compliance",
            description="Execute the verify_semi_f49_compliance capability on the qa-mcp-server.",
            params_schema=MCPToolSchema,
            token=token,
            span=span,
            mcp_calls=mcp_calls
        ))
        tools.append(self.create_mcp_tool(
            tool_name="get_iso_certifications",
            description="Execute the get_iso_certifications capability on the qa-mcp-server.",
            params_schema=MCPToolSchema,
            token=token,
            span=span,
            mcp_calls=mcp_calls
        ))

        return tools
