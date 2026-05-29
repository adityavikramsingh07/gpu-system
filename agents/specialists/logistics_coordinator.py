"""
agents/specialists/logistics_coordinator.py
=========================================
Logistics Coordinator DSW (LangGraph Autonomous Agent)

Domain: Optimize transport routes, assess port bandwidth, evaluate warehouse capacity, and estimate customs clearance times for raw materials.
MCP Server: logistics-mcp-server
MCP Tools: get_port_data, query_transport_routes, get_warehouse_capacity, get_customs_clearance_time
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

class LogisticsCoordinatorDSW(BaseDSW):
    worker_type    = DSWWorkerType.LOGISTICS_COORDINATOR
    mcp_server_id  = "logistics-mcp-server"
    required_scopes = ['READ_PORT_DATA', 'READ_ROUTES', 'READ_WAREHOUSE']

    def get_system_prompt(self, region_focus: str, material_focus: str) -> str:
        return f"""You are a highly intelligent Logistics Coordinator specializing in the GPU semiconductor supply chain.
Your mission: Optimize transport routes, assess port bandwidth, evaluate warehouse capacity, and estimate customs clearance times for raw materials.

Focus Area:
- Region: {region_focus}
- Material: {material_focus}

You have access to a set of specialized tools that connect to the `logistics-mcp-server`.
You must use these tools to gather real-world data, analyze the results, and formulate a comprehensive answer.

Guidelines:
1. Always utilize your tools to verify data before answering. Do not hallucinate.
2. If a tool times out or fails, note it in your summary and try to proceed with partial data if possible.
3. Be specific and quantitative in your final response.
"""

    def get_dynamic_tools(self, token: EphemeralToken, span: Any, mcp_calls: List[MCPToolCall]) -> List[Any]:
        tools = []
        tools.append(self.create_mcp_tool(
            tool_name="get_port_data",
            description="Execute the get_port_data capability on the logistics-mcp-server.",
            params_schema=MCPToolSchema,
            token=token,
            span=span,
            mcp_calls=mcp_calls
        ))
        tools.append(self.create_mcp_tool(
            tool_name="query_transport_routes",
            description="Execute the query_transport_routes capability on the logistics-mcp-server.",
            params_schema=MCPToolSchema,
            token=token,
            span=span,
            mcp_calls=mcp_calls
        ))
        tools.append(self.create_mcp_tool(
            tool_name="get_warehouse_capacity",
            description="Execute the get_warehouse_capacity capability on the logistics-mcp-server.",
            params_schema=MCPToolSchema,
            token=token,
            span=span,
            mcp_calls=mcp_calls
        ))
        tools.append(self.create_mcp_tool(
            tool_name="get_customs_clearance_time",
            description="Execute the get_customs_clearance_time capability on the logistics-mcp-server.",
            params_schema=MCPToolSchema,
            token=token,
            span=span,
            mcp_calls=mcp_calls
        ))

        return tools
