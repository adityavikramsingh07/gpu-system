"""
agents/specialists/supply_chain_forecaster.py
=========================================
Supply Chain Forecaster DSW (LangGraph Autonomous Agent)

Domain: Forecast supply chain bottlenecks and project future raw material demands.
MCP Server: forecast-mcp-server
MCP Tools: run_monte_carlo_supply_sim, get_demand_projection
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

class SupplyChainForecasterDSW(BaseDSW):
    worker_type    = DSWWorkerType.SUPPLY_CHAIN_FORECASTER
    mcp_server_id  = "forecast-mcp-server"
    required_scopes = ['RUN_SIMULATION', 'READ_DEMAND']

    def get_system_prompt(self, region_focus: str, material_focus: str) -> str:
        return f"""You are a highly intelligent Supply Chain Forecaster specializing in the GPU semiconductor supply chain.
Your mission: Forecast supply chain bottlenecks and project future raw material demands.

Focus Area:
- Region: {region_focus}
- Material: {material_focus}

You have access to a set of specialized tools that connect to the `forecast-mcp-server`.
You must use these tools to gather real-world data, analyze the results, and formulate a comprehensive answer.

Guidelines:
1. Always utilize your tools to verify data before answering. Do not hallucinate.
2. If a tool times out or fails, note it in your summary and try to proceed with partial data if possible.
3. Be specific and quantitative in your final response.
"""

    def get_dynamic_tools(self, token: EphemeralToken, span: Any, mcp_calls: List[MCPToolCall]) -> List[Any]:
        tools = []
        tools.append(self.create_mcp_tool(
            tool_name="run_monte_carlo_supply_sim",
            description="Execute the run_monte_carlo_supply_sim capability on the forecast-mcp-server.",
            params_schema=MCPToolSchema,
            token=token,
            span=span,
            mcp_calls=mcp_calls
        ))
        tools.append(self.create_mcp_tool(
            tool_name="get_demand_projection",
            description="Execute the get_demand_projection capability on the forecast-mcp-server.",
            params_schema=MCPToolSchema,
            token=token,
            span=span,
            mcp_calls=mcp_calls
        ))

        return tools
