import asyncio
import time
from datetime import datetime
from orchestration.cog.state_schema import MCPToolCall
from mcp.client.sse import sse_client
from mcp.client.session import ClientSession
import mcp.types as types

async def async_call_tool(session, tool_name, params, mcp_server_id, mcp_calls_ref):
    t_start = time.perf_counter()
    call_record = MCPToolCall(
        tool_name=tool_name,
        server_id=mcp_server_id,
        invoked_at=datetime.utcnow()
    )
    try:
        response = await session.call_tool(tool_name, arguments=params)
        call_record.success = True
        call_record.latency_ms = (time.perf_counter() - t_start) * 1000
        mcp_calls_ref.append(call_record)
        
        if response.content and isinstance(response.content[0], types.TextContent):
            return response.content[0].text
        return str(response.content)
    except Exception as e:
        call_record.success = False
        call_record.error_detail = str(e)
        call_record.latency_ms = (time.perf_counter() - t_start) * 1000
        mcp_calls_ref.append(call_record)
        raise e

async def execute_tools_parallel(url, token, required_tools, params, mcp_server_id):
    results = {}
    mcp_calls = []
    headers = token.as_bearer_header() if token else {}
    try:
        async with sse_client(url, headers=headers) as streams:
            async with ClientSession(streams[0], streams[1]) as session:
                await session.initialize()
                tasks = []
                for tool_name in required_tools:
                    tasks.append(async_call_tool(session, tool_name, params, mcp_server_id, mcp_calls))
                
                completed = await asyncio.gather(*tasks, return_exceptions=True)
                for tool_name, res in zip(required_tools, completed):
                    if isinstance(res, Exception):
                        results[tool_name] = f"ERROR: {str(res)}"
                    else:
                        results[tool_name] = res
    except Exception as e:
        for tool_name in required_tools:
            results[tool_name] = f"CONNECTION_ERROR: {str(e)}"
    return results, mcp_calls
