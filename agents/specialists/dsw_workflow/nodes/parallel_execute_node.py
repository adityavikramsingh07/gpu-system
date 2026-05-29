import asyncio
from agents.specialists.dsw_workflow.state import DSWWorkflowState
from agents.specialists.dsw_workflow.tools.mcp_client import execute_tools_parallel

def parallel_execute_node(state: DSWWorkflowState):
    token = state.get("_ephemeral_token")
    url = "http://localhost:8001/sse"
    params = {
        "region": state["region_focus"],
        "material": state["material_focus"],
        "query": state["query"]
    }
    
    results, mcp_calls = asyncio.run(execute_tools_parallel(
        url, token, state["required_tools"], params, state["mcp_server_id"]
    ))
    
    return {"tool_results": results, "mcp_calls": mcp_calls}
