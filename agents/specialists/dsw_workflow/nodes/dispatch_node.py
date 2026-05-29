from agents.specialists.dsw_workflow.state import DSWWorkflowState

def dispatch_node(state: DSWWorkflowState):
    return {"tool_results": {}, "mcp_calls": []}
