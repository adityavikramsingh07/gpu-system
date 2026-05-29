from typing import TypedDict, Dict, List, Any
from orchestration.cog.state_schema import MCPToolCall

class DSWWorkflowState(TypedDict):
    task_id: str
    job_id: str
    session_id: str
    trace_id: str
    query: str
    region_focus: str
    material_focus: str
    system_prompt: str
    mcp_server_id: str
    
    # Auth
    _ephemeral_token: Any
    
    # Tool Execution
    required_tools: List[str]
    tool_results: Dict[str, str]
    mcp_calls: List[MCPToolCall]
    
    # LLM Pipeline
    analysis_result: str
    correlation_result: str
    recommendation_result: str
    
    # Final Output
    final_summary: str
    final_data: Dict[str, Any]
    confidence: float
    status: str
    error: str
