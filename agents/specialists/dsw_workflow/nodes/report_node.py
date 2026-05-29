from agents.specialists.dsw_workflow.state import DSWWorkflowState
from agents.specialists.dsw_workflow.chains.report_chain import run_report_chain

def report_node(state: DSWWorkflowState):
    parsed = run_report_chain(
        state["system_prompt"], 
        state["analysis_result"], 
        state["correlation_result"], 
        state["recommendation_result"]
    )
    return {
        "final_summary": parsed.get("summary", ""),
        "final_data": parsed.get("data", {}),
        "confidence": parsed.get("confidence", 0.0),
        "status": "completed"
    }
