from agents.specialists.dsw_workflow.state import DSWWorkflowState
from agents.specialists.dsw_workflow.chains.analysis_chain import run_analysis_chain

def analysis_node(state: DSWWorkflowState):
    result = run_analysis_chain(
        state["system_prompt"], 
        state["region_focus"], 
        state["material_focus"], 
        state["tool_results"]
    )
    return {"analysis_result": result}
