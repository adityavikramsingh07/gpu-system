from agents.specialists.dsw_workflow.state import DSWWorkflowState
from agents.specialists.dsw_workflow.chains.correlation_chain import run_correlation_chain

def correlation_node(state: DSWWorkflowState):
    result = run_correlation_chain(state["system_prompt"], state["query"], state["analysis_result"])
    return {"correlation_result": result}
