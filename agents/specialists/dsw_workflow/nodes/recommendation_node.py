from agents.specialists.dsw_workflow.state import DSWWorkflowState
from agents.specialists.dsw_workflow.chains.recommendation_chain import run_recommendation_chain

def recommendation_node(state: DSWWorkflowState):
    result = run_recommendation_chain(state["system_prompt"], state["correlation_result"])
    return {"recommendation_result": result}
