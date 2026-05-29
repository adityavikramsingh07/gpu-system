from langgraph.graph import StateGraph, START, END
from agents.specialists.dsw_workflow.state import DSWWorkflowState
from agents.specialists.dsw_workflow.nodes.dispatch_node import dispatch_node
from agents.specialists.dsw_workflow.nodes.parallel_execute_node import parallel_execute_node
from agents.specialists.dsw_workflow.nodes.analysis_node import analysis_node
from agents.specialists.dsw_workflow.nodes.correlation_node import correlation_node
from agents.specialists.dsw_workflow.nodes.recommendation_node import recommendation_node
from agents.specialists.dsw_workflow.nodes.report_node import report_node

def build_dsw_workflow():
    builder = StateGraph(DSWWorkflowState)
    
    builder.add_node("dispatch", dispatch_node)
    builder.add_node("parallel_execute", parallel_execute_node)
    builder.add_node("analyze", analysis_node)
    builder.add_node("correlate", correlation_node)
    builder.add_node("recommend", recommendation_node)
    builder.add_node("report", report_node)
    
    builder.add_edge(START, "dispatch")
    builder.add_edge("dispatch", "parallel_execute")
    builder.add_edge("parallel_execute", "analyze")
    builder.add_edge("analyze", "correlate")
    builder.add_edge("correlate", "recommend")
    builder.add_edge("recommend", "report")
    builder.add_edge("report", END)
    
    return builder.compile()

# Singleton instance
dsw_graph = build_dsw_workflow()
