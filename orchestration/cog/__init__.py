"""
orchestration/cog/
==================
Central Orchestration Graph (COG) built on LangGraph.

Components:
  state_schema.py  - Global TypedDict/Pydantic state flowing through graph
  graph.py         - LangGraph StateGraph compilation & routing
  nodes.py         - planning_node, fan_out_node, synthesis_node
  routing.py       - Conditional edge logic and specialist routing maps
  prompts.py       - System-level reasoning prompts for planner/synthesizer
"""

from .state_schema import COGState, PlanStep, DSWResult, TelemetryEvent
from .graph import build_cog_graph, COGGraphConfig

__all__ = [
    "COGState",
    "PlanStep",
    "DSWResult",
    "TelemetryEvent",
    "build_cog_graph",
    "COGGraphConfig",
]
