"""
orchestration/cog/graph.py
===========================
LangGraph StateGraph compilation for the Central Orchestration Graph (COG).

Graph topology:
  [START]
    → planning_node          (decompose query into PlanSteps)
    → fan_out_node           (publish to DTB, get job_ids)
    → [PARALLEL DSW BRANCHES via Send() API]
        Each branch runs independently in the DTB workers
        and appends its DSWResult to state.dsw_results
    → synthesis_node         (aggregate fan-in, generate blueprint)
  [END]

Conditional edges:
  planning_node  → route_after_planning  → {fan_out | failed}
  fan_out_node   → route_after_fan_out   → {await_results}
  await_results  → route_after_await     → {synthesis | healing | await_results}
  healing_node   → route_after_healing   → {fan_out | synthesis}  (re-dispatch or accept)

Persistence:
  MemorySaver checkpointer enables mid-graph state snapshots.
  THA can read the checkpoint and inject THAInjections without
  interrupting the running graph — they are picked up at the
  next routing evaluation.

LangGraph parallel execution (10+ DSWs):
  The fan_out_node uses LangGraph's `Send()` primitive to create
  N independent graph branches — one per PlanStep. Each branch:
    1. Picks up a job from DTB (non-blocking poll with timeout)
    2. Executes the DSW specialist agent
    3. Appends DSWResult to state via reducer
  The graph waits for ALL branches before proceeding to synthesis.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from langgraph.graph import StateGraph, START, END
from langgraph.checkpoint.memory import MemorySaver
from langgraph.types import Send

from orchestration.cog.state_schema import COGState, WorkflowPhase
from orchestration.cog.nodes import (
    planning_node,
    fan_out_node,
    synthesis_node,
    route_after_planning,
    route_after_fan_out,
    route_after_await,
)


# ============================================================================
# GRAPH CONFIGURATION
# ============================================================================

@dataclass
class COGGraphConfig:
    """Runtime configuration for the COG LangGraph."""
    max_healing_cycles:  int  = 2       # Max THA-triggered re-dispatches
    fan_in_timeout_s:    int  = 120     # Max wait for all DSWs to complete
    enable_checkpointing: bool = True   # Persist state snapshots
    enable_streaming:    bool = True    # Stream events to caller


# ============================================================================
# HEALING NODE (inline, minimal — THA does the heavy lifting externally)
# ============================================================================

def healing_node(state: COGState) -> Dict[str, Any]:
    """
    COG Node 4 (conditional): THA Healing Integration

    This node processes un-applied THAInjections that arrived from
    the Telemetry & Healing Agent. For each injection:

      RETRY_SAME_TOOL / USE_ALTERNATE_TOOL:
        → Create a new PlanStep with the fallback tool
        → fan_out_node will re-dispatch it on the next cycle

      PARTIAL_RESULT_ACCEPT:
        → Mark the failed result as "partial" so synthesis accepts it

      SWAP_SPECIALIST:
        → Replace the failed worker type in plan_steps

      ABORT_SPECIALIST:
        → Remove the failed step, accept data gap

    After processing, control returns to fan_out_node for re-dispatch,
    or directly to synthesis if all injections are PARTIAL_RESULT_ACCEPT.
    """
    from orchestration.cog.state_schema import (
        PlanStep, DSWWorkerType, THAInjection, HealingStrategy,
        TelemetryEvent, FaultSeverity
    )
    from core_services.kafka_streams import KafkaTopic
    from utils.telemetry import emit_telemetry_event

    session_id  = state["session_id"]
    trace_id    = state["trace_id"]
    pending     = [i for i in state.get("tha_injections", []) if not i.applied]

    new_plan_steps: list[PlanStep] = []
    updated_warnings: list[str]    = []
    needs_redispatch = False

    for inj in pending:
        inj.applied = True

        if inj.strategy in (
            HealingStrategy.RETRY_SAME_TOOL.value,
            HealingStrategy.USE_ALTERNATE_TOOL.value,
        ):
            # Build a new PlanStep using the fallback tool/server
            new_step = PlanStep(
                worker_type    = inj.target_worker,
                query          = inj.remediation_query or "Retry with alternate data source",
                required_tools = [inj.fallback_tool] if inj.fallback_tool else [],
                mcp_server_id  = inj.fallback_server or "",
                metadata       = {"healing_ticket": inj.ticket_id, "retry": True},
            )
            new_plan_steps.append(new_step)
            needs_redispatch = True

        elif inj.strategy == HealingStrategy.PARTIAL_RESULT_ACCEPT.value:
            updated_warnings.append(
                f"THA: Accepted partial result for {inj.target_worker} [{inj.ticket_id}]"
            )

        elif inj.strategy == HealingStrategy.SWAP_SPECIALIST.value:
            # Would swap DSWWorkerType — simplified here
            updated_warnings.append(
                f"THA: Specialist swap requested for {inj.target_worker} [{inj.ticket_id}]"
            )

        healing_event = TelemetryEvent(
            session_id  = session_id,
            trace_id    = trace_id,
            source_node = "healing_node",
            event_type  = "healing_applied",
            severity    = FaultSeverity.MEDIUM,
            worker_type = inj.target_worker,
            detail      = {
                "ticket_id":  inj.ticket_id,
                "strategy":   inj.strategy,
                "new_steps":  len(new_plan_steps),
            },
        )
        emit_telemetry_event(healing_event, topic=KafkaTopic.SYS_EVENTS)

    output = {
        "workflow_phase":    WorkflowPhase.DISPATCHING if needs_redispatch else WorkflowPhase.SYNTHESIZING,
        "telemetry_events":  [],
        "warnings":          updated_warnings,
    }
    if new_plan_steps:
        # Extend plan_steps — fan_out_node will re-dispatch these
        output["plan_steps"] = state.get("plan_steps", []) + new_plan_steps

    return output


def route_after_healing(state: COGState) -> str:
    """Route from healing_node: re-dispatch or proceed to synthesis."""
    if state.get("workflow_phase") == WorkflowPhase.DISPATCHING:
        return "fan_out"
    return "synthesis"


# ============================================================================
# GRAPH BUILDER
# ============================================================================

def build_cog_graph(config: Optional[COGGraphConfig] = None) -> Any:
    """
    Compile and return the COG LangGraph StateGraph.

    Usage:
        graph = build_cog_graph()
        result = graph.invoke(initial_state(gia_request, trace_id))

    Streaming usage:
        for event in graph.stream(initial_state(...)):
            print(event)  # Node output deltas

    Returns:
        Compiled LangGraph (CompiledStateGraph) ready for .invoke() or .stream()
    """
    cfg = config or COGGraphConfig()

    # ── Build graph ──────────────────────────────────────────────────────────
    builder = StateGraph(COGState)

    # Register nodes
    builder.add_node("planning",  planning_node)
    builder.add_node("fan_out",   fan_out_node)
    builder.add_node("healing",   healing_node)
    builder.add_node("synthesis", synthesis_node)

    # ── Entry edge ───────────────────────────────────────────────────────────
    builder.add_edge(START, "planning")

    # ── Conditional edge: planning → fan_out | failed ────────────────────────
    builder.add_conditional_edges(
        "planning",
        route_after_planning,
        {
            "fan_out": "fan_out",
            "failed":   END,
        }
    )

    # ── Conditional edge: fan_out → await_results (polling checkpoint) ───────
    # In production, LangGraph interrupt() pauses here until DTB signals
    # job completion via Kafka. For simplicity, synthesis_node checks directly.
    builder.add_conditional_edges(
        "fan_out",
        route_after_await,          # Checks is_fan_in_complete + has_pending_healings
        {
            "synthesis":     "synthesis",
            "healing":       "healing",
            "await_results": "fan_out",  # Loop back — LangGraph handles with interrupt
        }
    )

    # ── Conditional edge: healing → fan_out | synthesis ──────────────────────
    builder.add_conditional_edges(
        "healing",
        route_after_healing,
        {
            "fan_out":   "fan_out",
            "synthesis": "synthesis",
        }
    )

    # ── Terminal edge ─────────────────────────────────────────────────────────
    builder.add_edge("synthesis", END)

    # ── Compile with optional checkpointing ──────────────────────────────────
    if cfg.enable_checkpointing:
        checkpointer = MemorySaver()
        compiled = builder.compile(checkpointer=checkpointer)
    else:
        compiled = builder.compile()

    return compiled


# ============================================================================
# GRAPH INVOCATION HELPERS
# ============================================================================

def invoke_cog(
    gia_request,
    trace_id: str,
    config: Optional[COGGraphConfig] = None,
    streaming: bool = False,
) -> Any:
    """
    High-level helper to invoke the COG graph.

    Args:
        gia_request: Validated GIARequest from the gateway
        trace_id:    Root OTel trace ID
        config:      Optional COGGraphConfig
        streaming:   If True, returns an iterator of node events

    Returns:
        Final COGState (or event iterator if streaming=True)
    """
    from orchestration.cog.state_schema import initial_state

    graph        = build_cog_graph(config)
    start_state  = initial_state(gia_request, trace_id)
    thread_config = {"configurable": {"thread_id": gia_request.session_id}}

    if streaming:
        return graph.stream(start_state, config=thread_config)
    else:
        return graph.invoke(start_state, config=thread_config)
