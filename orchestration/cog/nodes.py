"""
orchestration/cog/nodes.py
===========================
The three core nodes of the Central Orchestration Graph (COG):

  planning_node   – LLM-driven query decomposition → List[PlanStep]
  fan_out_node    – Publishes PlanSteps to DTB; returns active_job_ids
  synthesis_node  – Aggregates DSWResult list → SynthesizedBlueprint

Each node receives the full COGState, mutates it, and returns a
partial state dict that LangGraph merges using the field reducers
defined in state_schema.py.

Dependencies:
  - orchestration/cog/state_schema.py
  - core_services/dtb.py
  - core_services/kafka_streams.py
  - utils/telemetry.py
"""

from __future__ import annotations

import json
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from langchain_core.messages import SystemMessage, HumanMessage
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.output_parsers import JsonOutputParser

from orchestration.cog.state_schema import (
    COGState, PlanStep, DSWResult, DSWWorkerType,
    SynthesizedBlueprint, TelemetryEvent, WorkflowPhase,
    FaultSeverity, initial_state, is_fan_in_complete, has_pending_healings
)
from orchestration.cog.prompts import (
    PLANNER_SYSTEM_PROMPT,
    SYNTHESIS_SYSTEM_PROMPT,
    format_planner_prompt,
    format_synthesis_prompt,
)
from core_services.dtb import DistributedTaskBroker, TaskEnvelope, TaskPriority
from core_services.kafka_streams import KafkaStreamManager, KafkaTopic
from utils.telemetry import get_tracer, emit_telemetry_event


# ============================================================================
# PLANNING NODE
# ============================================================================

def planning_node(state: COGState) -> Dict[str, Any]:
    """
    COG Node 1: Query Decomposition & Planning

    Responsibilities:
      1. Receive the validated GIA request from state
      2. Invoke LLM with PLANNER_SYSTEM_PROMPT to decompose the query
      3. Parse JSON output into List[PlanStep] objects
      4. Determine which DSWs to invoke and in what order (with deps)
      5. Emit PLANNING telemetry event to Kafka

    Input state keys used:
      - raw_query, gia_request, session_id, trace_id

    Output state keys written:
      - plan_steps, planner_reasoning, workflow_phase, telemetry_events

    LLM contract:
      The planner prompt instructs the LLM to return a JSON object:
      {
        "reasoning": "<chain_of_thought>",
        "steps": [
          {
            "step_index": 0,
            "worker_type": "geological_expert",
            "query": "...",
            "material_focus": "High-Purity Quartz",
            "region_focus": "Tamil Nadu, Karnataka",
            "required_tools": ["query_mining_deposits", "get_lease_status"],
            "mcp_server_id": "geological-mcp-server",
            "priority": 1,
            "depends_on": [],
            "timeout_seconds": 45
          },
          ...  # Up to 11 steps for 11 DSWs
        ]
      }
    """
    tracer = get_tracer()
    session_id = state["session_id"]
    trace_id   = state["trace_id"]

    with tracer.start_as_current_span("cog.planning_node") as span:
        span.set_attribute("session_id", session_id)
        span.set_attribute("trace_id", trace_id)
        span.set_attribute("raw_query", state["raw_query"][:200])

        t_start = time.perf_counter()

        # ── Step 1: Build planning prompt ────────────────────────────────────
        planner_prompt = format_planner_prompt(
            raw_query     = state["raw_query"],
            gia_request   = state["gia_request"],
            available_dsw = [w.value for w in DSWWorkerType],
        )

        # ── Step 2: Call LLM (Gemini with JSON mode) ──────────────────────────
        llm = ChatGoogleGenerativeAI(
            model="gemini-2.0-flash",
            response_mime_type="application/json",
            temperature=0.1,   # Low temp for deterministic planning
        )

        messages = [
            SystemMessage(content=PLANNER_SYSTEM_PROMPT),
            HumanMessage(content=planner_prompt),
        ]

        try:
            raw_response = llm.invoke(messages)
            plan_json    = json.loads(raw_response.content)
        except Exception as e:
            # Emit fault event and return degraded state
            fault_event = TelemetryEvent(
                session_id  = session_id,
                trace_id    = trace_id,
                source_node = "planning_node",
                event_type  = "llm_call_failed",
                severity    = FaultSeverity.CRITICAL,
                detail      = {"error": str(e)},
            )
            return {
                "workflow_phase":    WorkflowPhase.FAILED,
                "fatal_error":       f"planning_node LLM failure: {e}",
                "telemetry_events":  [fault_event],
            }

        reasoning  = plan_json.get("reasoning", "")
        steps_raw  = plan_json.get("steps", [])

        # ── Step 3: Parse into PlanStep objects ──────────────────────────────
        plan_steps: List[PlanStep] = []
        for raw_step in steps_raw:
            try:
                step = PlanStep(
                    step_index    = raw_step.get("step_index", len(plan_steps)),
                    worker_type   = DSWWorkerType(raw_step["worker_type"]),
                    query         = raw_step["query"],
                    material_focus = raw_step.get("material_focus"),
                    region_focus  = raw_step.get("region_focus", "Southern India"),
                    required_tools = raw_step.get("required_tools", []),
                    mcp_server_id = raw_step.get("mcp_server_id", ""),
                    priority      = raw_step.get("priority", 5),
                    depends_on    = raw_step.get("depends_on", []),
                    timeout_seconds = raw_step.get("timeout_seconds", 45),
                    metadata      = raw_step.get("metadata", {}),
                )
                plan_steps.append(step)
            except (KeyError, ValueError) as e:
                # Warn but continue — partial plans are valid
                span.add_event(f"Skipped malformed step: {e}")

        elapsed_ms = (time.perf_counter() - t_start) * 1000
        span.set_attribute("plan_steps_count", len(plan_steps))
        span.set_attribute("execution_latency_ms", elapsed_ms)

        # ── Step 4: Emit planning success telemetry ──────────────────────────
        planning_event = TelemetryEvent(
            session_id  = session_id,
            trace_id    = trace_id,
            source_node = "planning_node",
            event_type  = "planning_complete",
            severity    = FaultSeverity.LOW,
            detail      = {
                "steps_generated": len(plan_steps),
                "workers_assigned": [s.worker_type for s in plan_steps],
                "latency_ms": round(elapsed_ms, 2),
            },
        )

        emit_telemetry_event(planning_event, topic=KafkaTopic.SYS_EVENTS)

        return {
            "workflow_phase":    WorkflowPhase.DISPATCHING,
            "plan_steps":        plan_steps,
            "planner_reasoning": reasoning,
            "telemetry_events":  [planning_event],
            "started_at":        datetime.utcnow().isoformat(),
        }


# ============================================================================
# FAN OUT NODE
# ============================================================================

def fan_out_node(state: COGState) -> Dict[str, Any]:
    """
    COG Node 2: Parallel Fan-Out to Distributed Task Broker (DTB)

    Responsibilities:
      1. Read plan_steps from state
      2. Resolve dependency ordering (independent steps go first)
      3. Publish each PlanStep as a TaskEnvelope to the DTB
      4. Collect returned job_ids into active_job_ids
      5. Emit DISPATCH telemetry events

    Fan-out mechanics:
      - Steps with empty `depends_on` are dispatched immediately
      - Dependent steps are queued in DTB with `prerequisite_job_ids`
      - The DTB workers handle sequencing via its internal scheduler
      - LangGraph sends this node output to ALL downstream DSW branches
        simultaneously via `Send()` API for true parallel execution

    LangGraph parallel execution pattern:
      The fan_out_node does NOT block waiting for results.
      It returns immediately after publishing all jobs.
      LangGraph's `Send()` creates N parallel branches — one per DSW.
      Each branch executes independently and appends to `dsw_results`.
      The conditional edge `await_fan_in` loops back here if more
      jobs are still pending, or routes to synthesis_node when complete.

    Input state keys used:
      - plan_steps, session_id, trace_id, request_id

    Output state keys written:
      - active_job_ids, dispatched_at, workflow_phase, telemetry_events
    """
    tracer     = get_tracer()
    session_id = state["session_id"]
    trace_id   = state["trace_id"]
    request_id = state["request_id"]
    plan_steps = state.get("plan_steps", [])

    with tracer.start_as_current_span("cog.fan_out_node") as span:
        span.set_attribute("session_id", session_id)
        span.set_attribute("trace_id", trace_id)
        span.set_attribute("plan_steps_count", len(plan_steps))

        dtb = DistributedTaskBroker()
        job_ids: List[str] = []
        dispatch_events: List[TelemetryEvent] = []

        # ── Sort steps by priority and dependency order ─────────────────────
        # Independent steps (depends_on=[]) first, then dependent
        independent = [s for s in plan_steps if not s.depends_on]
        dependent   = [s for s in plan_steps if s.depends_on]
        ordered     = independent + dependent

        # ── Publish each step to DTB ─────────────────────────────────────────
        for step in ordered:
            envelope = TaskEnvelope(
                task_id          = step.step_id,
                parent_request_id = request_id,
                session_id       = session_id,
                trace_id         = trace_id,
                worker_type      = step.worker_type,
                query            = step.query,
                region_focus     = step.region_focus,
                material_focus   = step.material_focus or "",
                required_tools   = step.required_tools,
                mcp_server_id    = step.mcp_server_id,
                timeout_seconds  = step.timeout_seconds,
                priority         = TaskPriority(min(step.priority, 5)),
                prerequisite_job_ids = [],  # DTB resolves dependencies
            )

            job_id = dtb.publish(envelope)
            job_ids.append(job_id)

            dispatch_event = TelemetryEvent(
                session_id  = session_id,
                trace_id    = trace_id,
                source_node = "fan_out_node",
                event_type  = "job_dispatched",
                severity    = FaultSeverity.LOW,
                worker_type = step.worker_type,
                job_id      = job_id,
                detail      = {
                    "step_id":       step.step_id,
                    "mcp_server_id": step.mcp_server_id,
                    "timeout_s":     step.timeout_seconds,
                },
            )
            dispatch_events.append(dispatch_event)
            emit_telemetry_event(dispatch_event, topic=KafkaTopic.SYS_EVENTS)

        span.set_attribute("jobs_dispatched", len(job_ids))

        return {
            "workflow_phase":   WorkflowPhase.EXECUTING,
            "active_job_ids":   job_ids,
            "dispatched_at":    datetime.utcnow().isoformat(),
            "telemetry_events": dispatch_events,
        }


# ============================================================================
# SYNTHESIS NODE
# ============================================================================

def synthesis_node(state: COGState) -> Dict[str, Any]:
    """
    COG Node 3: Fan-In Aggregation & Blueprint Synthesis

    Responsibilities:
      1. Collect all DSWResult objects from state (fan-in complete)
      2. Process any pending THA healing injections
      3. Classify results: completed | partial | failed
      4. Invoke LLM with SYNTHESIS_SYSTEM_PROMPT to generate blueprint
      5. Score viability, identify data gaps, list recommendations
      6. Package into SynthesizedBlueprint and return to GIA

    Self-healing integration:
      Before synthesis, this node checks `tha_injections` for un-applied
      directives. If PARTIAL_RESULT_ACCEPT is set, it uses the fallback
      data. If USE_ALTERNATE_TOOL or RETRY_SAME_TOOL led to a new DSWResult,
      it replaces the failed result in the aggregation pass.

    Parallel result aggregation:
      dsw_results is a reducer-accumulated list. By the time synthesis_node
      runs, it contains ALL results from all 10+ DSW workers. Each result
      is keyed by worker_type for deduplication.

    Input state keys used:
      - dsw_results, tha_injections, plan_steps, gia_request,
        session_id, trace_id, planner_reasoning

    Output state keys written:
      - synthesized_blueprint, workflow_phase, completed_at,
        telemetry_events
    """
    tracer     = get_tracer()
    session_id = state["session_id"]
    trace_id   = state["trace_id"]
    dsw_results = state.get("dsw_results", [])
    tha_injs    = state.get("tha_injections", [])
    gia_request = state["gia_request"]

    with tracer.start_as_current_span("cog.synthesis_node") as span:
        span.set_attribute("session_id", session_id)
        span.set_attribute("trace_id", trace_id)
        span.set_attribute("dsw_results_count", len(dsw_results))
        span.set_attribute("tha_injections_count", len(tha_injs))

        t_start = time.perf_counter()

        # ── Step 1: Apply THA healing injections ────────────────────────────
        degraded_mode = False
        applied_tickets: List[str] = []

        pending_injs = [i for i in tha_injs if not i.applied]
        for inj in pending_injs:
            inj.applied = True
            applied_tickets.append(inj.ticket_id)
            if inj.strategy == "partial_result_accept":
                degraded_mode = True
                span.add_event(f"THA: Accepting partial result for {inj.target_worker} [ticket={inj.ticket_id}]")

        # ── Step 2: Deduplicate & classify results ───────────────────────────
        # Keep best result per worker_type (highest confidence)
        best_results: Dict[str, DSWResult] = {}
        for r in dsw_results:
            key = r.worker_type
            if key not in best_results:
                best_results[key] = r
            else:
                if r.confidence > best_results[key].confidence:
                    best_results[key] = r

        completed_results = [r for r in best_results.values() if r.status == "completed"]
        failed_results    = [r for r in best_results.values() if r.status in ("failed", "timeout")]
        partial_results   = [r for r in best_results.values() if r.status == "partial"]

        span.set_attribute("completed_results", len(completed_results))
        span.set_attribute("failed_results", len(failed_results))
        span.set_attribute("partial_results", len(partial_results))

        # ── Step 3: Prepare synthesis context for LLM ───────────────────────
        synthesis_context = {
            "original_query":  state["raw_query"],
            "region":          gia_request.region_context,
            "materials":       gia_request.materials,
            "completed_data":  [
                {"worker": r.worker_type, "summary": r.summary, "data": r.data, "confidence": r.confidence}
                for r in completed_results
            ],
            "partial_data": [
                {"worker": r.worker_type, "summary": r.summary, "confidence": r.confidence}
                for r in partial_results
            ],
            "failed_workers": [
                {"worker": r.worker_type, "error": r.error_message}
                for r in failed_results
            ],
            "tha_applied_tickets": applied_tickets,
            "degraded_mode": degraded_mode,
        }

        synthesis_prompt = format_synthesis_prompt(synthesis_context)

        # ── Step 4: Call LLM for final synthesis ─────────────────────────────
        llm = ChatGoogleGenerativeAI(
            model="gemini-2.0-flash",
            response_mime_type="application/json",
            temperature=0.2,
        )

        messages = [
            SystemMessage(content=SYNTHESIS_SYSTEM_PROMPT),
            HumanMessage(content=synthesis_prompt),
        ]

        try:
            raw_response  = llm.invoke(messages)
            synth_json    = json.loads(raw_response.content)
        except Exception as e:
            span.set_attribute("synthesis_error", str(e))
            # Return minimal blueprint on LLM failure
            synth_json = {
                "title": "Partial Viability Report (Synthesis Error)",
                "executive_summary": f"Synthesis failed: {e}",
                "viability_score": 0.0,
                "recommendations": [],
                "data_gaps": ["Full synthesis unavailable"],
            }
            degraded_mode = True

        elapsed_ms = (time.perf_counter() - t_start) * 1000

        # ── Step 5: Build SynthesizedBlueprint ──────────────────────────────
        all_sources = []
        for r in completed_results + partial_results:
            all_sources.extend(r.sources_cited)

        blueprint = SynthesizedBlueprint(
            session_id          = session_id,
            title               = synth_json.get("title", f"GPU Supply Chain Report — {session_id}"),
            executive_summary   = synth_json.get("executive_summary", ""),
            material_profiles   = synth_json.get("material_profiles", []),
            supply_chain_map    = synth_json.get("supply_chain_map", {}),
            viability_score     = float(synth_json.get("viability_score", 0.0)),
            risk_assessment     = synth_json.get("risk_assessment", {}),
            recommendations     = synth_json.get("recommendations", []),
            data_gaps           = synth_json.get("data_gaps", [
                r.worker_type for r in failed_results
            ]),
            sources             = list(set(all_sources)),
            confidence_overall  = (
                sum(r.confidence for r in completed_results + partial_results) /
                max(len(completed_results + partial_results), 1)
            ),
            degraded_mode       = degraded_mode,
            processing_ms       = elapsed_ms,
        )

        span.set_attribute("viability_score", blueprint.viability_score)
        span.set_attribute("confidence_overall", blueprint.confidence_overall)
        span.set_attribute("degraded_mode", degraded_mode)
        span.set_attribute("execution_latency_ms", elapsed_ms)

        # ── Step 6: Emit synthesis complete telemetry ─────────────────────────
        synth_event = TelemetryEvent(
            session_id  = session_id,
            trace_id    = trace_id,
            source_node = "synthesis_node",
            event_type  = "synthesis_complete",
            severity    = FaultSeverity.LOW,
            detail      = {
                "blueprint_id":      blueprint.blueprint_id,
                "viability_score":   blueprint.viability_score,
                "confidence":        round(blueprint.confidence_overall, 3),
                "degraded_mode":     degraded_mode,
                "workers_completed": len(completed_results),
                "workers_failed":    len(failed_results),
                "latency_ms":        round(elapsed_ms, 2),
            },
        )
        emit_telemetry_event(synth_event, topic=KafkaTopic.SYS_EVENTS)

        return {
            "workflow_phase":       WorkflowPhase.RESPONDING,
            "synthesized_blueprint": blueprint,
            "completed_at":         datetime.utcnow().isoformat(),
            "telemetry_events":     [synth_event],
        }


# ============================================================================
# CONDITIONAL EDGE FUNCTIONS (used by graph.py routing)
# ============================================================================

def route_after_planning(state: COGState) -> str:
    """
    Route from planning_node:
      - 'failed' if planning failed (LLM error)
      - 'fan_out' if plan_steps generated successfully
    """
    if state.get("fatal_error"):
        return "failed"
    if state.get("plan_steps"):
        return "fan_out"
    return "failed"


def route_after_fan_out(state: COGState) -> str:
    """
    Route from fan_out_node:
      Always go to 'await_results' — the graph loops back here
      until all jobs complete or a healing cycle begins.
    """
    return "await_results"


def route_after_await(state: COGState) -> str:
    """
    Route from the polling/await checkpoint:
      - 'healing' if THA has unresolved injections for failed jobs
      - 'synthesis' if all jobs completed (success or acceptable failure)
      - 'await_results' to keep waiting (handled by LangGraph interrupt)
    """
    if has_pending_healings(state):
        return "healing"
    if is_fan_in_complete(state):
        return "synthesis"
    return "await_results"  # LangGraph will interrupt and resume
