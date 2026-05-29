"""
orchestration/cog/state_schema.py
===================================
Global State Schema for the Central Orchestration Graph (COG).

This TypedDict flows through every node of the LangGraph StateGraph.
Every field is optional except session_id and raw_query, enabling
partial state transitions at each node without validation failures.

Design principles:
  - Immutable once written: nodes append, never overwrite results
  - All DSW results accumulate in `dsw_results` (fan-in target)
  - Telemetry events stream live to THA via `telemetry_bus`
  - Healing injections from THA arrive in `tha_injections`
"""

from __future__ import annotations

import uuid
from datetime import datetime
from enum import Enum
from typing import Annotated, Any, Dict, List, Optional, Union
from typing_extensions import TypedDict

from pydantic import BaseModel, Field, field_validator
import operator


# ============================================================================
# ENUMS
# ============================================================================

class WorkflowPhase(str, Enum):
    """Lifecycle phase of the COG graph execution."""
    RECEIVED      = "received"       # GIA handed off request
    PLANNING      = "planning"       # planning_node decomposing query
    DISPATCHING   = "dispatching"    # fan_out_node publishing to DTB
    EXECUTING     = "executing"      # DSWs running in parallel
    HEALING       = "healing"        # THA-triggered remediation in progress
    SYNTHESIZING  = "synthesizing"   # synthesis_node aggregating results
    RESPONDING    = "responding"     # COG returning blueprint to GIA
    FAILED        = "failed"         # Unrecoverable error state


class DSWWorkerType(str, Enum):
    """Canonical identifiers for all Domain Specialist Workers."""
    GEOLOGICAL_EXPERT        = "geological_expert"
    CHEMICAL_INFRA_ANALYST   = "chemical_infra_analyst"
    SUPPLY_CHAIN_FORECASTER  = "supply_chain_forecaster"
    LOGISTICS_COORDINATOR    = "logistics_coordinator"
    FAB_LOCATOR              = "fab_locator"
    MINING_LEASE_ANALYST     = "mining_lease_analyst"
    ENVIRONMENTAL_COMPLIANCE = "environmental_compliance"
    WORKFORCE_ANALYST        = "workforce_analyst"
    TRADE_POLICY_EXPERT      = "trade_policy_expert"
    THERMAL_MATERIALS_EXPERT = "thermal_materials_expert"
    SEMICONDUCTOR_GRADE_QA   = "semiconductor_grade_qa"


class FaultSeverity(str, Enum):
    """THA fault severity classification."""
    LOW      = "low"       # Informational, no retry needed
    MEDIUM   = "medium"    # Soft retry with alternate tool
    HIGH     = "high"      # Hard retry with fallback specialist
    CRITICAL = "critical"  # Escalate, mark workflow degraded


class HealingStrategy(str, Enum):
    """Self-healing actions dispatched by THA."""
    RETRY_SAME_TOOL      = "retry_same_tool"
    USE_ALTERNATE_TOOL   = "use_alternate_tool"
    SWAP_SPECIALIST      = "swap_specialist"
    PARTIAL_RESULT_ACCEPT = "partial_result_accept"
    ABORT_SPECIALIST     = "abort_specialist"


# ============================================================================
# PYDANTIC MODELS — Sub-objects that live inside COGState
# ============================================================================

class PlanStep(BaseModel):
    """A single decomposed subtask produced by the planning_node."""
    step_id:          str               = Field(default_factory=lambda: f"step-{uuid.uuid4().hex[:8]}")
    step_index:       int               = 0
    worker_type:      DSWWorkerType
    query:            str               # The specific sub-query for this DSW
    region_focus:     str               = "Southern India"
    material_focus:   Optional[str]     = None
    required_tools:   List[str]         = Field(default_factory=list)   # MCP tool names
    mcp_server_id:    str               = ""                             # Target MCP server
    priority:         int               = 5                              # 1 (highest) - 10 (lowest)
    depends_on:       List[str]         = Field(default_factory=list)   # step_ids that must complete first
    timeout_seconds:  int               = 45
    metadata:         Dict[str, Any]    = Field(default_factory=dict)

    class Config:
        use_enum_values = True


class MCPToolCall(BaseModel):
    """Record of a single MCP tool invocation within a DSW."""
    tool_name:    str
    server_id:    str
    invoked_at:   datetime = Field(default_factory=datetime.utcnow)
    latency_ms:   float    = 0.0
    success:      bool     = True
    error_detail: Optional[str] = None
    result_hash:  Optional[str] = None  # SHA256 of result for audit


class DSWResult(BaseModel):
    """Result envelope returned by a Domain Specialist Worker."""
    job_id:          str
    step_id:         str
    worker_type:     DSWWorkerType
    status:          str                           # completed | failed | timeout | partial
    data:            Optional[Dict[str, Any]]  = None
    summary:         Optional[str]             = None
    confidence:      float                     = 0.0   # 0.0 – 1.0
    sources_cited:   List[str]                 = Field(default_factory=list)
    mcp_calls:       List[MCPToolCall]         = Field(default_factory=list)
    execution_ms:    float                     = 0.0
    retry_count:     int                       = 0
    error_message:   Optional[str]             = None
    fallback_used:   bool                      = False
    completed_at:    datetime                  = Field(default_factory=datetime.utcnow)

    class Config:
        use_enum_values = True


class TelemetryEvent(BaseModel):
    """Structured telemetry event published to Kafka `sys-events` topic."""
    event_id:     str           = Field(default_factory=lambda: str(uuid.uuid4()))
    timestamp:    datetime      = Field(default_factory=datetime.utcnow)
    session_id:   str           = ""
    trace_id:     str           = ""
    source_node:  str           = ""                     # Node or agent emitting this event
    event_type:   str           = ""                     # timeout | data_missing | mcp_error | etc.
    severity:     FaultSeverity = FaultSeverity.LOW
    worker_type:  Optional[DSWWorkerType] = None
    job_id:       Optional[str] = None
    detail:       Dict[str, Any] = Field(default_factory=dict)
    resolved:     bool           = False

    class Config:
        use_enum_values = True


class THAInjection(BaseModel):
    """
    Healing directive pushed by THA back into the COG state.
    The synthesis_node checks this list before aggregating results.
    """
    injection_id:      str            = Field(default_factory=lambda: str(uuid.uuid4()))
    triggered_by:      str            = ""          # event_id that caused this
    target_step_id:    str            = ""          # which PlanStep to remediate
    target_worker:     DSWWorkerType
    strategy:          HealingStrategy
    fallback_tool:     Optional[str]  = None        # For USE_ALTERNATE_TOOL
    fallback_server:   Optional[str]  = None
    remediation_query: Optional[str]  = None        # Revised query to re-dispatch
    ticket_id:         str            = Field(default_factory=lambda: f"TKT-{uuid.uuid4().hex[:6].upper()}")
    injected_at:       datetime       = Field(default_factory=datetime.utcnow)
    applied:           bool           = False       # Set True when COG acts on it

    class Config:
        use_enum_values = True


class GIARequest(BaseModel):
    """Validated payload forwarded from the Gateway Interface Agent."""
    request_id:     str          = Field(default_factory=lambda: f"req-{uuid.uuid4().hex[:12]}")
    raw_query:      str
    user_id:        str          = "anonymous"
    session_id:     str          = Field(default_factory=lambda: f"sess-{uuid.uuid4().hex[:8]}")
    region_context: str          = "India"
    materials:      List[str]    = Field(default_factory=list)    # Extracted material names
    priority:       int          = 5
    received_at:    datetime     = Field(default_factory=datetime.utcnow)
    metadata:       Dict[str, Any] = Field(default_factory=dict)


class SynthesizedBlueprint(BaseModel):
    """
    Final output of the synthesis_node — the viability report
    returned to the GIA for delivery to the user.
    """
    blueprint_id:       str       = Field(default_factory=lambda: f"bp-{uuid.uuid4().hex[:10]}")
    session_id:         str       = ""
    title:              str       = ""
    executive_summary:  str       = ""
    material_profiles:  List[Dict[str, Any]] = Field(default_factory=list)
    supply_chain_map:   Dict[str, Any]       = Field(default_factory=dict)
    viability_score:    float                = 0.0        # 0.0 – 10.0
    risk_assessment:    Dict[str, Any]       = Field(default_factory=dict)
    recommendations:    List[str]            = Field(default_factory=list)
    data_gaps:          List[str]            = Field(default_factory=list)
    sources:            List[str]            = Field(default_factory=list)
    confidence_overall: float                = 0.0
    degraded_mode:      bool                 = False      # True if THA fallbacks were used
    generated_at:       datetime             = Field(default_factory=datetime.utcnow)
    processing_ms:      float                = 0.0


# ============================================================================
# GLOBAL COG STATE — TypedDict for LangGraph compatibility
# ============================================================================

class COGState(TypedDict, total=False):
    """
    The global state object that flows through every node of the
    Central Orchestration Graph (COG).

    LangGraph mechanics:
      - `Annotated[List, operator.add]` fields use reducer functions
        so parallel branches APPEND rather than overwrite.
      - All scalar fields use last-write-wins semantics.
      - The graph is compiled with `checkpointer=MemorySaver()` for
        mid-execution snapshots and resumption after THA healing.

    Data flow:
      GIA injects `gia_request` and `session_id`
        → planning_node fills `plan_steps`
        → fan_out_node fills `active_job_ids`
        → DSWs (via DTB workers) append to `dsw_results`
        → THA appends to `telemetry_events` and `tha_injections`
        → synthesis_node produces `synthesized_blueprint`
        → GIA reads `synthesized_blueprint` and returns to user
    """

    # ── Identity & Routing ───────────────────────────────────────────────────
    session_id:    str              # Unique session across GIA↔COG boundary
    trace_id:      str              # OpenTelemetry root trace ID
    request_id:    str              # GIA-assigned request ID
    workflow_phase: WorkflowPhase  # Current lifecycle phase

    # ── Input ────────────────────────────────────────────────────────────────
    raw_query:     str              # Original user query string
    gia_request:   GIARequest       # Full validated GIA payload

    # ── Planning ─────────────────────────────────────────────────────────────
    plan_steps:    List[PlanStep]   # Decomposed subtasks (output of planning_node)
    planner_reasoning: str          # Chain-of-thought from LLM planner

    # ── Dispatch ─────────────────────────────────────────────────────────────
    # Annotated with reducer so parallel fan_out branches can each append job IDs
    active_job_ids: Annotated[List[str], operator.add]
    dispatched_at:  str             # ISO timestamp of fan_out completion

    # ── DSW Results (Fan-In accumulator) ─────────────────────────────────────
    # Reducer appends each DSWResult as workers complete
    dsw_results:   Annotated[List[DSWResult], operator.add]

    # ── Telemetry Bus (live event stream) ────────────────────────────────────
    # Reducer appends events; THA reads these from Kafka, not directly from state
    telemetry_events: Annotated[List[TelemetryEvent], operator.add]

    # ── THA Self-Healing Injections ───────────────────────────────────────────
    # THA pushes THAInjections here; synthesis_node processes them
    tha_injections: Annotated[List[THAInjection], operator.add]

    # ── Synthesis Output ─────────────────────────────────────────────────────
    synthesized_blueprint: Optional[SynthesizedBlueprint]

    # ── Error Tracking ───────────────────────────────────────────────────────
    fatal_error:   Optional[str]    # Set on unrecoverable failure
    warnings:      Annotated[List[str], operator.add]

    # ── Timing ───────────────────────────────────────────────────────────────
    started_at:    str              # ISO timestamp (set by planning_node)
    completed_at:  Optional[str]    # ISO timestamp (set by synthesis_node)


# ============================================================================
# STATE HELPERS
# ============================================================================

def initial_state(gia_request: GIARequest, trace_id: str) -> COGState:
    """
    Build the initial COGState from a GIA request.
    Called at the entry point of the LangGraph graph.

    Args:
        gia_request: Validated GIA payload
        trace_id:    Root OTel trace ID propagated from GIA

    Returns:
        Minimal COGState ready for planning_node ingestion
    """
    return COGState(
        session_id        = gia_request.session_id,
        trace_id          = trace_id,
        request_id        = gia_request.request_id,
        workflow_phase    = WorkflowPhase.RECEIVED,
        raw_query         = gia_request.raw_query,
        gia_request       = gia_request,
        plan_steps        = [],
        planner_reasoning = "",
        active_job_ids    = [],
        dispatched_at     = "",
        dsw_results       = [],
        telemetry_events  = [],
        tha_injections    = [],
        synthesized_blueprint = None,
        fatal_error       = None,
        warnings          = [],
        started_at        = datetime.utcnow().isoformat(),
        completed_at      = None,
    )


def is_fan_in_complete(state: COGState) -> bool:
    """
    Check if all dispatched jobs have returned a DSW result.
    Used by the conditional edge after fan_out_node.
    """
    dispatched = len(state.get("active_job_ids", []))
    returned   = len(state.get("dsw_results", []))
    return dispatched > 0 and returned >= dispatched


def has_pending_healings(state: COGState) -> bool:
    """Check if THA has injected un-applied healing directives."""
    return any(
        not inj.applied
        for inj in state.get("tha_injections", [])
    )


def get_failed_steps(state: COGState) -> List[str]:
    """Return step_ids of DSW results that failed or timed out."""
    return [
        r.step_id
        for r in state.get("dsw_results", [])
        if r.status in ("failed", "timeout")
    ]
