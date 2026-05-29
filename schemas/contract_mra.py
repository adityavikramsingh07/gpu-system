"""
MRA (Master Reasoning Agent) State Schema & Contracts

Defines the complete state flowing through the LangGraph DAG,
node signatures, and inter-component communication contracts.
"""

from typing import Dict, List, Any, Optional, TypedDict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import uuid


# ============================================================================
# STATE SCHEMA (Flows through LangGraph)
# ============================================================================

class MRAState(TypedDict):
    """
    Complete state flowing through the Master Reasoning Agent's DAG.
    
    This state is passed through every node:
    planner_node -> dispatcher_node -> synthesizer_node
    with fan-out to specialist agents in dispatcher.
    """
    
    # Request metadata
    request_id: str                    # Unique request ID
    user_id: str                       # User/tenant identifier
    timestamp: float                   # Request timestamp (Unix)
    trace_id: str                      # OpenTelemetry trace ID
    
    # User query
    query: str                         # Original user question
    geographic_scope: List[str]        # Target regions (e.g., ["TN", "KA"])
    urgency: str                       # "low", "medium", "high", "critical"
    
    # Planner output
    objective: str                     # Refined objective
    specialist_tasks: List[str]        # Required specialist agent types
    reasoning_chain: List[str]         # Step-by-step reasoning
    execution_plan: Dict[str, Any]     # Task breakdown & sequencing
    
    # Dispatcher output
    job_ids: List[str]                 # EJMS job IDs for specialist tasks
    dispatch_status: str               # "pending", "dispatched", "executing", "completed"
    dispatch_errors: List[Dict]        # Any dispatch failures
    
    # Specialist results (aggregated)
    specialist_results: Dict[str, Any] # {specialist_type: result_dict}
    failed_specialists: List[str]      # Specialist agents that failed
    partial_results: Dict[str, Any]    # Incomplete/degraded results
    
    # Synthesis output
    blueprint: Dict[str, Any]          # Final consolidated blueprint
    confidence_scores: Dict[str, float]# Per-section confidence
    gaps: List[str]                    # Data gaps or limitations
    recommendations: List[str]         # Actionable recommendations
    
    # Status tracking
    current_node: str                  # Current DAG node
    status: str                        # "planning", "dispatching", "synthesizing", "complete"
    error: Optional[str]               # Error message if failed
    completion_time: Optional[float]   # Execution time in seconds


class SpecialistTask(TypedDict):
    """Individual task dispatched to a specialist agent."""
    goal: str                          # High-level goal
    material_types: List[str]          # Materials to search
    geographic_scope: List[str]        # Regions to cover
    mcp_tools: List[str]              # MCP tools to use
    depth: str                         # "quick", "standard", "deep"


class DispatcherRequest(TypedDict):
    """Request structure sent by dispatcher to EJMS."""
    job_id: str
    specialist_type: str               # "geological_scout", "chemical_infra", etc.
    task: SpecialistTask
    timeout_seconds: int
    credentials_required: List[str]    # MCP servers needing credentials


class DispatcherResponse(TypedDict):
    """Response returned from EJMS after specialist execution."""
    job_id: str
    specialist_type: str
    status: str                        # "success", "failure", "timeout", "partial"
    result: Dict[str, Any]
    execution_time_ms: int
    error_message: Optional[str]


# ============================================================================
# NODE SIGNATURES
# ============================================================================

@dataclass
class PlannerNodeOutput:
    """Output of the Planner node."""
    objective: str
    specialist_tasks: List[SpecialistTask]
    reasoning_chain: List[str]
    execution_plan: Dict[str, Any]
    confidence: float


@dataclass
class DispatcherNodeOutput:
    """Output of the Dispatcher node."""
    job_ids: List[str]
    dispatch_status: str
    dispatch_errors: List[Dict]
    specialist_count: int


@dataclass
class SynthesizerNodeOutput:
    """Output of the Synthesizer node."""
    blueprint: Dict[str, Any]
    confidence_scores: Dict[str, float]
    gaps: List[str]
    recommendations: List[str]
    synthesis_notes: str


# ============================================================================
# NODE DEFINITIONS (LangGraph)
# ============================================================================

class MRANodeContract:
    """
    Contract for each node in the MRA graph.
    
    Each node receives the full MRAState and returns updated MRAState.
    """

    @staticmethod
    def planner_node(state: MRAState) -> MRAState:
        """
        PLANNER NODE:
        - Analyzes user query
        - Determines which specialists are needed
        - Creates task breakdown
        
        Input State:
            - query, geographic_scope, urgency
        
        Output State Updates:
            - objective, specialist_tasks, reasoning_chain, execution_plan
        
        Implementation:
            1. Parse query with LLM (with system prompt for GPU supply chain)
            2. Identify required specialists (geological, chemical, logistics, etc.)
            3. Create structured tasks for each specialist
            4. Determine execution sequence & parallelization
            5. Return updated state
        """
        pass

    @staticmethod
    def dispatcher_node(
        state: MRAState,
        ejms_client  # EJMS client
    ) -> MRAState:
        """
        DISPATCHER NODE:
        - Sends specialist tasks to EJMS
        - Manages parallel execution
        - Awaits all job completions
        
        Input State:
            - specialist_tasks, execution_plan
        
        Output State Updates:
            - job_ids, dispatch_status, specialist_results
        
        Implementation:
            1. For each specialist task:
               a. Create DispatcherRequest
               b. Call ACS to get temporary credentials for MCP servers
               c. Submit to EJMS via ejms_client.submit_job()
               d. Store job_id
            2. Wait for all jobs via ejms_client.wait_all_jobs(job_ids, timeout)
            3. Aggregate specialist_results
            4. Handle partial failures
            5. Return updated state
        """
        pass

    @staticmethod
    def synthesizer_node(state: MRAState) -> MRAState:
        """
        SYNTHESIZER NODE:
        - Consolidates all specialist results
        - Generates final blueprint
        - Identifies gaps & recommendations
        
        Input State:
            - specialist_results, failed_specialists, partial_results
        
        Output State Updates:
            - blueprint, confidence_scores, gaps, recommendations
        
        Implementation:
            1. Merge all specialist_results into unified data model
            2. Call LLM to synthesize blueprint (combining multiple perspectives)
            3. Calculate confidence scores per section
            4. Identify data gaps & anomalies
            5. Generate actionable recommendations
            6. Return updated state
        """
        pass


# ============================================================================
# EDGES (State Transitions)
# ============================================================================

class MRAEdgeContract:
    """
    Defines transitions between nodes and conditional routing.
    
    LangGraph DAG structure:
    
    START
      |
      v
    planner_node
      |
      v
    dispatcher_node (fan-out to specialists)
      |
      +-- specialist_agent_1
      +-- specialist_agent_2
      +-- specialist_agent_3
      ...
      +-- specialist_agent_N
      |
      v (fan-in after all complete or timeout)
    synthesizer_node
      |
      v
    END
    """

    @staticmethod
    def should_continue_to_synthesis(state: MRAState) -> str:
        """
        Conditional routing after dispatcher node.
        
        Returns:
            "synthesizer" -> proceed to synthesis
            "retry_dispatch" -> re-attempt failed jobs
            "error" -> abort
        """
        if state["dispatch_status"] == "failed":
            # Retry with degraded mode or abort
            if len(state["failed_specialists"]) / len(state["specialist_tasks"]) < 0.3:
                # < 30% failure: continue with partial results
                return "synthesizer"
            else:
                # > 30% failure: abort
                return "error"
        return "synthesizer"


# ============================================================================
# FAN-OUT/FAN-IN LOGIC
# ============================================================================

class FanOutFanInContract:
    """
    Details how the MRA dispatches to multiple specialists
    and aggregates results.
    """

    @staticmethod
    def fan_out_strategy(specialist_tasks: List[SpecialistTask]) -> Dict[str, List]:
        """
        Determine parallelization strategy.
        
        Returns:
            {
                "parallel_batches": [
                    [task_1, task_2, task_3],  # Batch 1 (can run in parallel)
                    [task_4, task_5]            # Batch 2 (depends on Batch 1)
                ],
                "dependencies": {
                    "task_4": ["task_1", "task_2"]
                }
            }
        """
        # Geological + Chemical + Logistics can run in parallel (no dependencies)
        # Synthesis depends on all above
        return {
            "parallel_batches": [
                ["geological_scout", "chemical_infra", "logistics_analyst"],
                ["fab_locator"],  # Can use results from above
            ],
            "dependencies": {}
        }

    @staticmethod
    def fan_in_aggregation(
        specialist_results: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Merge specialist results into unified structure.
        
        Input:
            {
                "geological_scout": { "deposits": [...], "confidence": 0.85 },
                "chemical_infra": { "facilities": [...], "confidence": 0.78 },
                "logistics_analyst": { "trade_flows": [...], "confidence": 0.82 }
            }
        
        Returns:
            {
                "material_sources": {...},
                "infrastructure": {...},
                "supply_chain": {...},
                "aggregated_confidence": 0.81,
                "conflicts": [...]  # Data conflicts detected
            }
        """
        pass

    @staticmethod
    def handle_partial_failure(
        completed: Dict[str, Any],
        failed: List[str],
        results_so_far: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Handle case where some specialists fail but others succeed.
        Decides whether to continue synthesis or abort.
        
        Strategy:
        - If core agents (geological, supply chain) fail: abort
        - If secondary agents fail: flag and continue with warnings
        """
        pass


# ============================================================================
# ROUTING CONTRACT
# ============================================================================

class RoutingContract:
    """
    Determines which specialist agents are needed based on query.
    """

    QUERY_PATTERNS_TO_AGENTS = {
        r"(raw materials|deposits|mining|quartz|copper|rare earth)": "geological_scout",
        r"(chemical|refinery|gases|photoresist|epoxy)": "chemical_infra",
        r"(logistics|supply chain|import|export|port)": "logistics_analyst",
        r"(fab|fabrication|osat|assembly|cleanroom)": "fab_locator",
        r"(workforce|labor|skills|training)": "workforce_analyzer",
        r"(thermal|cooling|heat|interface)": "thermal_specialist",
    }

    @staticmethod
    def query_to_specialists(query: str) -> List[str]:
        """
        Map user query to required specialist types.
        
        Example:
            "Source HPQ and copper foils in South India"
            -> ["geological_scout", "logistics_analyst", "fab_locator"]
        """
        import re
        specialists = set()
        
        for pattern, specialist in RoutingContract.QUERY_PATTERNS_TO_AGENTS.items():
            if re.search(pattern, query, re.IGNORECASE):
                specialists.add(specialist)
        
        # Always include logistics for supply chain visibility
        specialists.add("logistics_analyst")
        
        return list(specialists)


if __name__ == "__main__":
    print("MRA State Schema & Contracts loaded")
    
    # Example state initialization
    example_state: MRAState = {
        "request_id": str(uuid.uuid4()),
        "user_id": "user_42",
        "timestamp": datetime.now().timestamp(),
        "trace_id": str(uuid.uuid4()),
        "query": "Source High-Purity Quartz and copper foils in Southern India",
        "geographic_scope": ["TN", "KA", "AP"],
        "urgency": "high",
        "objective": "",
        "specialist_tasks": [],
        "reasoning_chain": [],
        "execution_plan": {},
        "job_ids": [],
        "dispatch_status": "pending",
        "dispatch_errors": [],
        "specialist_results": {},
        "failed_specialists": [],
        "partial_results": {},
        "blueprint": {},
        "confidence_scores": {},
        "gaps": [],
        "recommendations": [],
        "current_node": "planner",
        "status": "planning",
        "error": None,
        "completion_time": None,
    }
    
    print(f"Example state request_id: {example_state['request_id']}")
