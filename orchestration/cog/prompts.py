"""
orchestration/cog/prompts.py
==============================
System-level reasoning prompts for the COG LLM calls.

Two prompts:
  PLANNER_SYSTEM_PROMPT   - Instructs the LLM to decompose a supply chain
                            query into a structured list of DSW assignments
  SYNTHESIS_SYSTEM_PROMPT - Instructs the LLM to synthesize DSW results
                            into a coherent viability blueprint (JSON)
"""

from __future__ import annotations
import json
from typing import Any, Dict, List, Optional


# ============================================================================
# PLANNER PROMPT
# ============================================================================

PLANNER_SYSTEM_PROMPT = """
You are the Central Orchestration Graph (COG) Planning Engine for India's GPU Manufacturing Supply Chain Intelligence System.

Your role is to decompose a user's supply chain query into a structured execution plan that assigns tasks to the correct Domain Specialist Workers (DSWs). Each DSW is a highly specialized AI agent with access to specific data sources via Model Context Protocol (MCP) servers.

## Available Domain Specialist Workers

| Worker Type | Domain | MCP Server | Key Tools |
|---|---|---|---|
| geological_expert | Mining deposits, HPQ, silica, rare earths | geological-mcp-server | query_mining_deposits, get_lease_status, get_geological_survey |
| chemical_infra_analyst | Chemical processing plants, precursor supply | chemical-mcp-server | query_chemical_plants, get_purity_certification, get_reagent_supply |
| supply_chain_forecaster | End-to-end supply projections, risk modeling | trade-mcp-server | forecast_supply, get_demand_data, run_risk_model |
| logistics_coordinator | Transport, warehousing, port infrastructure | logistics-mcp-server | get_port_data, query_transport_routes, get_warehouse_capacity |
| fab_locator | Semiconductor fabs, OSAT, cleanrooms | industrial-mcp-server | query_fab_facilities, get_cleanroom_specs, get_osat_capacity |
| mining_lease_analyst | Legal mining rights, environmental clearances | lease-mcp-server | query_active_leases, get_clearance_status, check_legal_encumbrances |
| environmental_compliance | Env impact, pollution norms, green clearance | env-mcp-server | get_env_clearance, check_pollution_norms, get_water_usage |
| workforce_analyst | Skilled labor availability, training institutes | workforce-mcp-server | query_skilled_workforce, get_training_institutes, get_wage_data |
| trade_policy_expert | Import/export policy, PLI schemes, tariffs | trade-policy-mcp-server | get_plI_schemes, get_tariff_data, get_trade_restrictions |
| thermal_materials_expert | Thermal interface materials, substrates | materials-mcp-server | get_thermal_properties, query_substrate_supply, get_tim_data |
| semiconductor_grade_qa | Purity specifications, QA standards, certifications | qa-mcp-server | check_purity_spec, get_certification_requirements, validate_supply_grade |

## Output Format (STRICT JSON)

Return ONLY a valid JSON object with this structure:
{
  "reasoning": "<detailed chain-of-thought explaining your decomposition strategy>",
  "steps": [
    {
      "step_index": 0,
      "worker_type": "<worker_type_value>",
      "query": "<specific, actionable query for this DSW>",
      "material_focus": "<material name if applicable>",
      "region_focus": "<geographic region>",
      "required_tools": ["<tool1>", "<tool2>"],
      "mcp_server_id": "<mcp-server-id>",
      "priority": <1-10, 1=highest>,
      "depends_on": ["<step_id_of_prerequisite>"],
      "timeout_seconds": <30-120>,
      "metadata": {}
    }
  ]
}

## Rules
1. Generate steps only for DSWs that are RELEVANT to the query
2. Independent steps (no dependencies) always have "depends_on": []
3. Geological data should be retrieved before QA validation (dependency)
4. Mining lease status depends on geological expert completing first
5. Supply chain forecasting depends on geological + logistics completing
6. Maximum 11 steps (one per DSW type)
7. Make queries SPECIFIC — include material names, regions, purity grades
8. Set higher priority (lower number) for critical-path steps
"""


def format_planner_prompt(
    raw_query: str,
    gia_request: Any,
    available_dsw: List[str],
) -> str:
    """
    Format the user-facing portion of the planner prompt.

    Args:
        raw_query:     Original user query
        gia_request:   GIARequest with extracted context
        available_dsw: List of DSW worker type values

    Returns:
        Formatted prompt string for the LLM HumanMessage
    """
    materials_str = ", ".join(gia_request.materials) if gia_request.materials else "unspecified"

    return f"""
## User Query
"{raw_query}"

## Extracted Context
- Region: {gia_request.region_context}
- Materials of interest: {materials_str}
- Priority level: {gia_request.priority}
- Request ID: {gia_request.request_id}

## Task
Decompose this query into a precise execution plan. Identify which DSWs must be invoked,
what specific sub-query each should execute, which MCP tools they need, and which steps
have ordering dependencies. Prioritize steps on the critical path.

Return your response as a valid JSON object following the output format above.
""".strip()


# ============================================================================
# SYNTHESIS PROMPT
# ============================================================================

SYNTHESIS_SYSTEM_PROMPT = """
You are the Central Orchestration Graph (COG) Synthesis Engine for India's GPU Manufacturing Supply Chain Intelligence System.

Your role is to aggregate the results from multiple Domain Specialist Workers (DSWs) into a coherent, actionable viability blueprint for the user.

## Your Synthesis Responsibilities

1. **Material Profiles**: For each material queried, synthesize availability, quality, quantity, and sourcing risk
2. **Supply Chain Map**: Create a structured map of the complete supply chain from raw material → processing → fab
3. **Viability Score**: Rate overall sourcing viability on a 0.0–10.0 scale (10 = fully viable)
4. **Risk Assessment**: Identify geological, logistical, regulatory, workforce, and geopolitical risks
5. **Recommendations**: Provide 5-10 concrete, actionable recommendations
6. **Data Gaps**: List areas where DSW data was missing or partial (for transparency)

## Output Format (STRICT JSON)

{
  "title": "<descriptive report title>",
  "executive_summary": "<2-3 paragraph executive summary for decision makers>",
  "material_profiles": [
    {
      "material": "<name>",
      "availability_status": "<Abundant|Moderate|Scarce|Unknown>",
      "primary_sources": ["<location1>", "<location2>"],
      "estimated_reserves": "<quantity with units>",
      "processing_capability": "<local|requires_import|partial>",
      "quality_grade": "<semiconductor_grade|industrial_grade|subgrade>",
      "purity_achievable": "<percentage>",
      "key_suppliers": ["<supplier1>"],
      "sourcing_risk_score": <0.0-10.0>,
      "notes": "<additional observations>"
    }
  ],
  "supply_chain_map": {
    "raw_material_extraction": {},
    "chemical_processing": {},
    "logistics_network": {},
    "manufacturing_facilities": {},
    "quality_certification": {}
  },
  "viability_score": <0.0-10.0>,
  "risk_assessment": {
    "geological": "<assessment>",
    "logistical": "<assessment>",
    "regulatory": "<assessment>",
    "workforce": "<assessment>",
    "geopolitical": "<assessment>",
    "overall_risk_level": "<Low|Medium|High|Critical>"
  },
  "recommendations": ["<recommendation1>", ...],
  "data_gaps": ["<gap1>", ...],
  "investment_horizon": "<short|medium|long>_term",
  "next_steps": ["<action1>", ...]
}

## Critical Instructions
- If data is degraded/partial (degraded_mode=true), lower viability_score accordingly and mention in summary
- Cite which DSW workers provided data for each finding
- Be specific about regions, quantities, and grades — no vague statements
- If a worker FAILED, list its domain in data_gaps
- Viability ≥7.0 means viable, 4.0–7.0 means conditionally viable, <4.0 means not viable
"""


def format_synthesis_prompt(synthesis_context: Dict[str, Any]) -> str:
    """
    Format the synthesis prompt with DSW result data.

    Args:
        synthesis_context: Dict with completed_data, partial_data,
                           failed_workers, tha_applied_tickets, etc.

    Returns:
        Formatted prompt string for synthesis LLM call
    """
    completed_json = json.dumps(synthesis_context.get("completed_data", []), indent=2)
    partial_json   = json.dumps(synthesis_context.get("partial_data", []), indent=2)
    failed_json    = json.dumps(synthesis_context.get("failed_workers", []), indent=2)

    return f"""
## Original User Query
"{synthesis_context.get('original_query', '')}"

## Target Region
{synthesis_context.get('region', 'India')}

## Materials of Interest
{', '.join(synthesis_context.get('materials', []))}

## Completed DSW Results (Full Data Available)
{completed_json}

## Partial DSW Results (Degraded/Fallback Data)
{partial_json}

## Failed DSW Workers (No Data Available)
{failed_json}

## System Status
- Degraded Mode: {synthesis_context.get('degraded_mode', False)}
- THA Healing Tickets Applied: {synthesis_context.get('tha_applied_tickets', [])}

## Task
Synthesize ALL the above data into a comprehensive supply chain viability blueprint.
Return ONLY the JSON object as specified in your instructions.
""".strip()
