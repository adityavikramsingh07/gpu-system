"""
agents/specialists/geological_expert.py
=========================================
Geological Expert DSW

Domain: Raw material mining deposits, HPQ seam surveys, rare earth reserves,
        geological formations suitable for semiconductor-grade mineral extraction.

MCP Server: geological-mcp-server
MCP Tools:
  - query_mining_deposits(region, material, grade_filter)
  - get_lease_status(deposit_id)
  - get_geological_survey(survey_area, depth_range)
  - get_mineral_grade_report(deposit_id, material)

System prompt context: Mining geology, mineral purity grading,
  Karnataka Geological Survey, Geological Survey of India (GSI) data,
  Indian Bureau of Mines (IBM) deposit registry.
"""

from __future__ import annotations

import json
import hashlib
from typing import Any, Dict, List, Optional, Tuple

from orchestration.cog.state_schema import DSWWorkerType, MCPToolCall
from core_services.svb import EphemeralToken, MCPScope
from agents.specialists.base_dsw import BaseDSW, DSW_BASE_SYSTEM_PROMPT


class GeologicalExpertDSW(BaseDSW):
    """
    Geological Expert DSW — India's mineral deposit mapping specialist.

    Queries:
      1. Primary HPQ (High-Purity Quartz) deposits in Southern India
      2. Silicon Carbide (SiC) precursor mineral availability
      3. Rare earth element occurrences (Gallium, Germanium, Indium)
      4. Active mining lease status for identified deposits
      5. Semiconductor-grade mineral certification requirements
    """

    worker_type    = DSWWorkerType.GEOLOGICAL_EXPERT
    mcp_server_id  = "geological-mcp-server"
    required_scopes = [
        MCPScope.READ_MINING_DEPOSITS.value,
        MCPScope.READ_LEASE_STATUS.value,
        MCPScope.READ_GEO_SURVEY.value,
    ]
    available_tools = [
        "query_mining_deposits",
        "get_lease_status",
        "get_geological_survey",
        "get_mineral_grade_report",
    ]

    SYSTEM_PROMPT = DSW_BASE_SYSTEM_PROMPT.format(
        worker_role   = "Senior Geological Expert specializing in India's mineral resources",
        mission       = "Map HPQ and critical mineral deposits in Southern India relevant to GPU semiconductor manufacturing. Determine availability, purity grades, reserve quantities, and lease status.",
        region_focus  = "{region_focus}",
        material_focus = "{material_focus}",
        mcp_server_id = "geological-mcp-server",
        available_tools = "query_mining_deposits, get_lease_status, get_geological_survey, get_mineral_grade_report",
    )

    def execute_core(
        self,
        token:          EphemeralToken,
        query:          str,
        region_focus:   str,
        material_focus: str,
        span:           Any,
    ) -> Tuple[Dict[str, Any], List[MCPToolCall], float, List[str]]:
        """
        Geological data retrieval workflow:
          1. Query mineral deposits for target material + region
          2. Filter by semiconductor grade (SiO2 purity ≥99.998% for HPQ)
          3. For each deposit found, fetch lease status
          4. Get geological survey data for reserve estimation
          5. Compute confidence based on data completeness
        """
        mcp_calls: List[MCPToolCall] = []
        sources:   List[str]         = []

        # ── Tool 1: Query mineral deposits ────────────────────────────────────
        span.add_event(f"Querying mining deposits: {material_focus} in {region_focus}")
        deposits_result, mcp_call_1 = self.call_mcp_tool(
            tool_name  = "query_mining_deposits",
            params     = {
                "region":       region_focus,
                "material":     material_focus,
                "grade_filter": "semiconductor_grade",
                "status":       "active",
                "max_results":  20,
            },
            token = token,
            span  = span,
        )
        mcp_calls.append(mcp_call_1)
        span.set_attribute("mcp_tool_invoked", "query_mining_deposits")

        deposits   = deposits_result.get("deposits", [])
        sources.append(f"query_mining_deposits({material_focus}, {region_focus})")

        if not deposits:
            return (
                {
                    "summary": f"No semiconductor-grade {material_focus} deposits found in {region_focus}.",
                    "deposits": [],
                    "reserve_estimate_tonnes": 0,
                    "active_leases": 0,
                },
                mcp_calls, 0.1, sources
            )

        # ── Tool 2: Get lease status for top 5 deposits ──────────────────────
        lease_results = []
        for deposit in deposits[:5]:
            deposit_id = deposit.get("deposit_id", "")
            if not deposit_id:
                continue

            span.add_event(f"Checking lease for deposit: {deposit_id}")
            lease_data, mcp_call_2 = self.call_mcp_tool(
                tool_name = "get_lease_status",
                params    = {"deposit_id": deposit_id},
                token     = token,
                span      = span,
            )
            mcp_calls.append(mcp_call_2)
            lease_results.append({
                "deposit_id":   deposit_id,
                "location":     deposit.get("location", ""),
                "reserve_t":    deposit.get("estimated_reserve_tonnes", 0),
                "purity_sio2":  deposit.get("purity_sio2_percent", 0),
                "lease_active": lease_data.get("active", False),
                "lease_holder": lease_data.get("lease_holder", "Unknown"),
                "lease_expiry": lease_data.get("expiry_date", ""),
            })
            sources.append(f"get_lease_status({deposit_id})")

        # ── Tool 3: Get geological survey for reserve estimation ──────────────
        span.add_event("Fetching geological survey data")
        survey_result, mcp_call_3 = self.call_mcp_tool(
            tool_name = "get_geological_survey",
            params    = {
                "survey_area":  region_focus,
                "material":     material_focus,
                "depth_range":  "0-500m",
            },
            token = token,
            span  = span,
        )
        mcp_calls.append(mcp_call_3)
        span.set_attribute("mcp_tool_invoked", "get_geological_survey")
        sources.append(f"get_geological_survey({region_focus})")

        # ── Compute confidence ────────────────────────────────────────────────
        total_reserve = sum(d.get("reserve_t", 0) for d in lease_results)
        active_leases = sum(1 for d in lease_results if d.get("lease_active", False))
        confidence = min(1.0, (len(deposits) / 10) * 0.7 + (active_leases / max(len(deposits), 1)) * 0.3)

        span.set_attribute("deposits_found",    len(deposits))
        span.set_attribute("active_leases",     active_leases)
        span.set_attribute("total_reserve_t",   total_reserve)
        span.set_attribute("confidence",        round(confidence, 3))

        result_data = {
            "summary": (
                f"Found {len(deposits)} {material_focus} deposits in {region_focus}. "
                f"{active_leases} have active mining leases. "
                f"Total estimated reserves: {total_reserve:,.0f} tonnes. "
                f"Semiconductor-grade (SiO2 ≥99.998%) deposits confirmed at {active_leases} sites."
            ),
            "deposits_found":          len(deposits),
            "semiconductor_grade_deposits": [d for d in lease_results if d.get("purity_sio2", 0) >= 99.998],
            "all_deposits_sampled":    lease_results,
            "total_reserve_tonnes":    total_reserve,
            "active_leased_deposits":  active_leases,
            "geological_survey":       survey_result,
            "region":                  region_focus,
            "material":                material_focus,
        }

        return result_data, mcp_calls, confidence, sources
