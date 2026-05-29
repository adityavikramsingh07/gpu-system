"""
agents/specialists/logistics_coordinator.py
=============================================
Logistics Coordinator DSW

Domain: Transport infrastructure, port capacity, warehouse logistics,
        cold chain for chemical precursors, cross-state trade flow analysis.

MCP Server: logistics-mcp-server (primary)
            trade-api-fallback  (THA-injected alternate on timeout)

MCP Tools:
  - get_port_data(port_name, material, throughput_type)
  - query_transport_routes(origin, destination, material, mode)
  - get_warehouse_capacity(region, material_class)
  - get_customs_clearance_time(port_name, material)

This DSW is the one involved in the Copper MCP timeout scenario in Task 3.
The THA intercepts its MCP timeout, injects a USE_ALTERNATE_TOOL directive,
and it retries using query_trade_routes instead of get_port_data.
"""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

from orchestration.cog.state_schema import DSWWorkerType, MCPToolCall
from core_services.svb import EphemeralToken, MCPScope
from agents.specialists.base_dsw import BaseDSW, DSW_BASE_SYSTEM_PROMPT


class LogisticsCoordinatorDSW(BaseDSW):
    """
    Logistics Coordinator DSW — supply chain transport and port logistics.

    Primary use case in Task 3:
      - Find Copper foil import/transport routes to South Indian fabs
      - Query Kattupalli/Krishnapatnam port data for copper imports
      - Assess warehouse capacity in Tamil Nadu for copper coil storage
    """

    worker_type    = DSWWorkerType.LOGISTICS_COORDINATOR
    mcp_server_id  = "logistics-mcp-server"
    required_scopes = [
        MCPScope.READ_PORT_DATA.value,
        MCPScope.READ_TRANSPORT_ROUTES.value,
        MCPScope.READ_WAREHOUSE_CAP.value,
    ]
    available_tools = [
        "get_port_data",
        "query_transport_routes",
        "get_warehouse_capacity",
        "get_customs_clearance_time",
    ]

    def execute_core(
        self,
        token:          EphemeralToken,
        query:          str,
        region_focus:   str,
        material_focus: str,
        span:           Any,
    ) -> Tuple[Dict[str, Any], List[MCPToolCall], float, List[str]]:
        """
        Logistics data retrieval:
          1. Get port data for major South Indian ports
          2. Query transport routes for material delivery
          3. Check warehouse capacity in target region
          4. Assess customs clearance timelines

        NOTE: Tool 1 (get_port_data) simulates a timeout in Task 3 Execution
        Specification. THA intercepts and redirects to query_transport_routes
        as fallback. The fallback=True flag is set in that case.
        """
        mcp_calls: List[MCPToolCall] = []
        sources:   List[str]        = []
        fallback_used = False

        # ── Tool 1: Get port data (primary — may timeout for Copper MCP) ──────
        span.add_event(f"Querying port data for {material_focus} at {region_focus} ports")
        try:
            port_result, mcp_call_1 = self.call_mcp_tool(
                tool_name = "get_port_data",
                params    = {
                    "region":           region_focus,
                    "material":         material_focus,
                    "throughput_type":  "import",
                    "ports":            ["Kattupalli", "Krishnapatnam", "Chennai", "Kochi"],
                },
                token = token,
                span  = span,
            )
            mcp_calls.append(mcp_call_1)
            sources.append("get_port_data(Kattupalli,Krishnapatnam,Chennai,Kochi)")
            port_data = port_result.get("ports", [])

        except TimeoutError:
            # ── FALLBACK: Use query_transport_routes instead ──────────────────
            # (This is what THA's USE_ALTERNATE_TOOL injection triggers)
            span.add_event("get_port_data TIMED OUT — using query_transport_routes fallback")
            span.set_attribute("fallback_triggered", True)
            fallback_used = True

            route_result, mcp_call_fallback = self.call_mcp_tool(
                tool_name = "query_transport_routes",
                params    = {
                    "origin":      "Chennai Port",
                    "destination": region_focus,
                    "material":    material_focus,
                    "mode":        "road",
                },
                token = token,
                span  = span,
            )
            mcp_call_fallback.tool_name = "query_transport_routes [FALLBACK for get_port_data]"
            mcp_calls.append(mcp_call_fallback)
            sources.append("query_transport_routes(fallback)")
            port_data = []     # Less data from fallback, acknowledged in confidence

        # ── Tool 2: Query transport routes ───────────────────────────────────
        span.add_event(f"Querying transport routes for {material_focus}")
        routes_result, mcp_call_2 = self.call_mcp_tool(
            tool_name = "query_transport_routes",
            params    = {
                "origin":       "Chennai Port / Krishnapatnam",
                "destination":  region_focus,
                "material":     material_focus,
                "mode":         "multimodal",
                "include_rail": True,
            },
            token = token,
            span  = span,
        )
        mcp_calls.append(mcp_call_2)
        sources.append(f"query_transport_routes({material_focus})")

        # ── Tool 3: Warehouse capacity ───────────────────────────────────────
        span.add_event("Checking warehouse capacity")
        warehouse_result, mcp_call_3 = self.call_mcp_tool(
            tool_name = "get_warehouse_capacity",
            params    = {
                "region":         region_focus,
                "material_class": "non_ferrous_metals",
                "climate_control": True,    # Copper foils need humidity control
            },
            token = token,
            span  = span,
        )
        mcp_calls.append(mcp_call_3)
        sources.append(f"get_warehouse_capacity({region_focus})")

        # ── Compute confidence ────────────────────────────────────────────────
        confidence = 0.85 if not fallback_used else 0.55   # Penalized for fallback

        span.set_attribute("fallback_used", fallback_used)
        span.set_attribute("confidence",    round(confidence, 3))

        routes = routes_result.get("routes", [])
        warehouses = warehouse_result.get("facilities", [])

        result_data = {
            "summary": (
                f"Logistics assessment for {material_focus} in {region_focus}: "
                f"Found {len(routes)} viable transport routes. "
                f"{len(warehouses)} climate-controlled warehouse facilities available. "
                + ("Port data obtained via fallback route query." if fallback_used else
                   f"Port capacity data available from {len(port_data)} ports.")
            ),
            "port_data":             port_data,
            "transport_routes":      routes,
            "warehouse_facilities":  warehouses,
            "fallback_used":         fallback_used,
            "region":                region_focus,
            "material":              material_focus,
            "primary_entry_ports":   ["Kattupalli", "Krishnapatnam"] if not fallback_used else [],
        }

        return result_data, mcp_calls, confidence, sources
