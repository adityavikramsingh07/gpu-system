"""
mcp_servers/gateway.py
======================
Unified FastMCP Data Gateway

Serves simulated MCP tools for all 11 DSWs.
Handles dynamic routing based on the Server ID.
"""

from fastapi import FastAPI, Header, HTTPException, Request
from pydantic import BaseModel
import uvicorn
import asyncio
import time

app = FastAPI(title="Unified MCP Data Gateway")

# Simulated Data Store
DB = {
    "query_mining_deposits": {"data": "Found 23 deposits in TN and KA, 7 semiconductor-grade HPQ"},
    "get_lease_status": {"data": "Active leases found for Gudur, Nellore"},
    "get_geological_survey": {"data": "2.1M tonnes estimated reserves in Southern India"},
    "get_mineral_grade_report": {"data": "SiO2 purity >=99.998% compliant with SEMI F49"},
    
    "get_port_data": {"data": "Port data retrieved (Simulated fallback handling not triggered if success)"},
    "query_transport_routes": {"data": "6 viable routes via road/rail identified"},
    "get_warehouse_capacity": {"data": "12 climate-controlled facilities available"},
    "get_customs_clearance_time": {"data": "Average 2.4 days for imported precursors"},

    # Other tools...
    "map_processing_plants": {"data": "2 SiO2 plants in Karnataka"},
    "get_chemical_precursor_supply": {"data": "Adequate precursor supply projected"},
    "run_monte_carlo_supply_sim": {"data": "95% probability of supply constraint within 12 months"},
    "get_demand_projection": {"data": "Demand increasing 3x by 2027"},
    "query_industrial_parks": {"data": "SIPCOT Hosur has suitable plots"},
    "get_water_power_metrics": {"data": "Power grid resilient; water supply needs desalinization backup"},
    "check_legal_disputes": {"data": "No major disputes blocking land acquisition"},
    "query_land_registry": {"data": "Land titles verified in primary zones"},
    "check_green_clearances": {"data": "2 clearances issued, 1 pending review"},
    "get_pollution_index": {"data": "Index below threshold; compliant"},
    "query_university_graduates": {"data": "3800 engineering graduates annually in region"},
    "get_labor_costs": {"data": "Competitive labor costs for specialized roles"},
    "query_pli_schemes": {"data": "HPQ falls under PLI 2.0 component scheme"},
    "get_import_tariffs": {"data": "Copper foil import duty at 5%"},
    "query_tim_suppliers": {"data": "Sourcing primarily from Taiwan/Japan"},
    "get_substrate_pricing": {"data": "Substrate pricing volatile, average $15/sqm"},
    "verify_semi_f49_compliance": {"data": "Compliance verified for HPQ samples"},
    "get_iso_certifications": {"data": "All top vendors are ISO 9001 certified"},
}

@app.post("/tools/{tool_name}")
async def invoke_tool(tool_name: str, request: Request, authorization: str = Header(None)):
    if not authorization:
        raise HTTPException(status_code=401, detail="Missing Authorization token")
    
    # Task 3 Scenario: simulate timeout for logistics get_port_data
    if tool_name == "get_port_data":
        # Simulate 30s timeout by actually sleeping
        await asyncio.sleep(31)

    if tool_name not in DB:
        return {"data": f"Simulated execution of {tool_name} successful.", "status": "ok"}
    
    return {"data": DB[tool_name]["data"], "status": "ok"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)
