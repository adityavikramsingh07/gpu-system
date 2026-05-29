from mcp.server.fastmcp import FastMCP
import asyncio

# Initialize FastMCP Server
mcp = FastMCP("GPU Supply Chain MAS Gateway")

# Import all modular tools
from tools.geological import query_mining_deposits, get_lease_status, get_geological_survey, get_mineral_grade_report
from tools.logistics import get_port_data, query_transport_routes, get_warehouse_capacity, get_customs_clearance_time
from tools.chemical import map_processing_plants, get_chemical_precursor_supply
from tools.forecast import run_monte_carlo_supply_sim, get_demand_projection
from tools.fab import query_industrial_parks, get_water_power_metrics
from tools.legal import check_legal_disputes, query_land_registry
from tools.env import check_green_clearances, get_pollution_index
from tools.workforce import query_university_graduates, get_labor_costs
from tools.trade import query_pli_schemes, get_import_tariffs
from tools.thermal import query_tim_suppliers, get_substrate_pricing
from tools.qa import verify_semi_f49_compliance, get_iso_certifications

# --- Register Geological Tools ---
mcp.add_tool(query_mining_deposits)
mcp.add_tool(get_lease_status)
mcp.add_tool(get_geological_survey)
mcp.add_tool(get_mineral_grade_report)

# --- Register Logistics Tools ---
mcp.add_tool(get_port_data)
mcp.add_tool(query_transport_routes)
mcp.add_tool(get_warehouse_capacity)
mcp.add_tool(get_customs_clearance_time)

# --- Register Chemical Infra Tools ---
mcp.add_tool(map_processing_plants)
mcp.add_tool(get_chemical_precursor_supply)

# --- Register Supply Chain Forecaster Tools ---
mcp.add_tool(run_monte_carlo_supply_sim)
mcp.add_tool(get_demand_projection)

# --- Register Fab Locator Tools ---
mcp.add_tool(query_industrial_parks)
mcp.add_tool(get_water_power_metrics)

# --- Register Mining Lease Analyst Tools ---
mcp.add_tool(check_legal_disputes)
mcp.add_tool(query_land_registry)

# --- Register Environmental Compliance Tools ---
mcp.add_tool(check_green_clearances)
mcp.add_tool(get_pollution_index)

# --- Register Workforce Analyst Tools ---
mcp.add_tool(query_university_graduates)
mcp.add_tool(get_labor_costs)

# --- Register Trade Policy Tools ---
mcp.add_tool(query_pli_schemes)
mcp.add_tool(get_import_tariffs)

# --- Register Thermal Materials Tools ---
mcp.add_tool(query_tim_suppliers)
mcp.add_tool(get_substrate_pricing)

# --- Register QA Tools ---
mcp.add_tool(verify_semi_f49_compliance)
mcp.add_tool(get_iso_certifications)

if __name__ == "__main__":
    import sys
    # To run SSE server, we use mcp.run()
    mcp.run(transport="sse")
