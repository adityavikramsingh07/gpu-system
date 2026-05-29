"""Trade Policy Expert MCP Tools"""
async def query_pli_schemes(region: str, material: str, query: str) -> str:
    """Check eligibility for PLI schemes."""
    return "HPQ falls under PLI 2.0 component scheme"

async def get_import_tariffs(region: str, material: str, query: str) -> str:
    """Get the current import duties and tariffs."""
    return "Copper foil import duty at 5%"
