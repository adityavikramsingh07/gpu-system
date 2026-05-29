"""
agents/specialists/semiconductor_grade_qa.py
======================================
Quality Assurance & Yield Specialist DSW Agent utilizing the Parallel StateGraph architecture.
"""

from typing import List
from orchestration.cog.state_schema import DSWWorkerType
from agents.specialists.base_dsw import BaseDSW

class SemiconductorGradeQADSW(BaseDSW):
    worker_type = DSWWorkerType.SEMICONDUCTOR_GRADE_QA
    mcp_server_id = "semiconductor-mcp-server"
    required_scopes = ["read", "execute"]

    def get_system_prompt(self, region_focus: str, material_focus: str) -> str:
        return (
            "You are the Quality Assurance & Yield Specialist in a Multi-Agent System building a secure GPU supply chain.\n"
            "Your specific expertise is: Verifies material compliances like SEMI F49 and ISO standards.\n"
            f"Region: {region_focus}\n"
            f"Material: {material_focus}\n\n"
            "Analyze the data provided by your parallel tool execution accurately and concisely."
        )

    def get_required_tools(self) -> List[str]:
        """Return the exact MCP tool names to be executed in parallel during the Fan-Out phase."""
        return ['verify_semi_f49_compliance', 'get_iso_certifications']
