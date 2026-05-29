"""
BaseAgent: Abstract interface for all specialist agents

Defines the contract for agent execution, credential injection,
OpenTelemetry tracing, and result formatting.

All specialist agents must inherit from BaseAgent.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional

from utils.logger import trace_agent_operation

class BaseAgent(ABC):
    """
    Abstract base class for all specialist agents.
    """
    agent_id: str
    agent_type: str

    def __init__(self, agent_id: str):
        self.agent_id = agent_id
        self.agent_type = self.__class__.__name__

    @abstractmethod
    @trace_agent_operation("execute_specialist_task", include_result=True, include_args=True)
    def execute(
        self,
        task: Dict[str, Any],
        credential: Dict[str, Any],
        trace_id: Optional[str] = None,
        job_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Execute the assigned specialist task.
        
        Args:
            task: Task dictionary (from EJMS)
            credential: TemporaryCredential dict (from ACS)
            trace_id: Trace ID for observability
            job_id: Job ID for tracking
        Returns:
            result: Dictionary with result data
        """
        pass

    def format_result(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Format result for MRA aggregation.
        Override if agent needs custom formatting.
        """
        return {
            "agent_id": self.agent_id,
            "agent_type": self.agent_type,
            "result": result
        }
