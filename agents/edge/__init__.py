"""
agents/edge/__init__.py
========================
Edge agents: GIA (Gateway Interface Agent) and THA (Telemetry & Healing Agent).
"""

from .gia import GatewayInterfaceAgent
from .tha import TelemetryHealingAgent

__all__ = ["GatewayInterfaceAgent", "TelemetryHealingAgent"]
