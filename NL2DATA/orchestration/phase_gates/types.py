"""Type definitions for phase gates."""

from dataclasses import dataclass
from typing import List, Dict, Any, Optional


@dataclass
class GateResult:
    """Result of a phase gate check."""
    passed: bool
    issues: List[str]
    warnings: List[str]
    metadata: Dict[str, Any]


class PhaseGateError(Exception):
    """Exception raised when a phase gate fails."""
    
    def __init__(self, phase: int, result: GateResult, message: str = None):
        self.phase = phase
        self.result = result
        self.message = message or f"Phase {phase} gate failed"
        super().__init__(self.message)

