"""Base generation strategy with Pydantic model and generate method."""

from abc import ABC, abstractmethod
from typing import List, Any, Dict, Optional
from pydantic import BaseModel, Field, ConfigDict


class BaseGenerationStrategy(BaseModel, ABC):
    """Base class for all generation strategies with embedded generate method."""
    
    name: str = Field(description="Strategy name (stable identifier)")
    kind: str = Field(description="Strategy kind: 'distribution', 'string', 'location', 'datetime'")
    description: str = Field(description="Human-readable description of what this strategy does")
    
    model_config = ConfigDict(extra="forbid")
    
    @abstractmethod
    def generate(self, size: int) -> List[Any]:
        """
        Generate a list of values using this strategy.
        
        Args:
            size: Number of values to generate
            
        Returns:
            List of generated values (type depends on strategy)
        """
        pass
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert strategy to dictionary representation."""
        return self.model_dump()
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "BaseGenerationStrategy":
        """Create strategy instance from dictionary."""
        return cls(**data)

