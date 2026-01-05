"""Regex-based string generation strategy with validation, sanitization, and deduplication."""

from typing import List, Dict, Optional
from pydantic import Field, field_validator
import re
import hashlib

try:
    from rstr import xeger
    RSTR_AVAILABLE = True
except ImportError:
    RSTR_AVAILABLE = False

from NL2DATA.phases.phase9.strategies.base import BaseGenerationStrategy


class RegexStrategy(BaseGenerationStrategy):
    """Regex pattern-based string generation strategy."""
    
    name: str = Field(default="regex", frozen=True)
    kind: str = Field(default="string", frozen=True)
    description: str = Field(
        default="Generate strings matching a regex pattern using rstr.xeger. Use for string attributes with specific pattern requirements (e.g., SKU codes, license plates, IDs with format constraints).",
        frozen=True
    )
    
    pattern: str = Field(description="Regex pattern used for generation")
    bounds: Dict[str, int] = Field(
        default_factory=lambda: {"star": 16, "plus": 16, "dot": 16},
        description="Explosion control: max repetitions for *, +, .*, .+ (prevents infinite loops)"
    )
    unique: bool = Field(default=True, description="If true, deduplicate generated values using hash-based deduplication")
    max_attempts_per_value: Optional[int] = Field(default=None, description="If set, fail fast when regex language seems exhausted")
    
    @field_validator("pattern")
    @classmethod
    def validate_pattern(cls, v: str) -> str:
        """Validate regex pattern for generatable features."""
        if not RSTR_AVAILABLE:
            raise ValueError("rstr library is not available. Install with: pip install rstr")
        
        # Check for non-generatable features
        forbidden_patterns = [
            (r"\(\?[=!<]", "lookahead/lookbehind"),
            (r"\\[1-9]", "backreferences"),
            (r"\(\?\(", "conditional groups"),
            (r"\(\?>", "atomic groups"),
        ]
        
        for pat, desc in forbidden_patterns:
            if re.search(pat, v):
                raise ValueError(f"Regex contains non-generatable feature: {desc}")
        
        # Try to compile the regex to ensure it's valid
        try:
            re.compile(v)
        except re.error as e:
            raise ValueError(f"Invalid regex pattern: {e}")
        
        return v
    
    def _sanitize_pattern(self) -> str:
        """Sanitize regex pattern with explosion control."""
        sanitized = self.pattern
        # Replace unbounded quantifiers with bounded ones
        sanitized = re.sub(r"\*", f"{{0,{self.bounds['star']}}}", sanitized)
        sanitized = re.sub(r"\+", f"{{1,{self.bounds['plus']}}}", sanitized)
        sanitized = re.sub(r"\.\+", f".{{1,{self.bounds['dot']}}}", sanitized)
        sanitized = re.sub(r"\.\*", f".{{0,{self.bounds['dot']}}}", sanitized)
        return sanitized
    
    def generate(self, size: int) -> List[str]:
        """Generate regex-matching strings with validation, sanitization, and deduplication."""
        if not RSTR_AVAILABLE:
            raise RuntimeError("rstr library is not available. Install with: pip install rstr")
        
        # Sanitize pattern
        sanitized_pattern = self._sanitize_pattern()
        
        # Generate with deduplication
        seen_hashes = set()
        output = []
        consecutive_failures = 0
        
        while len(output) < size:
            try:
                # xeger() is a function, not a method
                s = xeger(sanitized_pattern)
                h = hashlib.blake2b(s.encode('utf-8'), digest_size=8).hexdigest()
                
                if not self.unique or h not in seen_hashes:
                    seen_hashes.add(h)
                    output.append(s)
                    consecutive_failures = 0
                else:
                    consecutive_failures += 1
                    if self.max_attempts_per_value and consecutive_failures >= self.max_attempts_per_value:
                        raise RuntimeError(
                            f"Regex space exhausted or too restrictive after {consecutive_failures} attempts. "
                            f"Generated {len(output)}/{size} unique values."
                        )
            except Exception as e:
                raise RuntimeError(f"Regex generation failed: {e}")
        
        return output

