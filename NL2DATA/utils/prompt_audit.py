"""Utility script to audit and fix system prompts in step functions.

This script helps ensure all step functions:
1. Have explicit OUTPUT STRUCTURE sections in system prompts
2. Use Pydantic objects (not raw JSON) for output handling
"""

import re
import ast
from pathlib import Path
from typing import List, Dict, Any, Tuple

def find_llm_calls(root_dir: Path) -> List[Tuple[Path, Dict[str, Any]]]:
    """Find all files with standardized_llm_call and extract relevant info."""
    results = []
    
    for py_file in root_dir.rglob("step_*.py"):
        try:
            content = py_file.read_text(encoding="utf-8")
            
            # Check if file uses standardized_llm_call
            if "standardized_llm_call" not in content:
                continue
            
            # Extract output schema class name
            output_schema_match = re.search(r'output_schema=(\w+)', content)
            output_schema = output_schema_match.group(1) if output_schema_match else None
            
            # Check if system prompt mentions output structure
            has_output_structure = bool(re.search(
                r'OUTPUT STRUCTURE|output structure|OUTPUT FORMAT|Return.*Pydantic|Return.*JSON.*structure',
                content,
                re.IGNORECASE
            ))
            
            # Check for raw JSON handling
            has_raw_json = bool(re.search(
                r'\.json\(\)|json\.loads|json\.dumps|result\.get\(|result\[|if.*in result|for.*in result\.items',
                content
            ))
            
            # Check if using Pydantic properly
            uses_pydantic = bool(re.search(
                r'\.model_dump\(\)|\.model_copy\(|result\.\w+\.|result:\s*\w+Output\s*=',
                content
            ))
            
            results.append({
                "file": py_file,
                "output_schema": output_schema,
                "has_output_structure": has_output_structure,
                "has_raw_json": has_raw_json,
                "uses_pydantic": uses_pydantic,
            })
        except Exception as e:
            print(f"Error processing {py_file}: {e}")
    
    return results


if __name__ == "__main__":
    phases_dir = Path(__file__).parent.parent / "phases"
    results = find_llm_calls(phases_dir)
    
    print(f"Found {len(results)} files with LLM calls")
    print("\nFiles missing OUTPUT STRUCTURE in system prompt:")
    for r in results:
        if not r["has_output_structure"]:
            print(f"  - {r['file']} (schema: {r['output_schema']})")
    
    print("\nFiles with potential raw JSON handling:")
    for r in results:
        if r["has_raw_json"] and not r["uses_pydantic"]:
            print(f"  - {r['file']}")
