"""Script to update test files to handle Pydantic return types.

This script finds all test files that use .get() on result and adds
conversion to dict for Pydantic models.
"""

import re
from pathlib import Path
from typing import List, Tuple

def update_test_file(file_path: Path) -> Tuple[bool, List[str]]:
    """Update a test file to handle Pydantic models.
    
    Returns:
        (changed, errors) tuple
    """
    try:
        content = file_path.read_text(encoding='utf-8')
        original_content = content
        errors = []
        
        # Pattern to find result.get() calls that need conversion
        # Look for patterns like: result.get(...) or result_dict.get(...)
        # We want to add conversion before first result.get() usage
        
        # Check if file already has conversion pattern
        if 'result.model_dump()' in content or 'result_dict = result.model_dump()' in content:
            return False, []  # Already updated
        
        # Find first occurrence of result.get( or result[" or result[
        # But only if it's not already in a conversion block
        pattern = r'(result\s*=\s*await\s+[^\n]+)\n(\s+TestResultDisplay\.print_output_summary\(result\))'
        match = re.search(pattern, content, re.MULTILINE)
        
        if match:
            # Insert conversion after print_output_summary
            insertion_point = match.end()
            indent = "    "  # Standard indent
            conversion_code = f'\n{indent}# Convert to dict for validation\n{indent}result_dict = result.model_dump() if hasattr(result, \'model_dump\') else result\n'
            
            # Find the next non-empty line to determine where to insert
            lines = content[:insertion_point].split('\n')
            content = content[:insertion_point] + conversion_code + content[insertion_point:]
            
            # Replace all result.get( with result_dict.get(
            # But be careful not to replace result_dict.get( again
            content = re.sub(r'\bresult\.get\(', 'result_dict.get(', content)
            content = re.sub(r'\bresult\[', 'result_dict[', content)
            
            if content != original_content:
                file_path.write_text(content, encoding='utf-8')
                return True, []
        
        return False, errors
    except Exception as e:
        return False, [str(e)]

def main():
    """Update all test files in phases 1-4."""
    base_dir = Path(__file__).parent.parent
    
    test_dirs = [
        base_dir / "tests" / "phase1",
        base_dir / "tests" / "phase2", 
        base_dir / "tests" / "phase3",
        base_dir / "tests" / "phase4",
    ]
    
    updated_files = []
    errors = []
    
    for test_dir in test_dirs:
        if not test_dir.exists():
            continue
        
        for test_file in test_dir.glob("test_*.py"):
            if test_file.name.startswith("test_step_"):
                changed, file_errors = update_test_file(test_file)
                if changed:
                    updated_files.append(test_file)
                if file_errors:
                    errors.extend([f"{test_file}: {e}" for e in file_errors])
    
    print(f"Updated {len(updated_files)} test files")
    if errors:
        print(f"Errors: {errors}")
    if updated_files:
        print("Updated files:")
        for f in updated_files:
            print(f"  - {f}")

if __name__ == "__main__":
    main()
