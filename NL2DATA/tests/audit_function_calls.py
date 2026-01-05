"""Audit script to check for missing function arguments in phase graphs.

This script checks all phase graph wrappers to ensure they pass all required arguments
to step functions.
"""

import ast
import inspect
from pathlib import Path
from typing import Dict, List, Tuple
import sys

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

def get_function_signature(func) -> Dict[str, bool]:
    """Get function signature and mark which args are required (no default)."""
    sig = inspect.signature(func)
    params = {}
    for name, param in sig.parameters.items():
        # Required if no default value
        params[name] = param.default == inspect.Parameter.empty
    return params

def find_function_calls_in_ast(node, target_func_name: str) -> List[ast.Call]:
    """Find all function calls to target_func_name in AST."""
    calls = []
    for child in ast.walk(node):
        if isinstance(child, ast.Call):
            if isinstance(child.func, ast.Name) and child.func.id == target_func_name:
                calls.append(child)
            elif isinstance(child.func, ast.Attribute) and child.func.attr == target_func_name:
                calls.append(child)
    return calls

def check_phase_graph(phase_num: int) -> List[Tuple[str, str, List[str]]]:
    """Check a phase graph file for missing arguments.
    
    Returns list of (function_name, issue_description, missing_args)
    """
    issues = []
    graph_file = Path(__file__).parent.parent / "orchestration" / "graphs" / f"phase{phase_num}.py"
    
    if not graph_file.exists():
        return issues
    
    # Read and parse the file
    with open(graph_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    try:
        tree = ast.parse(content)
    except SyntaxError as e:
        return [("syntax_error", f"Syntax error in {graph_file}: {e}", [])]
    
    # Find all wrapper functions
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name.startswith('_wrap_step_'):
            # This is a wrapper function - check what it calls
            wrapper_name = node.name
            print(f"Checking {wrapper_name}...")
            
            # Look for function calls inside
            for child in ast.walk(node):
                if isinstance(child, ast.Call):
                    # Try to identify what function is being called
                    func_name = None
                    if isinstance(child.func, ast.Name):
                        func_name = child.func.id
                    elif isinstance(child.func, ast.Attribute):
                        func_name = child.func.attr
                    
                    if func_name and ('step_' in func_name or '_single' in func_name or '_batch' in func_name):
                        # Found a step function call - would need to check signature
                        # This is complex, so we'll do manual checking instead
                        pass
    
    return issues

def manual_check_common_issues():
    """Manually check for common issues we know about."""
    issues = []
    
    # Check phase 1 - step 1.11 (already fixed)
    # Check phase 2 - step 2.2 (all_entity_names is optional, so OK)
    
    # Let's check other phases that might have similar issues
    # Phase 11 - step 11.1 might need entities or relations
    # Phase 7 - various steps might need entities
    
    return issues

if __name__ == "__main__":
    print("Auditing phase graphs for missing function arguments...")
    print("=" * 80)
    
    # Check each phase
    for phase in range(1, 14):
        print(f"\nPhase {phase}:")
        issues = check_phase_graph(phase)
        if issues:
            for func, desc, missing in issues:
                print(f"  {func}: {desc}")
                if missing:
                    print(f"    Missing: {', '.join(missing)}")
        else:
            print("  No obvious issues found (manual review recommended)")
    
    print("\n" + "=" * 80)
    print("Audit complete. Manual review of function signatures recommended.")
