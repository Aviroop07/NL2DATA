"""Simple verification script for LangChain best practices.

This script checks the code structure without requiring all dependencies.
Run this to verify that improvements are in place.
"""

import sys
from pathlib import Path
import re

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

def check_file_for_patterns(file_path: Path, patterns: dict) -> dict:
    """Check a file for specific patterns."""
    try:
        content = file_path.read_text(encoding='utf-8')
        results = {}
        for name, pattern in patterns.items():
            matches = len(re.findall(pattern, content, re.MULTILINE))
            results[name] = matches > 0
        return results
    except Exception as e:
        return {"error": str(e)}


def verify_langchain_improvements():
    """Verify that LangChain best practices are implemented."""
    print("=" * 80)
    print("LangChain Best Practices Verification")
    print("=" * 80)
    
    phases_dir = Path(__file__).parent.parent / "phases"
    step_files = list(phases_dir.rglob("step_*.py"))
    
    print(f"\nFound {len(step_files)} step files to check\n")
    
    patterns_to_check = {
        "traceable_step_decorator": r"@traceable_step",
        "get_trace_config_import": r"from NL2DATA\.utils\.observability import.*get_trace_config",
        "get_trace_config_call": r"config = get_trace_config\(",
        "config_parameter": r"config=config",
        "runnable_retry": r"RunnableRetry",
    }
    
    stats = {
        "files_with_decorator": 0,
        "files_with_config": 0,
        "files_with_import": 0,
        "total_files": len(step_files),
    }
    
    issues = []
    
    for step_file in step_files:
        results = check_file_for_patterns(step_file, patterns_to_check)
        
        if "error" in results:
            issues.append(f"{step_file.name}: {results['error']}")
            continue
        
        if results.get("traceable_step_decorator"):
            stats["files_with_decorator"] += 1
        else:
            issues.append(f"{step_file.name}: Missing @traceable_step decorator")
        
        if results.get("get_trace_config_call"):
            stats["files_with_config"] += 1
        else:
            # Some files might not have LLM calls (deterministic steps)
            if "compilation" not in step_file.name.lower() and "normalization" not in step_file.name.lower():
                issues.append(f"{step_file.name}: Missing get_trace_config() call")
        
        if results.get("get_trace_config_import"):
            stats["files_with_import"] += 1
    
    # Check chain_utils.py
    chain_utils = Path(__file__).parent.parent / "utils" / "llm" / "chain_utils.py"
    if chain_utils.exists():
        chain_results = check_file_for_patterns(chain_utils, {
            "runnable_retry": r"RunnableRetry",
            "runnable_config": r"RunnableConfig",
        })
        print("\n[chain_utils.py]")
        print(f"  RunnableRetry: {'[OK]' if chain_results.get('runnable_retry') else '[MISSING]'}")
        print(f"  RunnableConfig: {'[OK]' if chain_results.get('runnable_config') else '[MISSING]'}")
    
    # Check observability module
    observability_init = Path(__file__).parent.parent / "utils" / "observability" / "__init__.py"
    if observability_init.exists():
        print("\n[observability/__init__.py]")
        print("  [OK] Observability module exists")
    
    # Print statistics
    print("\n" + "=" * 80)
    print("Statistics")
    print("=" * 80)
    print(f"Total step files: {stats['total_files']}")
    print(f"Files with @traceable_step: {stats['files_with_decorator']}/{stats['total_files']}")
    print(f"Files with get_trace_config(): {stats['files_with_config']}/{stats['total_files']}")
    print(f"Files with observability import: {stats['files_with_import']}/{stats['total_files']}")
    
    if issues:
        print("\n" + "=" * 80)
        print("Issues Found")
        print("=" * 80)
        for issue in issues[:20]:  # Show first 20 issues
            print(f"  - {issue}")
        if len(issues) > 20:
            print(f"  ... and {len(issues) - 20} more issues")
    else:
        print("\n[OK] No issues found!")
    
    print("\n" + "=" * 80)
    print("Verification Complete")
    print("=" * 80)
    
    return len(issues) == 0


if __name__ == "__main__":
    success = verify_langchain_improvements()
    sys.exit(0 if success else 1)

