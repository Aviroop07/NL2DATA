"""Test script for Step 2.6: Naming Convention Validation."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase2 import step_2_6_naming_convention_validation
from NL2DATA.utils.logging import setup_logging, get_logger
from NL2DATA.config import get_config

def test_step_2_6():
    """Test Step 2.6: Naming Convention Validation."""
    logger = get_logger(__name__)
    setup_logging(
        level=get_config('logging')['level'],
        format_type=get_config('logging')['format'],
        log_to_file=get_config('logging')['log_to_file'],
        log_file=get_config('logging')['log_file']
    )
    
    print("=" * 80)
    print("Testing Step 2.6: Naming Convention Validation")
    print("=" * 80)
    
    all_passed = True
    
    try:
        # Test Case 1: Valid names
        print("\n" + "-" * 80)
        print("Test Case 1: Valid names (should pass)")
        print("-" * 80)
        entities_1 = [
            {"name": "Customer", "description": "Customer entity"},
            {"name": "Purchase", "description": "Purchase entity"},
        ]
        entity_attributes_1 = {
            "Customer": ["customer_id", "name", "email", "phone"],
            "Purchase": ["purchase_id", "purchase_date", "total_amount"],
        }
        
        result_1 = step_2_6_naming_convention_validation(
            entities=entities_1,
            entity_attributes=entity_attributes_1
        )
        
        validation_passed_1 = result_1.get("validation_passed", False)
        conflicts_1 = result_1.get("naming_conflicts", [])
        violations_1 = result_1.get("naming_violations", [])
        
        if validation_passed_1 and len(conflicts_1) == 0 and len(violations_1) == 0:
            print(f"[PASS] Test Case 1: Validation passed (conflicts={len(conflicts_1)}, violations={len(violations_1)})")
        else:
            print(f"[ERROR] Test Case 1: Expected validation to pass, but got conflicts={len(conflicts_1)}, violations={len(violations_1)}")
            if conflicts_1:
                print(f"  Conflicts found:")
                for c in conflicts_1:
                    print(f"    - {c.get('name')}: {c.get('conflict_type')} in {c.get('entities_affected')}")
            if violations_1:
                print(f"  Violations found:")
                for v in violations_1:
                    print(f"    - {v.get('name')}: {v.get('violation')}")
            all_passed = False
        
        # Test Case 2: Reserved keywords
        print("\n" + "-" * 80)
        print("Test Case 2: Reserved keywords (should detect conflicts)")
        print("-" * 80)
        entities_2 = [
            {"name": "select", "description": "Entity with reserved keyword name"},
        ]
        entity_attributes_2 = {
            "select": ["from", "where", "name"],
        }
        
        result_2 = step_2_6_naming_convention_validation(
            entities=entities_2,
            entity_attributes=entity_attributes_2
        )
        
        validation_passed_2 = result_2.get("validation_passed", False)
        conflicts_2 = result_2.get("naming_conflicts", [])
        reserved_keyword_conflicts = [c for c in conflicts_2 if c.get("conflict_type") == "reserved_keyword"]
        
        if not validation_passed_2 and len(reserved_keyword_conflicts) > 0:
            print(f"[PASS] Test Case 2: Detected {len(reserved_keyword_conflicts)} reserved keyword conflicts")
            for conflict in reserved_keyword_conflicts[:3]:
                print(f"  - {conflict.get('name')}: {conflict.get('suggestion')}")
        else:
            print(f"[ERROR] Test Case 2: Expected reserved keyword conflicts, got conflicts={len(conflicts_2)}")
            all_passed = False
        
        # Test Case 3: Invalid characters
        print("\n" + "-" * 80)
        print("Test Case 3: Invalid characters (should detect violations)")
        print("-" * 80)
        entities_3 = [
            {"name": "Customer-Info", "description": "Entity with invalid characters"},
        ]
        entity_attributes_3 = {
            "Customer-Info": ["customer name", "email@address"],
        }
        
        result_3 = step_2_6_naming_convention_validation(
            entities=entities_3,
            entity_attributes=entity_attributes_3
        )
        
        validation_passed_3 = result_3.get("validation_passed", False)
        violations_3 = result_3.get("naming_violations", [])
        
        if not validation_passed_3 and len(violations_3) > 0:
            print(f"[PASS] Test Case 3: Detected {len(violations_3)} naming violations")
            for violation in violations_3[:3]:
                print(f"  - {violation.get('name')}: {violation.get('violation')} -> {violation.get('suggestion')}")
        else:
            print(f"[ERROR] Test Case 3: Expected naming violations, got violations={len(violations_3)}")
            all_passed = False
        
        # Test Case 4: Duplicate attribute names
        print("\n" + "-" * 80)
        print("Test Case 4: Duplicate attribute names (should detect conflicts)")
        print("-" * 80)
        entities_4 = [
            {"name": "Customer", "description": "Customer entity"},
        ]
        entity_attributes_4 = {
            "Customer": ["name", "email", "name"],  # Duplicate "name"
        }
        
        result_4 = step_2_6_naming_convention_validation(
            entities=entities_4,
            entity_attributes=entity_attributes_4
        )
        
        validation_passed_4 = result_4.get("validation_passed", False)
        conflicts_4 = result_4.get("naming_conflicts", [])
        duplicate_conflicts = [c for c in conflicts_4 if c.get("conflict_type") == "duplicate_attribute"]
        
        if not validation_passed_4 and len(duplicate_conflicts) > 0:
            print(f"[PASS] Test Case 4: Detected {len(duplicate_conflicts)} duplicate attribute conflicts")
            for conflict in duplicate_conflicts:
                print(f"  - {conflict.get('name')}: duplicate in {conflict.get('entities_affected')}")
        else:
            print(f"[ERROR] Test Case 4: Expected duplicate attribute conflicts, got conflicts={len(conflicts_4)}")
            all_passed = False
        
        print("\n" + "=" * 80)
        if all_passed:
            print("[PASS] All Step 2.6 tests passed!")
        else:
            print("[ERROR] Some Step 2.6 tests failed")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n[ERROR] Step 2.6 test failed: {e}")
        import traceback
        traceback.print_exc()
        all_passed = False
    
    return all_passed

if __name__ == "__main__":
    success = test_step_2_6()
    sys.exit(0 if success else 1)

