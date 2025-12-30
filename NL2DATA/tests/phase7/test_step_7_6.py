"""Test script for Step 7.6: Distribution Compilation.

This is a deterministic step that can be tested without LLM calls.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase7 import step_7_6_distribution_compilation
from NL2DATA.utils.logging import setup_logging, get_logger
from NL2DATA.config import get_config

def test_step_7_6():
    """Test Step 7.6: Distribution Compilation."""
    logger = get_logger(__name__)
    setup_logging(
        level=get_config('logging')['level'],
        format_type=get_config('logging')['format'],
        log_to_file=get_config('logging')['log_to_file'],
        log_file=get_config('logging')['log_file']
    )
    
    print("=" * 80)
    print("Testing Step 7.6: Distribution Compilation")
    print("=" * 80)
    
    all_passed = True
    
    try:
        # Test Case 1: Compile generation strategies
        print("\n" + "-" * 80)
        print("Test Case 1: Compile generation strategies")
        print("-" * 80)
        
        numerical_strategies = {
            "Transaction.amount": {
                "distribution_type": "lognormal",
                "parameters": {"mu": 3.5, "sigma": 1.2},
                "min": 0.01,
                "max": 100000.0,
            },
            "Customer.age": {
                "distribution_type": "normal",
                "parameters": {"mu": 35, "sigma": 10},
                "min": 18,
                "max": 100,
            },
        }
        
        text_strategies = {
            "Customer.name": {
                "generator_type": "faker.name",
                "parameters": {},
                "fallback": "faker.first_name",
                "not_possible": False,
            },
            "Customer.email": {
                "generator_type": "faker.email",
                "parameters": {},
                "fallback": None,
                "not_possible": False,
            },
        }
        
        boolean_strategies = {
            "Customer.is_premium": {
                "is_random": False,
                "dependency_dsl": "IF subscription_type = 'premium' THEN true ELSE false",
            },
            "Order.is_shipped": {
                "is_random": True,  # Random booleans now use bernoulli distribution
                "dependency_dsl": None,
            },
        }
        
        categorical_strategies = {
            "Order.status": {
                "distribution": {
                    "pending": 0.3,
                    "shipped": 0.5,
                    "delivered": 0.2,
                },
            },
        }
        
        entity_volumes = {
            "Customer": {
                "min_rows": 1000,
                "max_rows": 10000,
                "expected_rows": 5000,
            },
            "Transaction": {
                "min_rows": 100000,
                "max_rows": 1000000,
                "expected_rows": 500000,
            },
        }
        
        # Step 7.6: Distribution Compilation
        print("\nStep 7.6: Distribution Compilation")
        result_7_6 = step_7_6_distribution_compilation(
            numerical_strategies=numerical_strategies,
            text_strategies=text_strategies,
            boolean_strategies=boolean_strategies,
            categorical_strategies=categorical_strategies,
            entity_volumes=entity_volumes,
        )
        
        column_gen_specs = result_7_6.get("column_gen_specs", [])
        volumes = result_7_6.get("entity_volumes", {})
        
        print(f"[PASS] Step 7.6 completed")
        print(f"  - Column generation specs: {len(column_gen_specs)}")
        print(f"  - Entity volumes: {len(volumes)}")
        
        # Show some specs
        for spec in column_gen_specs[:5]:
            table = spec.get("table", "")
            column = spec.get("column", "")
            spec_type = spec.get("type", "")
            print(f"    - {table}.{column}: {spec_type}")
        
        if len(column_gen_specs) != 7:  # 2 numerical + 2 text + 2 boolean + 1 categorical
            print(f"    [ERROR] Expected 7 column specs, got {len(column_gen_specs)}")
            all_passed = False
        else:
            print(f"    [OK] Distribution compilation successful")
        
        print("\n" + "=" * 80)
        if all_passed:
            print("[PASS] All Step 7.6 tests passed!")
        else:
            print("[ERROR] Some Step 7.6 tests failed")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n[ERROR] Step 7.6 test failed: {e}")
        import traceback
        traceback.print_exc()
        all_passed = False
    
    assert all_passed, "Some Step 7.6 tests failed"

if __name__ == "__main__":
    success = test_step_7_6()
    sys.exit(0 if success else 1)

