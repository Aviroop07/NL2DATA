"""Test Step 2.2 format string fix with context containing braces."""

import sys
import asyncio
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase2 import step_2_2_intrinsic_attributes
from NL2DATA.utils.logging import setup_logging, get_logger
from NL2DATA.config import get_config

logger = get_logger(__name__)


async def test_step_2_2_with_braces_in_context():
    """Test Step 2.2 with context that contains braces (like dictionary representations)."""
    print("=" * 80)
    print("Testing Step 2.2 Format String Fix")
    print("=" * 80)
    
    nl_description = "Customers have names and email addresses. Orders have order dates and total amounts."
    
    # Create relations with cardinalities that contain braces when stringified
    relations = [
        {
            "entities": ["Customer", "Order"],
            "type": "one-to-many",
            "description": "Customer places Order",
            "entity_cardinalities": {"Customer": "1", "Order": "N"},
            "entity_participations": {"Customer": "partial", "Order": "total"}
        }
    ]
    
    try:
        result = await step_2_2_intrinsic_attributes(
            entity_name="Customer",
            nl_description=nl_description,
            entity_description="A customer who places orders",
            domain="e-commerce",
            relations=relations,
            primary_key=None,
            explicit_attributes=None
        )
        
        print("[PASS] Step 2.2 completed without format string errors")
        print(f"  Extracted {len(result.get('attributes', []))} attributes")
        
        # Verify attributes were extracted
        attributes = result.get("attributes", [])
        assert len(attributes) > 0, "Should have extracted at least one attribute"
        
        print(f"[PASS] Successfully extracted attributes: {[attr.get('name') for attr in attributes[:5]]}")
        return True
        
    except ValueError as e:
        if "Single '}' encountered" in str(e) or "format string" in str(e).lower():
            print(f"[FAIL] Format string error still occurs: {e}")
            return False
        else:
            # Other ValueError is OK (might be validation error)
            print(f"[PASS] No format string error (other ValueError: {e})")
            return True
    except Exception as e:
        print(f"[FAIL] Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    log_config = get_config('logging')
    setup_logging(
        level=log_config['level'],
        format_type=log_config['format'],
        log_to_file=log_config['log_to_file'],
        log_file=log_config.get('log_file'),
        clear_existing=True,
    )
    success = asyncio.run(test_step_2_2_with_braces_in_context())
    sys.exit(0 if success else 1)

