"""Test script for Step 1.1: Domain Detection."""

import asyncio
import sys
from pathlib import Path

# Add project root to path so we can import NL2DATA
# tests/phase1 -> tests -> NL2DATA -> Project root
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase1.step_1_1_domain_detection import step_1_1_domain_detection
from NL2DATA.utils.logging import setup_logging, get_logger
from NL2DATA.config import get_config


async def test_domain_detection():
    """Test domain detection with example descriptions."""
    logger = get_logger(__name__)
    
    # Setup logging
    setup_logging(
        level=get_config('logging')['level'],
        format_type=get_config('logging')['format'],
        log_to_file=get_config('logging')['log_to_file'],
        log_file=get_config('logging')['log_file']
    )
    
    test_cases = [
        "I need a database for an e-commerce store",
        "Create a hospital management system",
        "I want to track customer orders and products",
    ]
    
    print("=" * 60)
    print("Testing Step 1.1: Domain Detection")
    print("=" * 60)
    
    for i, description in enumerate(test_cases, 1):
        print(f"\nTest Case {i}:")
        print(f"Description: {description}")
        print("-" * 60)
        
        try:
            result = await step_1_1_domain_detection(description)
            
            print(f"[PASS] Success")
            print(f"  - Has explicit domain: {result['has_explicit_domain']}")
            if result.get('domain'):
                print(f"  - Domain: {result['domain']}")
            if result.get('reasoning'):
                print(f"  - Reasoning: {result['reasoning']}")
                
        except Exception as e:
            print(f"[ERROR] Error: {e}")
            logger.error(f"Test case {i} failed", exc_info=True)


if __name__ == "__main__":
    asyncio.run(test_domain_detection())

