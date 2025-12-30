"""Quick test script for IoT description to verify recent fixes.

Uses a separate log file: NL2DATA/logs/nl2data_test_iot.log
"""

import asyncio
import sys
import argparse
import os
from pathlib import Path

# Add project root to path (go up 3 levels from NL2DATA/tests/)
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from NL2DATA.tests.integration_test import test_phases_1_2_3_4_5_6_7_integration
from NL2DATA.utils.logging import setup_logging, get_logger
from NL2DATA.config import get_config
from NL2DATA.tests.utils.pipeline_logger import get_pipeline_logger

# IoT description from nl_descriptions.txt (line 3)
IOT_DESCRIPTION = """Create an IoT telemetry dataset for 10,000 industrial sensors deployed across 100 plants. There should be one high-frequency fact table called sensor_reading with at least 200 million rows over a 30-day period, plus dimension tables for sensors, plants, and sensor types. Sensor readings (temperature, vibration, current) should mostly remain within normal operating bands that differ by sensor type, with rare anomalies (0.1–0.5% of readings) modeled as spikes, drifts, or sudden step changes. Inject 3–5 "cascading failure" incidents in which a single plant experiences coordinated anomalies across many sensors in a narrow time window, and each incident is preceded by subtle early-warning deviations. Timestamps should be approximately uniform over time but with random missing intervals per sensor to simulate connectivity issues. Include synthetic maintenance events that reset some sensors' behavior. The data should stress time-series joins, anomaly detection queries, and "before/after incident" window aggregations."""


async def test_iot_with_separate_log(max_phase: int = None):
    """Test IoT description with separate log file.
    
    Args:
        max_phase: Maximum phase to run (1-7). If None, runs all phases.
    """
    logger = get_logger(__name__)

    # Enable full, untruncated step output dumps in logs for this test script.
    # This is intentionally scoped to this test to avoid noisy logs in normal runs.
    os.environ.setdefault("NL2DATA_DEBUG_DUMP", "1")
    
    # Use separate test log file (relative to NL2DATA root).
    # NOTE: NL2DATA/tests/integration_test.py also calls setup_logging() internally, so we pass
    # this path down to prevent it from overwriting our test log file configuration.
    test_log_file = "logs/nl2data_test_iot.log"
    
    # Setup logging with test log file
    log_config = get_config('logging')
    setup_logging(
        level=log_config['level'],
        format_type=log_config['format'],
        log_to_file=True,  # Force file logging for this test run
        log_file=test_log_file,
        clear_existing=True,  # Clear the test log file
    )
    
    # Initialize pipeline logger for capturing LLM responses, ER diagrams, and schemas
    output_dir = Path(__file__).parent / "output"
    pipeline_logger = get_pipeline_logger()
    pipeline_logger.initialize(
        output_dir=str(output_dir),
        filename="pipeline_output.txt"
    )
    pipeline_output_file = pipeline_logger.output_file
    
    logger.info("=" * 80)
    logger.info("TEST RUN: IoT Description with Recent Fixes")
    logger.info(f"Log file: {test_log_file}")
    logger.info(f"Pipeline output file: {pipeline_output_file}")
    logger.info("=" * 80)
    
    print("\n" + "=" * 80)
    print("TEST RUN: IoT Description with Recent Fixes")
    print(f"Log file: {test_log_file}")
    print(f"Pipeline output file: {pipeline_output_file}")
    print("=" * 80)
    print(f"\nDescription:\n{IOT_DESCRIPTION}\n")
    
    try:
        # Run integration test with IoT description
        success = await test_phases_1_2_3_4_5_6_7_integration(
            nl_description=IOT_DESCRIPTION,
            description_index=1,
            max_phase=max_phase,
            log_file_override=test_log_file,
        )
        
        if success:
            print("\n" + "=" * 80)
            print("[PASS] IoT test completed successfully!")
            print(f"Check log file: {test_log_file}")
            print(f"Check pipeline output: {pipeline_output_file}")
            print("=" * 80)
        else:
            print("\n" + "=" * 80)
            print("[FAIL] IoT test completed with errors")
            print(f"Check log file: {test_log_file}")
            print(f"Check pipeline output: {pipeline_output_file}")
            print("=" * 80)
        
        return success
        
    except Exception as e:
        logger.error(f"Test failed with exception: {e}", exc_info=True)
        print(f"\n[ERROR] Test failed: {e}")
        print(f"Check log file: {test_log_file}")
        print(f"Check pipeline output: {pipeline_output_file}")
        return False
    finally:
        # Close pipeline logger
        pipeline_logger.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Test IoT description with NL2DATA pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python test_iot_fixes.py                    # Run all phases (1-7)
  python test_iot_fixes.py --max-phase 3      # Run phases 1, 2, and 3 only
  python test_iot_fixes.py -m 1               # Run only phase 1
        """
    )
    parser.add_argument(
        "--max-phase", "-m",
        type=int,
        default=None,
        choices=range(1, 8),
        metavar="PHASE",
        help="Maximum phase to run (1-7). If not specified, runs all phases."
    )
    
    args = parser.parse_args()
    
    success = asyncio.run(test_iot_with_separate_log(max_phase=args.max_phase))
    sys.exit(0 if success else 1)

