"""Test script to verify NL2DATA setup."""

import sys
from pathlib import Path

# Add project root to path so we can import NL2DATA
# tests -> NL2DATA -> Project root
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from NL2DATA.config import load_config, get_config
from NL2DATA.utils.env import load_env, get_api_key
from NL2DATA.utils.logging import setup_logging, get_logger


def test_config():
    """Test configuration loading."""
    print("Testing configuration loading...")
    try:
        config = load_config()
        print(f"[PASS] Configuration loaded successfully")
        print(f"  - OpenAI model: {get_config('openai')['model']}")
        print(f"  - Logging level: {get_config('logging')['level']}")
        return True
    except Exception as e:
        print(f"[FAIL] Configuration loading failed: {e}")
        return False


def test_env():
    """Test environment variable loading."""
    print("\nTesting environment variable loading...")
    try:
        load_env()
        api_key = get_api_key()
        if api_key:
            # Mask the key for security
            masked_key = api_key[:8] + "..." + api_key[-4:] if len(api_key) > 12 else "***"
            print(f"[PASS] Environment variables loaded successfully")
            print(f"  - OPENAI_API_KEY: {masked_key}")
            return True
    except Exception as e:
        print(f"[FAIL] Environment variable loading failed: {e}")
        print("  Note: Make sure .env file exists with OPENAI_API_KEY")
        return False


def test_logging():
    """Test logging setup."""
    print("\nTesting logging setup...")
    try:
        setup_logging(
            level=get_config('logging')['level'],
            format_type=get_config('logging')['format'],
            log_to_file=get_config('logging')['log_to_file'],
            log_file=get_config('logging')['log_file']
        )
        logger = get_logger(__name__)
        logger.info("Test log message")
        print("[PASS] Logging setup successful")
        return True
    except Exception as e:
        print(f"[FAIL] Logging setup failed: {e}")
        return False


def main():
    """Run all setup tests."""
    print("=" * 50)
    print("NL2DATA Setup Verification")
    print("=" * 50)
    
    results = []
    results.append(("Configuration", test_config()))
    results.append(("Environment", test_env()))
    results.append(("Logging", test_logging()))
    
    print("\n" + "=" * 50)
    print("Test Results Summary")
    print("=" * 50)
    
    for name, passed in results:
        status = "[PASS]" if passed else "[FAIL]"
        print(f"{status} - {name}")
    
    all_passed = all(result[1] for result in results)
    
    if all_passed:
        print("\n[PASS] All tests passed! Setup is complete.")
        return 0
    else:
        print("\n[FAIL] Some tests failed. Please check the errors above.")
        return 1


if __name__ == "__main__":
    sys.exit(main())

