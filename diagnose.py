"""Diagnostic script to check if backend and frontend are working."""

import sys
import requests
import json
from datetime import datetime

BASE_URL = "http://127.0.0.1:8000"

def print_header(text):
    print("\n" + "=" * 80)
    print(text)
    print("=" * 80)

def test_backend_health():
    """Test if backend is running."""
    print_header("TEST 1: Backend Health Check")
    try:
        response = requests.get(f"{BASE_URL}/health", timeout=5)
        if response.status_code == 200:
            print(f"[OK] Backend is running")
            print(f"  Response: {response.json()}")
            return True
        else:
            print(f"[FAIL] Backend returned status {response.status_code}")
            return False
    except requests.exceptions.ConnectionError:
        print("[FAIL] Cannot connect to backend")
        print(f"  Make sure backend is running on {BASE_URL}")
        print("  Start it with: python -m uvicorn backend.main:app --host 127.0.0.1 --port 8000 --reload")
        return False
    except Exception as e:
        print(f"[ERROR] Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_suggestions_endpoint():
    """Test suggestions endpoint with detailed validation."""
    print_header("TEST 2: Suggestions Endpoint")
    try:
        test_nl = "I need a database for a library with books, authors, and members."
        print(f"  Testing with NL: \"{test_nl}\"")
        
        response = requests.post(
            f"{BASE_URL}/api/suggestions",
            json={"nl_description": test_nl},
            timeout=30
        )
        
        if response.status_code != 200:
            print(f"[FAIL] Suggestions endpoint returned status {response.status_code}")
            print(f"  Response: {response.text[:200]}")
            return False
        
        data = response.json()
        
        # Validate response structure
        if "keywords" not in data:
            print("[FAIL] Response missing 'keywords' field")
            return False
        
        if "extracted_items" not in data:
            print("[FAIL] Response missing 'extracted_items' field")
            return False
        
        keywords = data.get("keywords", [])
        extracted_items = data.get("extracted_items", {})
        
        # Check if response is empty (likely an error fallback)
        entities = extracted_items.get("entities", [])
        if len(keywords) == 0 and len(entities) == 0:
            print("\n  [FAIL] Empty response detected - likely LLM call failed or returned empty result")
            print("  This usually indicates:")
            print("    - LLM API error (check backend logs)")
            print("    - API key issue")
            print("    - Network/timeout issue")
            print("    - LLM parsing error")
            print("\n  Check backend terminal for detailed error logs")
            return False
        
        # Validate keywords structure and print details
        print(f"\n  [VALIDATION] Keywords returned: {len(keywords)}")
        if len(keywords) == 0:
            print("  [WARNING] No keywords returned - this might be expected for very complete descriptions")
        else:
            print(f"\n  [DETAILED KEYWORDS]")
            print(f"  {'=' * 76}")
            for idx, keyword in enumerate(keywords, 1):
                if "text" not in keyword:
                    print(f"  [FAIL] Keyword {idx} missing 'text' field")
                    return False
                if "type" not in keyword:
                    print(f"  [FAIL] Keyword {idx} missing 'type' field")
                    return False
                if "enhanced_nl_description" not in keyword:
                    print(f"  [FAIL] Keyword {idx} missing 'enhanced_nl_description' field")
                    return False
                
                keyword_text = keyword.get("text", "")
                keyword_type = keyword.get("type", "")
                enhanced = keyword.get("enhanced_nl_description", "")
                
                # Check that enhanced description contains the original text
                contains_original = test_nl.lower() in enhanced.lower()
                
                print(f"\n  Keyword #{idx}:")
                print(f"    Text: \"{keyword_text}\"")
                print(f"    Type: {keyword_type}")
                print(f"    Enhanced Description Length: {len(enhanced)} characters")
                print(f"    Contains Original Text: {'Yes' if contains_original else 'No (WARNING)'}")
                if not contains_original:
                    print(f"    [WARNING] Enhanced description doesn't contain original text!")
                
                # Show enhanced description (truncated if too long)
                if len(enhanced) > 200:
                    print(f"    Enhanced Description (first 200 chars):")
                    print(f"      \"{enhanced[:200]}...\"")
                    print(f"    Enhanced Description (last 100 chars):")
                    print(f"      \"...{enhanced[-100:]}\"")
                else:
                    print(f"    Enhanced Description:")
                    print(f"      \"{enhanced}\"")
                
                print(f"    {'-' * 76}")
        
        # Validate extracted_items structure and print details
        print(f"\n  [VALIDATION] Extracted Items:")
        required_fields = ["domain", "entities", "column_names", "relationships", "constraints", "cardinalities"]
        for field in required_fields:
            if field not in extracted_items:
                print(f"  [FAIL] Extracted items missing '{field}' field")
                return False
        
        print(f"\n  [DETAILED EXTRACTED ITEMS]")
        print(f"  {'=' * 76}")
        
        # Domain
        domain = extracted_items.get("domain")
        print(f"\n  Domain: {domain if domain else '(none)'}")
        
        # Entities
        print(f"\n  Entities ({len(entities)}):")
        if entities:
            for idx, entity in enumerate(entities, 1):
                print(f"    [{idx}] {entity}")
        else:
            print(f"    (none)")
        
        # Column Names
        column_names = extracted_items.get("column_names", [])
        print(f"\n  Column Names ({len(column_names)}):")
        if column_names:
            for idx, col in enumerate(column_names, 1):
                print(f"    [{idx}] {col}")
        else:
            print(f"    (none)")
        
        # Relationships
        relationships = extracted_items.get("relationships", [])
        print(f"\n  Relationships ({len(relationships)}):")
        if relationships:
            for idx, rel in enumerate(relationships, 1):
                print(f"    [{idx}] {rel}")
        else:
            print(f"    (none)")
        
        # Constraints
        constraints = extracted_items.get("constraints", [])
        print(f"\n  Constraints ({len(constraints)}):")
        if constraints:
            for idx, constraint in enumerate(constraints, 1):
                print(f"    [{idx}] {constraint}")
        else:
            print(f"    (none)")
        
        # Cardinalities
        cardinalities = extracted_items.get("cardinalities", [])
        print(f"\n  Cardinalities ({len(cardinalities)}):")
        if cardinalities:
            for idx, card in enumerate(cardinalities, 1):
                print(f"    [{idx}] {card}")
        else:
            print(f"    (none)")
        
        print(f"  {'=' * 76}")
        
        # Check that extracted entities match the input
        nl_lower = test_nl.lower()
        found_entities = []
        for entity in ["books", "authors", "members", "library"]:
            if entity in nl_lower:
                found_entities.append(entity)
        
        if len(found_entities) > 0:
            print(f"\n  [VALIDATION] Expected entities in input: {found_entities}")
            extracted_entities_lower = [e.lower() for e in entities]
            matches = [e for e in found_entities if any(e in ex for ex in extracted_entities_lower)]
            if len(matches) > 0:
                print(f"    [OK] Found {len(matches)} matching entities in extracted items")
            else:
                print(f"    [WARNING] No matching entities found - extraction might need improvement")
        
        # Final validation - ensure we got meaningful data
        if len(keywords) == 0 and len(entities) == 0:
            print(f"\n[FAIL] Suggestions endpoint returned empty response")
            print(f"  This indicates the LLM call failed or returned no data")
            print(f"  Check backend logs for detailed error information")
            return False
        
        print(f"\n[OK] Suggestions endpoint working correctly")
        print(f"  ✓ Response structure valid")
        print(f"  ✓ Keywords structure valid ({len(keywords)} keywords)")
        print(f"  ✓ Extracted items structure valid ({len(entities)} entities extracted)")
        return True
        
    except requests.exceptions.ConnectionError:
        print("[FAIL] Cannot connect to backend")
        print(f"  Make sure backend is running on {BASE_URL}")
        return False
    except requests.exceptions.Timeout:
        print("[FAIL] Request timed out (LLM call may be slow)")
        return False
    except json.JSONDecodeError as e:
        print(f"[FAIL] Invalid JSON response: {e}")
        print(f"  Response: {response.text[:500]}")
        return False
    except Exception as e:
        print(f"[ERROR] Unexpected error: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_processing_endpoint():
    """Test processing start endpoint."""
    print_header("TEST 3: Processing Start Endpoint")
    try:
        test_nl = "I need a database for a library with books, authors, and members."
        response = requests.post(
            f"{BASE_URL}/api/process/start",
            json={"nl_description": test_nl},
            timeout=10
        )
        if response.status_code == 200:
            data = response.json()
            print(f"[OK] Processing endpoint working")
            print(f"  Job ID: {data.get('job_id')}")
            print(f"  Status: {data.get('status')}")
            return True, data.get('job_id')
        else:
            print(f"[FAIL] Processing endpoint returned status {response.status_code}")
            print(f"  Response: {response.text[:200]}")
            return False, None
    except requests.exceptions.ConnectionError:
        print("[FAIL] Cannot connect to backend")
        print(f"  Make sure backend is running on {BASE_URL}")
        return False, None
    except requests.exceptions.Timeout:
        print("[FAIL] Request timed out")
        return False, None
    except Exception as e:
        print(f"[ERROR] Unexpected error: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return False, None

def test_distributions_metadata():
    """Test distributions metadata endpoint."""
    print_header("TEST 4: Distributions Metadata Endpoint")
    try:
        response = requests.get(f"{BASE_URL}/api/schema/distributions/metadata", timeout=10)
        if response.status_code == 200:
            data = response.json()
            print(f"[OK] Distributions metadata endpoint working")
            print(f"  Distributions returned: {len(data.get('distributions', []))})")
            return True
        else:
            print(f"[FAIL] Distributions endpoint returned status {response.status_code}")
            return False
    except requests.exceptions.ConnectionError:
        print("[FAIL] Cannot connect to backend")
        print(f"  Make sure backend is running on {BASE_URL}")
        return False
    except requests.exceptions.Timeout:
        print("[FAIL] Request timed out")
        return False
    except Exception as e:
        print(f"[ERROR] Unexpected error: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    print_header("BACKEND DIAGNOSTIC TOOL")
    print(f"Testing backend at: {BASE_URL}")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    results = []
    
    # Test 1: Health check (CRITICAL - must pass for other tests)
    health_result = test_backend_health()
    results.append(("Health Check", health_result))
    
    if not health_result:
        print("\n" + "=" * 80)
        print("BACKEND IS NOT RUNNING!")
        print("=" * 80)
        print("\nTo start the backend:")
        print("  1. Activate virtual environment: .\\venv\\Scripts\\Activate.ps1")
        print("  2. Run: python -m uvicorn backend.main:app --host 127.0.0.1 --port 8000 --reload")
        print("\n" + "=" * 80)
        print("STOPPING TESTS - Backend must be running for other tests")
        print("=" * 80)
        # Still show summary with just health check
        print_header("SUMMARY")
        print("  [FAIL]: Health Check")
        print("\nResults: 0/1 tests passed")
        print("\n[FAILURE] Backend is not running. Start the backend and try again.")
        return
    
    # Test 2: Distributions (simple endpoint)
    results.append(("Distributions Metadata", test_distributions_metadata()))
    
    # Test 3: Suggestions (LLM call)
    results.append(("Suggestions Endpoint", test_suggestions_endpoint()))
    
    # Test 4: Processing start
    success, job_id = test_processing_endpoint()
    results.append(("Processing Start", success))
    
    # Summary
    print_header("SUMMARY")
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for name, result in results:
        status = "[PASS]" if result else "[FAIL]"
        print(f"  {status}: {name}")
    
    print(f"\nResults: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n[SUCCESS] All tests passed! Backend is working correctly.")
    else:
        print("\n[FAILURE] Some tests failed. Check the errors above.")

if __name__ == "__main__":
    main()

