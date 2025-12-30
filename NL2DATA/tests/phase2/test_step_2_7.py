"""Test script for Step 2.7: Primary Key Identification."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase2 import step_2_7_primary_key_identification_batch
from NL2DATA.utils.logging import setup_logging, get_logger
from NL2DATA.config import get_config

async def test_step_2_7():
    """Test Step 2.7: Primary Key Identification."""
    logger = get_logger(__name__)
    setup_logging(
        level=get_config('logging')['level'],
        format_type=get_config('logging')['format'],
        log_to_file=get_config('logging')['log_to_file'],
        log_file=get_config('logging')['log_file']
    )
    
    print("=" * 80)
    print("Testing Step 2.7: Primary Key Identification")
    print("=" * 80)
    
    all_passed = True
    
    try:
        # Test Case 1: Bookstore entities with clear primary keys
        print("\n" + "-" * 80)
        print("Test Case 1: Bookstore entities")
        print("-" * 80)
        nl_description = """I need a database for an online bookstore. Customers can place orders for books. 
        Each order can contain multiple books. Books have titles, authors, ISBN, publication date, and prices. 
        Customers have names, email addresses, phone numbers, and shipping addresses. 
        Orders have order dates, total amounts, and status (pending, shipped, delivered)."""
        
        entities_1 = [
            {"name": "Customer", "description": "An individual who purchases books from the online bookstore"},
            {"name": "Book", "description": "An item available for purchase in the online bookstore"},
            {"name": "Purchase", "description": "A transaction made by a customer to purchase one or more books"},
        ]
        
        entity_attributes_1 = {
            "Customer": ["customer_id", "name", "email", "phone", "address"],
            "Book": ["book_id", "title", "author", "isbn", "publication_date", "price"],
            "Purchase": ["purchase_id", "purchase_date", "total_amount", "status"],
        }
        
        result_1 = await step_2_7_primary_key_identification_batch(
            entities=entities_1,
            entity_attributes=entity_attributes_1,
            nl_description=nl_description,
            domain="e-commerce"
        )
        
        entity_results_1 = result_1.get("entity_results", {})
        print(f"[PASS] Step 2.7 completed: processed {len(entity_results_1)} entities")
        
        for entity_name, result in entity_results_1.items():
            pk = result.get("primary_key", [])
            reasoning = result.get("reasoning", "")
            alt_keys = result.get("alternative_keys", [])
            print(f"  - {entity_name}:")
            print(f"    Primary Key: {pk}")
            print(f"    Reasoning: {reasoning if reasoning else 'N/A'}")
            if alt_keys:
                print(f"    Alternative Keys: {alt_keys}")
            
            # Validate that PK attributes exist
            available_attrs = entity_attributes_1.get(entity_name, [])
            invalid_pk_attrs = [attr for attr in pk if attr not in available_attrs]
            if invalid_pk_attrs:
                print(f"    [ERROR] Invalid PK attributes: {invalid_pk_attrs}")
                all_passed = False
            elif not pk:
                print(f"    [WARNING] No primary key identified")
            else:
                print(f"    [OK] Primary key validated")
        
        # Test Case 2: Entity without explicit ID
        print("\n" + "-" * 80)
        print("Test Case 2: Entity without explicit ID attribute")
        print("-" * 80)
        entities_2 = [
            {"name": "Product", "description": "A product in the catalog"},
        ]
        
        entity_attributes_2 = {
            "Product": ["name", "sku", "category", "price", "description"],
        }
        
        result_2 = await step_2_7_primary_key_identification_batch(
            entities=entities_2,
            entity_attributes=entity_attributes_2,
            nl_description="Products have SKU codes that uniquely identify them",
        )
        
        entity_results_2 = result_2.get("entity_results", {})
        for entity_name, result in entity_results_2.items():
            pk = result.get("primary_key", [])
            reasoning = result.get("reasoning", "")
            print(f"  - {entity_name}:")
            print(f"    Primary Key: {pk}")
            print(f"    Reasoning: {reasoning if reasoning else 'N/A'}")
            
            if not pk:
                print(f"    [WARNING] No primary key identified - may need to suggest ID creation")
            else:
                print(f"    [OK] Primary key identified")
        
        print("\n" + "=" * 80)
        if all_passed:
            print("[PASS] All Step 2.7 tests passed!")
        else:
            print("[ERROR] Some Step 2.7 tests failed")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n[ERROR] Step 2.7 test failed: {e}")
        import traceback
        traceback.print_exc()
        all_passed = False
    
    return all_passed

if __name__ == "__main__":
    success = asyncio.run(test_step_2_7())
    sys.exit(0 if success else 1)

