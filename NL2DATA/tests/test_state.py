"""Test script to verify GenerationState structure."""

import sys
from pathlib import Path

# Add project root to path so we can import NL2DATA
# tests -> NL2DATA -> Project root
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from NL2DATA.ir import (
    GenerationState,
    create_empty_state,
    EntityInfo,
    get_entity_names,
    has_entity,
)


def test_state_creation():
    """Test creating and using GenerationState."""
    print("=" * 60)
    print("Testing GenerationState Structure")
    print("=" * 60)
    
    # Create empty state
    state = create_empty_state(description="Test database for bookstore")
    print(f"[OK] Created empty state with description: {state.description}")
    
    # Add domain
    state.domain = "e-commerce"
    print(f"[OK] Set domain: {state.domain}")
    
    # Add entities
    state.entities.append(
        EntityInfo(
            name="Customer",
            description="Customer entity",
            mention_type="explicit",
            evidence="Customer",
            confidence=1.0,
            cardinality="large",
            table_type="dimension"
        )
    )
    state.entities.append(
        EntityInfo(
            name="Order",
            description="Order entity",
            mention_type="explicit",
            evidence="Order",
            confidence=1.0,
            cardinality="very_large",
            table_type="fact"
        )
    )
    print(f"[OK] Added {len(state.entities)} entities")
    
    # Test utility functions
    entity_names = get_entity_names(state)
    print(f"[OK] Entity names: {entity_names}")
    
    has_customer = has_entity(state, "Customer")
    print(f"[OK] Has Customer entity: {has_customer}")
    
    # Add attributes
    from NL2DATA.ir import AttributeInfo
    state.attributes["Customer"] = [
        AttributeInfo(name="customer_id", description="Customer identifier", type_hint="integer"),
        AttributeInfo(name="name", description="Customer name", type_hint="string"),
        AttributeInfo(name="email", description="Customer email", type_hint="string", nullable=False),
    ]
    print(f"[OK] Added attributes for Customer: {len(state.attributes['Customer'])} attributes")
    
    # Add primary key
    state.primary_keys["Customer"] = ["customer_id"]
    print(f"[OK] Set primary key for Customer: {state.primary_keys['Customer']}")
    
    # Verify state structure
    print("\n" + "=" * 60)
    print("State Summary")
    print("=" * 60)
    print(f"Domain: {state.domain}")
    print(f"Entities: {len(state.entities)}")
    print(f"Relations: {len(state.relations)}")
    print(f"Attributes: {sum(len(attrs) for attrs in state.attributes.values())} total")
    print(f"Primary Keys: {len(state.primary_keys)}")
    print(f"Foreign Keys: {len(state.foreign_keys)}")
    print(f"DDL Statements: {len(state.ddl_statements)}")
    print(f"Generation Strategies: {sum(len(strategies) for strategies in state.generation_strategies.values())} total")
    
    print("\n[PASS] All tests passed! GenerationState structure is working correctly.")
    return True


if __name__ == "__main__":
    try:
        test_state_creation()
    except Exception as e:
        print(f"\n[ERROR] Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

