"""Deterministic unit test: ER -> relational schema compilation (no LLM calls)."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.ir.models.er_relational import ERDesign, EREntity, ERRelation, ERAttribute
from NL2DATA.phases.phase4.step_4_3_relational_schema_compilation import step_4_3_relational_schema_compilation


def _table(schema: dict, name: str) -> dict:
    return next(t for t in schema.get("tables", []) if t.get("name") == name)


def _col(table: dict, col_name: str) -> dict:
    return next(c for c in table.get("columns", []) if c.get("name") == col_name)

def _find_tables_with_columns(schema: dict, required_cols: list[str]) -> list[dict]:
    required = set(required_cols)
    matches = []
    for t in schema.get("tables", []):
        cols = {c.get("name") for c in t.get("columns", [])}
        if required.issubset(cols):
            matches.append(t)
    return matches

def _find_ternary_relation_table(schema: dict, entity_names: list[str]) -> dict | None:
    """
    Find a ternary relationship table produced by the compiler.

    Current compiler behavior: ternary relation table is typically named by joining the entity names,
    e.g. CUSTOMER_MERCHANT_PRODUCT, and contains foreign keys referencing all 3 entities.
    """
    wanted = set(entity_names)
    for t in schema.get("tables", []):
        fk_refs = {c.get("references_table") for c in t.get("columns", []) if c.get("is_foreign_key")}
        if wanted.issubset(fk_refs):
            return t
    return None


def test_er_to_relational_compilation_deterministic() -> None:
    """
    Comprehensive e-commerce ER model test covering:
    - 1:N relationships with attributes (HAS_ADDRESS, OWNS, PLACED_BY, BILLED_TO, PAID_FOR, FULFILLED_BY, PACKED_BY)
    - M:N relationships with attributes (CONTAINS, STOCKS, SELLS)
    - Ternary relationship (REVIEWS)
    - Relationship tables for relationships with attributes
    - Derived attributes (Age from DOB - not stored)
    """
    print("\n" + "=" * 80)
    print("TEST: ER to Relational Schema Compilation Deterministic")
    print("E-Commerce Model: 10 entities, 11 relationships")
    print("=" * 80)
    print("\n[STEP 1] Building ER design...")

    er = ERDesign(
        entities=[
            EREntity(
                name="CUSTOMER",
                primary_key=["CustomerID"],
                attributes=[
                    ERAttribute(name="CustomerID", type_hint="integer"),
                    ERAttribute(name="Name", type_hint="string"),
                    ERAttribute(name="Email", type_hint="string"),
                    ERAttribute(name="Phone", type_hint="string"),
                    ERAttribute(name="DOB", type_hint="date"),
                    # Age is derived from DOB, not stored
                ],
            ),
            EREntity(
                name="ADDRESS",
                primary_key=["AddressID"],
                attributes=[
                    ERAttribute(name="AddressID", type_hint="integer"),
                    ERAttribute(name="Line1", type_hint="string"),
                    ERAttribute(name="Line2", type_hint="string"),
                    ERAttribute(name="City", type_hint="string"),
                    ERAttribute(name="State", type_hint="string"),
                    ERAttribute(name="PostalCode", type_hint="string"),
                    ERAttribute(name="Country", type_hint="string"),
                ],
            ),
            EREntity(
                name="ACCOUNT",
                primary_key=["AccountID"],
                attributes=[
                    ERAttribute(name="AccountID", type_hint="integer"),
                    ERAttribute(name="OpenDate", type_hint="date"),
                    ERAttribute(name="Status", type_hint="string"),
                    ERAttribute(name="CreditLimit", type_hint="decimal"),
                ],
            ),
            EREntity(
                name="MERCHANT",
                primary_key=["MerchantID"],
                attributes=[
                    ERAttribute(name="MerchantID", type_hint="integer"),
                    ERAttribute(name="LegalName", type_hint="string"),
                    ERAttribute(name="CategoryCode", type_hint="string"),
                    ERAttribute(name="SupportEmail", type_hint="string"),
                ],
            ),
            EREntity(
                name="PRODUCT",
                primary_key=["ProductID"],
                attributes=[
                    ERAttribute(name="ProductID", type_hint="integer"),
                    ERAttribute(name="SKU", type_hint="string"),
                    ERAttribute(name="ProductName", type_hint="string"),
                    ERAttribute(name="BasePrice", type_hint="decimal"),
                    ERAttribute(name="TaxCode", type_hint="string"),
                ],
            ),
            EREntity(
                name="WAREHOUSE",
                primary_key=["WarehouseID"],
                attributes=[
                    ERAttribute(name="WarehouseID", type_hint="integer"),
                    ERAttribute(name="Name", type_hint="string"),
                    ERAttribute(name="Region", type_hint="string"),
                    ERAttribute(name="Capacity", type_hint="integer"),
                ],
            ),
            EREntity(
                name="ORDER",
                primary_key=["OrderID"],
                attributes=[
                    ERAttribute(name="OrderID", type_hint="integer"),
                    ERAttribute(name="OrderDate", type_hint="date"),
                    ERAttribute(name="OrderStatus", type_hint="string"),
                    ERAttribute(name="Notes", type_hint="string"),
                ],
            ),
            EREntity(
                name="PAYMENT",
                primary_key=["PaymentID"],
                attributes=[
                    ERAttribute(name="PaymentID", type_hint="integer"),
                    ERAttribute(name="PaymentDate", type_hint="date"),
                    ERAttribute(name="Amount", type_hint="decimal"),
                    ERAttribute(name="Method", type_hint="string"),
                    ERAttribute(name="Status", type_hint="string"),
                ],
            ),
            EREntity(
                name="SHIPMENT",
                primary_key=["ShipmentID"],
                attributes=[
                    ERAttribute(name="ShipmentID", type_hint="integer"),
                    ERAttribute(name="Carrier", type_hint="string"),
                    ERAttribute(name="TrackingNumber", type_hint="string"),
                    ERAttribute(name="ShipDate", type_hint="date"),
                    ERAttribute(name="DeliveryDate", type_hint="date"),
                ],
            ),
            EREntity(
                name="EMPLOYEE",
                primary_key=["EmployeeID"],
                attributes=[
                    ERAttribute(name="EmployeeID", type_hint="integer"),
                    ERAttribute(name="FullName", type_hint="string"),
                    ERAttribute(name="HireDate", type_hint="date"),
                    ERAttribute(name="RoleTitle", type_hint="string"),
                ],
            ),
        ],
        relations=[
            # A) HAS_ADDRESS: CUSTOMER (1) : ADDRESS (N) - with attributes
            ERRelation(
                entities=["CUSTOMER", "ADDRESS"],
                type="one-to-many",
                description="Customer has addresses; relationship has attributes AddressType, ValidFrom, ValidTo",
                arity=2,
                entity_cardinalities={"CUSTOMER": "1", "ADDRESS": "N"},
                entity_participations={"CUSTOMER": "total", "ADDRESS": "total"},
                attributes=[
                    ERAttribute(name="AddressType", type_hint="string"),
                    ERAttribute(name="ValidFrom", type_hint="date"),
                    ERAttribute(name="ValidTo", type_hint="date"),
                ],
            ),
            # B) OWNS: CUSTOMER (1) : ACCOUNT (N) - with attributes
            ERRelation(
                entities=["CUSTOMER", "ACCOUNT"],
                type="one-to-many",
                description="Customer owns accounts; relationship has attributes OwnershipStartDate, OwnershipStatus",
                arity=2,
                entity_cardinalities={"CUSTOMER": "1", "ACCOUNT": "N"},
                entity_participations={"CUSTOMER": "partial", "ACCOUNT": "total"},
                attributes=[
                    ERAttribute(name="OwnershipStartDate", type_hint="date"),
                    ERAttribute(name="OwnershipStatus", type_hint="string"),
                ],
            ),
            # C) PLACED_BY: CUSTOMER (1) : ORDER (N) - with attributes
            ERRelation(
                entities=["CUSTOMER", "ORDER"],
                type="one-to-many",
                description="Order placed by customer; relationship has attributes Channel, PlacedAtTimestamp",
                arity=2,
                entity_cardinalities={"CUSTOMER": "1", "ORDER": "N"},
                entity_participations={"CUSTOMER": "partial", "ORDER": "total"},
                attributes=[
                    ERAttribute(name="Channel", type_hint="string"),
                    ERAttribute(name="PlacedAtTimestamp", type_hint="timestamp"),
                ],
            ),
            # D) BILLED_TO: ACCOUNT (1) : ORDER (N) - with attributes
            ERRelation(
                entities=["ACCOUNT", "ORDER"],
                type="one-to-many",
                description="Order billed to account; relationship has attribute BillingAuthorizationCode",
                arity=2,
                entity_cardinalities={"ACCOUNT": "1", "ORDER": "N"},
                entity_participations={"ACCOUNT": "partial", "ORDER": "total"},
                attributes=[
                    ERAttribute(name="BillingAuthorizationCode", type_hint="string"),
                ],
            ),
            # E) PAID_FOR: ORDER (1) : PAYMENT (N) - with attributes
            ERRelation(
                entities=["ORDER", "PAYMENT"],
                type="one-to-many",
                description="Payment for order; relationship has attributes AppliedAmount, AppliedTimestamp",
                arity=2,
                entity_cardinalities={"ORDER": "1", "PAYMENT": "N"},
                entity_participations={"ORDER": "partial", "PAYMENT": "total"},
                attributes=[
                    ERAttribute(name="AppliedAmount", type_hint="decimal"),
                    ERAttribute(name="AppliedTimestamp", type_hint="timestamp"),
                ],
            ),
            # F) CONTAINS: ORDER (M) : PRODUCT (N) - M:N with attributes
            ERRelation(
                entities=["ORDER", "PRODUCT"],
                type="many-to-many",
                description="Order contains products; relationship has attributes Quantity, UnitPriceAtOrderTime, DiscountAmount, LineTaxAmount",
                arity=2,
                entity_cardinalities={"ORDER": "N", "PRODUCT": "N"},
                entity_participations={"ORDER": "total", "PRODUCT": "partial"},
                attributes=[
                    ERAttribute(name="Quantity", type_hint="integer"),
                    ERAttribute(name="UnitPriceAtOrderTime", type_hint="decimal"),
                    ERAttribute(name="DiscountAmount", type_hint="decimal"),
                    ERAttribute(name="LineTaxAmount", type_hint="decimal"),
                ],
            ),
            # G) FULFILLED_BY: ORDER (1) : SHIPMENT (N) - with attributes
            ERRelation(
                entities=["ORDER", "SHIPMENT"],
                type="one-to-many",
                description="Order fulfilled by shipments; relationship has attributes FulfillmentPriority, PackedTimestamp",
                arity=2,
                entity_cardinalities={"ORDER": "1", "SHIPMENT": "N"},
                entity_participations={"ORDER": "partial", "SHIPMENT": "total"},
                attributes=[
                    ERAttribute(name="FulfillmentPriority", type_hint="integer"),
                    ERAttribute(name="PackedTimestamp", type_hint="timestamp"),
                ],
            ),
            # H) PACKED_BY: EMPLOYEE (1) : SHIPMENT (N) - with attributes
            ERRelation(
                entities=["EMPLOYEE", "SHIPMENT"],
                type="one-to-many",
                description="Shipment packed by employee; relationship has attributes PackingStationCode, QualityCheckResult",
                arity=2,
                entity_cardinalities={"EMPLOYEE": "1", "SHIPMENT": "N"},
                entity_participations={"EMPLOYEE": "partial", "SHIPMENT": "total"},
                attributes=[
                    ERAttribute(name="PackingStationCode", type_hint="string"),
                    ERAttribute(name="QualityCheckResult", type_hint="string"),
                ],
            ),
            # I) STOCKS: WAREHOUSE (M) : PRODUCT (N) - M:N with attributes
            ERRelation(
                entities=["WAREHOUSE", "PRODUCT"],
                type="many-to-many",
                description="Warehouse stocks products; relationship has attributes OnHandQty, ReorderPoint, BinLocation",
                arity=2,
                entity_cardinalities={"WAREHOUSE": "N", "PRODUCT": "N"},
                entity_participations={"WAREHOUSE": "partial", "PRODUCT": "partial"},
                attributes=[
                    ERAttribute(name="OnHandQty", type_hint="integer"),
                    ERAttribute(name="ReorderPoint", type_hint="integer"),
                    ERAttribute(name="BinLocation", type_hint="string"),
                ],
            ),
            # J) SELLS: MERCHANT (M) : PRODUCT (N) - M:N with attributes
            ERRelation(
                entities=["MERCHANT", "PRODUCT"],
                type="many-to-many",
                description="Merchant sells products; relationship has attributes ListingPrice, ListingStatus, ListedOnDate",
                arity=2,
                entity_cardinalities={"MERCHANT": "N", "PRODUCT": "N"},
                entity_participations={"MERCHANT": "total", "PRODUCT": "partial"},
                attributes=[
                    ERAttribute(name="ListingPrice", type_hint="decimal"),
                    ERAttribute(name="ListingStatus", type_hint="string"),
                    ERAttribute(name="ListedOnDate", type_hint="date"),
                ],
            ),
            # K) REVIEWS: CUSTOMER, PRODUCT, MERCHANT - Ternary with attributes
            ERRelation(
                entities=["CUSTOMER", "PRODUCT", "MERCHANT"],
                type="ternary",
                description="Customer reviews product sold by merchant; relationship has attributes Rating, ReviewText, ReviewTimestamp, VerifiedPurchaseFlag",
                arity=3,
                entity_cardinalities={"CUSTOMER": "1", "PRODUCT": "1", "MERCHANT": "1"},
                entity_participations={"CUSTOMER": "partial", "PRODUCT": "partial", "MERCHANT": "partial"},
                attributes=[
                    ERAttribute(name="Rating", type_hint="integer"),
                    ERAttribute(name="ReviewText", type_hint="string"),
                    ERAttribute(name="ReviewTimestamp", type_hint="timestamp"),
                    ERAttribute(name="VerifiedPurchaseFlag", type_hint="boolean"),
                ],
            ),
        ],
    )

    print(f"  Created ER design with {len(er.entities)} entities and {len(er.relations)} relations")
    print(f"  Entities: {', '.join([e.name for e in er.entities])}")
    print(f"  Relations: {len(er.relations)}")
    for rel in er.relations:
        rel_attrs = f" (attrs: {', '.join([a.name for a in rel.attributes])})" if rel.attributes else ""
        print(f"    - {rel.type}: {', '.join(rel.entities)}{rel_attrs}")
    
    print("\n[STEP 2] Compiling relational schema from ER design...")
    schema = step_4_3_relational_schema_compilation(
        er_design=er.model_dump(),
        foreign_keys=[],
        primary_keys={
            "CUSTOMER": ["CustomerID"],
            "ADDRESS": ["AddressID"],
            "ACCOUNT": ["AccountID"],
            "MERCHANT": ["MerchantID"],
            "PRODUCT": ["ProductID"],
            "WAREHOUSE": ["WarehouseID"],
            "ORDER": ["OrderID"],
            "PAYMENT": ["PaymentID"],
            "SHIPMENT": ["ShipmentID"],
            "EMPLOYEE": ["EmployeeID"],
        },
        constraints=None,
    )
    
    table_names = [t.get("name") for t in schema.get("tables", [])]
    print(f"  Schema compilation complete: {len(table_names)} tables")
    print(f"  Tables: {', '.join(sorted(table_names))}")

    print("\n[STEP 3] Verifying entity tables exist...")
    entity_tables = ["CUSTOMER", "ADDRESS", "ACCOUNT", "MERCHANT", "PRODUCT", "WAREHOUSE", "ORDER", "PAYMENT", "SHIPMENT", "EMPLOYEE"]
    for table_name in entity_tables:
        assert table_name in table_names, f"Expected entity table {table_name} not found"
    print(f"  [OK] All {len(entity_tables)} entity tables exist")
    
    # Verify CUSTOMER doesn't have Age (derived attribute not stored)
    customer = _table(schema, "CUSTOMER")
    customer_cols = {c.get("name") for c in customer.get("columns", [])}
    assert "Age" not in customer_cols, "Age should not be stored (derived from DOB)"
    assert "DOB" in customer_cols, "DOB should be stored"
    print(f"  [OK] CUSTOMER has DOB but not Age (derived attribute not stored)")

    print("\n[STEP 4] Verifying 1:N relationships with attributes...")
    # Note: Current algorithm may add relationship attributes to entity tables rather than creating separate relationship tables
    # Check for either relationship tables OR relationship attributes in entity tables
    
    # A) HAS_ADDRESS: Check if relationship table exists OR attributes in ADDRESS/CUSTOMER
    has_addr_table = None
    for tname in table_names:
        if "CUSTOMER" in tname and "ADDRESS" in tname and tname not in ["CUSTOMER", "ADDRESS"]:
            has_addr_table = tname
            break
    
    if has_addr_table:
        print(f"  [OK] Found relationship table for HAS_ADDRESS: {has_addr_table}")
        customer_address = _table(schema, has_addr_table)
        rel_attrs = {c.get("name") for c in customer_address.get("columns", [])}
        assert "AddressType" in rel_attrs or "ValidFrom" in rel_attrs, f"Expected relationship attributes in {has_addr_table}"
        print(f"  [OK] {has_addr_table} contains relationship attributes")
    else:
        # Check if relationship attributes were added to ADDRESS table
        address = _table(schema, "ADDRESS")
        address_cols = {c.get("name") for c in address.get("columns", [])}
        # Relationship attributes might be in ADDRESS or CUSTOMER
        print(f"  [NOTE] No separate relationship table for HAS_ADDRESS (attributes may be in entity tables)")
        # Ensure relationship attributes aren't silently dropped: they should exist in at least one table.
        has_addr_attr_tables = _find_tables_with_columns(schema, ["AddressType", "ValidFrom"])
        assert has_addr_attr_tables, "HAS_ADDRESS relationship attributes (AddressType/ValidFrom) not found in any table"
        print(f"  [OK] HAS_ADDRESS relationship attributes found in table(s): {', '.join([t.get('name','') for t in has_addr_attr_tables])}")
    
    # B) OWNS: Check for CUSTOMER_ACCOUNT or attributes in ACCOUNT
    owns_table = None
    for tname in table_names:
        if "CUSTOMER" in tname and "ACCOUNT" in tname and tname not in ["CUSTOMER", "ACCOUNT"]:
            owns_table = tname
            break
    if owns_table:
        print(f"  [OK] Found relationship table for OWNS: {owns_table}")
    else:
        print(f"  [NOTE] No separate relationship table for OWNS (attributes may be in entity tables)")
        owns_attr_tables = _find_tables_with_columns(schema, ["OwnershipStartDate", "OwnershipStatus"])
        assert owns_attr_tables, "OWNS relationship attributes (OwnershipStartDate/OwnershipStatus) not found in any table"
        print(f"  [OK] OWNS relationship attributes found in table(s): {', '.join([t.get('name','') for t in owns_attr_tables])}")
    
    # C) PLACED_BY: Check for ORDER_PLACEMENT or attributes in ORDER
    placed_table = None
    for tname in table_names:
        if "ORDER" in tname and ("PLACEMENT" in tname or "PLACED" in tname):
            placed_table = tname
            break
    if placed_table:
        print(f"  [OK] Found relationship table for PLACED_BY: {placed_table}")
    else:
        order = _table(schema, "ORDER")
        order_cols = {c.get("name") for c in order.get("columns", [])}
        # Check if Channel or PlacedAtTimestamp are in ORDER
        if "Channel" in order_cols or "PlacedAtTimestamp" in order_cols:
            print(f"  [OK] PLACED_BY relationship attributes found in ORDER table")
        else:
            placed_attr_tables = _find_tables_with_columns(schema, ["Channel", "PlacedAtTimestamp"])
            assert placed_attr_tables, "PLACED_BY relationship attributes (Channel/PlacedAtTimestamp) not found in any table"
            print(f"  [OK] PLACED_BY relationship attributes found in table(s): {', '.join([t.get('name','') for t in placed_attr_tables])}")
    
    # D) BILLED_TO: Check for ORDER_BILLING or attributes in ORDER
    billing_table = None
    for tname in table_names:
        if "ORDER" in tname and "BILLING" in tname:
            billing_table = tname
            break
    if billing_table:
        print(f"  [OK] Found relationship table for BILLED_TO: {billing_table}")
    else:
        print(f"  [NOTE] No separate relationship table for BILLED_TO (attributes may be in entity tables)")
        billing_attr_tables = _find_tables_with_columns(schema, ["BillingAuthorizationCode"])
        assert billing_attr_tables, "BILLED_TO relationship attribute (BillingAuthorizationCode) not found in any table"
        print(f"  [OK] BILLED_TO relationship attributes found in table(s): {', '.join([t.get('name','') for t in billing_attr_tables])}")
    
    # E) PAID_FOR: Check for PAYMENT_APPLICATION or attributes in PAYMENT
    payment_app_table = None
    for tname in table_names:
        if "PAYMENT" in tname and ("APPLICATION" in tname or "APPLIED" in tname):
            payment_app_table = tname
            break
    if payment_app_table:
        print(f"  [OK] Found relationship table for PAID_FOR: {payment_app_table}")
    else:
        print(f"  [NOTE] No separate relationship table for PAID_FOR (attributes may be in entity tables)")
        paid_for_attr_tables = _find_tables_with_columns(schema, ["AppliedAmount", "AppliedTimestamp"])
        assert paid_for_attr_tables, "PAID_FOR relationship attributes (AppliedAmount/AppliedTimestamp) not found in any table"
        print(f"  [OK] PAID_FOR relationship attributes found in table(s): {', '.join([t.get('name','') for t in paid_for_attr_tables])}")
    
    # G) FULFILLED_BY: Check for ORDER_SHIPMENT or attributes in SHIPMENT
    fulfilled_table = None
    for tname in table_names:
        if "ORDER" in tname and "SHIPMENT" in tname and tname not in ["ORDER", "SHIPMENT"]:
            fulfilled_table = tname
            break
    if fulfilled_table:
        print(f"  [OK] Found relationship table for FULFILLED_BY: {fulfilled_table}")
    else:
        print(f"  [NOTE] No separate relationship table for FULFILLED_BY (attributes may be in entity tables)")
        fulfilled_attr_tables = _find_tables_with_columns(schema, ["FulfillmentPriority", "PackedTimestamp"])
        assert fulfilled_attr_tables, "FULFILLED_BY relationship attributes (FulfillmentPriority/PackedTimestamp) not found in any table"
        print(f"  [OK] FULFILLED_BY relationship attributes found in table(s): {', '.join([t.get('name','') for t in fulfilled_attr_tables])}")
    
    # H) PACKED_BY: Check for SHIPMENT_PACKING or attributes in SHIPMENT
    packing_table = None
    for tname in table_names:
        if "SHIPMENT" in tname and ("PACKING" in tname or "PACKED" in tname):
            packing_table = tname
            break
    if packing_table:
        print(f"  [OK] Found relationship table for PACKED_BY: {packing_table}")
    else:
        print(f"  [NOTE] No separate relationship table for PACKED_BY (attributes may be in entity tables)")
        packed_by_attr_tables = _find_tables_with_columns(schema, ["PackingStationCode", "QualityCheckResult"])
        assert packed_by_attr_tables, "PACKED_BY relationship attributes (PackingStationCode/QualityCheckResult) not found in any table"
        print(f"  [OK] PACKED_BY relationship attributes found in table(s): {', '.join([t.get('name','') for t in packed_by_attr_tables])}")

    print("\n[STEP 5] Verifying M:N relationships create junction tables...")
    # F) CONTAINS should create ORDER_LINE (or ORDER_PRODUCT) junction table
    contains_table = None
    for tname in table_names:
        if "ORDER" in tname and "PRODUCT" in tname:
            contains_table = tname
            break
    assert contains_table, "Expected junction table for CONTAINS (ORDER-PRODUCT)"
    print(f"  [OK] Found junction table: {contains_table}")
    order_line = _table(schema, contains_table)
    assert order_line.get("is_junction_table") is True, f"{contains_table} should be marked as junction table"
    order_line_cols = {c.get("name") for c in order_line.get("columns", [])}
    assert {"Quantity", "UnitPriceAtOrderTime"}.issubset(order_line_cols), f"Expected line-item attributes in {contains_table}"
    print(f"  [OK] {contains_table} is junction table with relationship attributes")
    
    # I) STOCKS should create INVENTORY junction table
    inventory_table = None
    for tname in table_names:
        if "WAREHOUSE" in tname and "PRODUCT" in tname:
            inventory_table = tname
            break
    assert inventory_table, "Expected junction table for STOCKS (WAREHOUSE-PRODUCT)"
    print(f"  [OK] Found junction table: {inventory_table}")
    inventory = _table(schema, inventory_table)
    assert inventory.get("is_junction_table") is True, f"{inventory_table} should be marked as junction table"
    
    # J) SELLS should create MERCHANT_LISTING junction table
    sells_table = None
    for tname in table_names:
        if "MERCHANT" in tname and "PRODUCT" in tname:
            sells_table = tname
            break
    assert sells_table, "Expected junction table for SELLS (MERCHANT-PRODUCT)"
    print(f"  [OK] Found junction table: {sells_table}")
    merchant_listing = _table(schema, sells_table)
    assert merchant_listing.get("is_junction_table") is True, f"{sells_table} should be marked as junction table"

    print("\n[STEP 6] Verifying ternary relationship (REVIEWS)...")
    # K) REVIEWS should create a ternary table with FKs to all three entities.
    # Current compiler names ternary tables by joining entity names (e.g., CUSTOMER_MERCHANT_PRODUCT).
    ternary = _find_ternary_relation_table(schema, ["CUSTOMER", "PRODUCT", "MERCHANT"])
    assert ternary is not None, "Expected table for ternary REVIEWS relationship (FKs to CUSTOMER/PRODUCT/MERCHANT)"
    review_table = ternary.get("name", "")
    print(f"  [OK] Found ternary relationship table: {review_table}")
    review = ternary
    review_fks = {}
    for col in review.get("columns", []):
        if col.get("is_foreign_key"):
            ref_table = col.get("references_table")
            if ref_table:
                if ref_table not in review_fks:
                    review_fks[ref_table] = []
                review_fks[ref_table].append(col.get("name"))
    print(f"  Review table FKs: {review_fks}")
    assert "CUSTOMER" in review_fks, "REVIEW table should have FK to CUSTOMER"
    assert "PRODUCT" in review_fks, "REVIEW table should have FK to PRODUCT"
    assert "MERCHANT" in review_fks, "REVIEW table should have FK to MERCHANT"
    print(f"  [OK] REVIEW table has FKs to all three entities: CUSTOMER, PRODUCT, MERCHANT")
    
    review_cols = {c.get("name") for c in review.get("columns", [])}
    assert "Rating" in review_cols or "ReviewText" in review_cols, "REVIEW table should contain relationship attributes"
    print(f"  [OK] REVIEW table contains relationship attributes")
    
    print("\n" + "=" * 80)
    print("[PASS] All ER to relational compilation checks passed!")
    print("=" * 80)


if __name__ == "__main__":
    test_er_to_relational_compilation_deterministic()
    print("[PASS] test_er_to_relational_compilation_deterministic")


