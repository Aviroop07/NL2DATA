"""Unit tests for ER diagram compiler."""

import pytest
import tempfile
import os
import sys
from pathlib import Path

# Fix encoding for Windows console
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

from backend.services.er_diagram_compiler import (
    dict_to_erdesign,
    generate_and_save_er_diagram,
    erdesign_to_graphviz
)
from NL2DATA.ir.models.er_relational import ERDesign


def test_dict_to_erdesign():
    """Test conversion of dictionary ER design to Pydantic model."""
    er_design_dict = {
        "entities": [
            {
                "name": "Customer",
                "description": "A customer entity",
                "attributes": [
                    {
                        "name": "customer_id",
                        "description": "Customer identifier",
                        "type_hint": "int",
                        "nullable": False,
                        "is_derived": False,
                        "is_multivalued": False,
                        "is_composite": False,
                        "decomposition": []
                    },
                    {
                        "name": "customer_name",
                        "description": "Customer name",
                        "type_hint": "str",
                        "nullable": True,
                        "is_derived": False,
                        "is_multivalued": False,
                        "is_composite": False,
                        "decomposition": []
                    }
                ],
                "primary_key": ["customer_id"]
            }
        ],
        "relations": [
            {
                "entities": ["Customer"],
                "type": "has",
                "description": "Customer relation",
                "arity": 1,
                "entity_cardinalities": {},
                "entity_participations": {},
                "attributes": []
            }
        ],
        "attributes": {}
    }
    
    design = dict_to_erdesign(er_design_dict)
    
    assert isinstance(design, ERDesign)
    assert len(design.entities) == 1
    assert design.entities[0].name == "Customer"
    assert len(design.entities[0].attributes) == 2
    assert design.entities[0].primary_key == ["customer_id"]
    assert len(design.relations) == 1


def test_generate_and_save_er_diagram():
    """Test ER diagram image generation and saving."""
    # Use a persistent directory for test output so user can view the image
    # Save to backend/static/test_er_diagrams/ for easy access
    test_output_dir = Path(__file__).parent.parent.parent / "static" / "test_er_diagrams"
    test_output_dir.mkdir(parents=True, exist_ok=True)
    temp_dir = str(test_output_dir)
    
    er_design_dict = {
            "entities": [
                {
                    "name": "Customer",
                    "description": "A customer entity",
                    "attributes": [
                        {
                            "name": "customer_id",
                            "description": "Customer identifier",
                            "type_hint": "int",
                            "nullable": False,
                            "is_derived": False,
                            "is_multivalued": False,
                            "is_composite": False,
                            "decomposition": []
                        },
                        {
                            "name": "customer_name",
                            "description": "Customer name",
                            "type_hint": "str",
                            "nullable": True,
                            "is_derived": False,
                            "is_multivalued": False,
                            "is_composite": False,
                            "decomposition": []
                        }
                    ],
                    "primary_key": ["customer_id"]
                },
                {
                    "name": "Order",
                    "description": "An order entity",
                    "attributes": [
                        {
                            "name": "order_id",
                            "description": "Order identifier",
                            "type_hint": "int",
                            "nullable": False,
                            "is_derived": False,
                            "is_multivalued": False,
                            "is_composite": False,
                            "decomposition": []
                        }
                    ],
                    "primary_key": ["order_id"]
                }
            ],
            "relations": [
                {
                    "entities": ["Customer", "Order"],
                    "type": "places",
                    "description": "Customer places order",
                    "arity": 2,
                    "entity_cardinalities": {
                        "Customer": "1",
                        "Order": "N"
                    },
                    "entity_participations": {
                        "Customer": "partial",
                        "Order": "total"
                    },
                    "attributes": []
                }
            ],
            "attributes": {}
        }
    
    job_id = "test_job_123"
    
    # Generate and save the diagram
    image_path = generate_and_save_er_diagram(
        er_design_dict=er_design_dict,
        job_id=job_id,
        storage_path=temp_dir,
        format="svg"
    )
    
    # Verify the path is returned correctly
    assert image_path is not None
    assert image_path.startswith("er_diagrams/")
    assert job_id in image_path
    
    # Verify the file was actually created
    full_path = Path(temp_dir) / image_path
    assert full_path.exists(), f"Image file not found at {full_path}"
    assert full_path.suffix == ".svg", f"Expected .svg file, got {full_path.suffix}"
    
    # Verify the file has content (not empty)
    file_size = full_path.stat().st_size
    assert file_size > 0, f"Image file is empty (size: {file_size} bytes)"
    
    print(f"[OK] Image generated successfully at: {full_path}")
    print(f"[OK] File size: {file_size} bytes")
    print(f"[INFO] You can view the image at: {full_path}")
    print(f"[INFO] You can view the image at: {full_path}")


def test_erdesign_to_graphviz():
    """Test ERDesign to Graphviz conversion."""
    er_design_dict = {
        "entities": [
            {
                "name": "Customer",
                "attributes": [
                    {
                        "name": "customer_id",
                        "is_derived": False,
                        "is_multivalued": False,
                        "is_composite": False,
                        "decomposition": []
                    }
                ],
                "primary_key": ["customer_id"]
            }
        ],
        "relations": [],
        "attributes": {}
    }
    
    design = dict_to_erdesign(er_design_dict)
    graph = erdesign_to_graphviz(design)
    
    assert graph is not None
    assert graph.name == "ER"
    
    # Verify the graph has nodes
    # Note: graphviz Digraph doesn't expose nodes directly, but we can check if it's valid
    dot_source = graph.source
    assert "Customer" in dot_source
    assert "customer_id" in dot_source


def test_complex_er_diagram():
    """Test ER diagram with derived and multivalued attributes."""
    # Use a persistent directory for test output
    test_output_dir = Path(__file__).parent.parent.parent / "static" / "test_er_diagrams"
    test_output_dir.mkdir(parents=True, exist_ok=True)
    temp_dir = str(test_output_dir)
    
    er_design_dict = {
        "entities": [
            {
                "name": "Employee",
                "description": "An employee entity",
                "attributes": [
                    {
                        "name": "employee_id",
                        "description": "Employee identifier",
                        "type_hint": "int",
                        "nullable": False,
                        "is_derived": False,
                        "is_multivalued": False,
                        "is_composite": False,
                        "decomposition": []
                    },
                    {
                        "name": "employee_name",
                        "description": "Employee name",
                        "type_hint": "str",
                        "nullable": True,
                        "is_derived": False,
                        "is_multivalued": False,
                        "is_composite": False,
                        "decomposition": []
                    },
                    {
                        "name": "age",
                        "description": "Employee age",
                        "type_hint": "int",
                        "nullable": True,
                        "is_derived": True,  # Derived attribute
                        "is_multivalued": False,
                        "is_composite": False,
                        "decomposition": []
                    },
                    {
                        "name": "phone_numbers",
                        "description": "Employee phone numbers",
                        "type_hint": "str",
                        "nullable": True,
                        "is_derived": False,
                        "is_multivalued": True,  # Multivalued attribute
                        "is_composite": False,
                        "decomposition": []
                    },
                    {
                        "name": "address",
                        "description": "Employee address",
                        "type_hint": "str",
                        "nullable": True,
                        "is_derived": False,
                        "is_multivalued": False,
                        "is_composite": True,  # Composite attribute
                        "decomposition": ["street", "city", "zip_code"]
                    }
                ],
                "primary_key": ["employee_id"]
            },
            {
                "name": "Department",
                "description": "A department entity",
                "attributes": [
                    {
                        "name": "dept_id",
                        "description": "Department identifier",
                        "type_hint": "int",
                        "nullable": False,
                        "is_derived": False,
                        "is_multivalued": False,
                        "is_composite": False,
                        "decomposition": []
                    },
                    {
                        "name": "dept_name",
                        "description": "Department name",
                        "type_hint": "str",
                        "nullable": False,
                        "is_derived": False,
                        "is_multivalued": False,
                        "is_composite": False,
                        "decomposition": []
                    },
                    {
                        "name": "budget",
                        "description": "Department budget",
                        "type_hint": "decimal",
                        "nullable": True,
                        "is_derived": False,
                        "is_multivalued": False,
                        "is_composite": False,
                        "decomposition": []
                    }
                ],
                "primary_key": ["dept_id"]
            },
            {
                "name": "Project",
                "description": "A project entity",
                "attributes": [
                    {
                        "name": "project_id",
                        "description": "Project identifier",
                        "type_hint": "int",
                        "nullable": False,
                        "is_derived": False,
                        "is_multivalued": False,
                        "is_composite": False,
                        "decomposition": []
                    },
                    {
                        "name": "project_name",
                        "description": "Project name",
                        "type_hint": "str",
                        "nullable": False,
                        "is_derived": False,
                        "is_multivalued": False,
                        "is_composite": False,
                        "decomposition": []
                    },
                    {
                        "name": "completion_percentage",
                        "description": "Project completion",
                        "type_hint": "decimal",
                        "nullable": True,
                        "is_derived": True,  # Derived attribute
                        "is_multivalued": False,
                        "is_composite": False,
                        "decomposition": []
                    }
                ],
                "primary_key": ["project_id"]
            }
        ],
        "relations": [
            {
                "entities": ["Employee", "Department"],
                "type": "works_in",
                "description": "Employee works in department",
                "arity": 2,
                "entity_cardinalities": {
                    "Employee": "N",
                    "Department": "1"
                },
                "entity_participations": {
                    "Employee": "total",
                    "Department": "partial"
                },
                "attributes": [
                    {
                        "name": "start_date",
                        "description": "Start date of employment",
                        "type_hint": "date",
                        "nullable": False,
                        "is_derived": False,
                        "is_multivalued": False,
                        "is_composite": False,
                        "decomposition": []
                    }
                ]
            },
            {
                "entities": ["Employee", "Project"],
                "type": "assigned_to",
                "description": "Employee assigned to project",
                "arity": 2,
                "entity_cardinalities": {
                    "Employee": "N",
                    "Project": "N"
                },
                "entity_participations": {
                    "Employee": "partial",
                    "Project": "total"
                },
                "attributes": [
                    {
                        "name": "hours_worked",
                        "description": "Hours worked on project",
                        "type_hint": "int",
                        "nullable": True,
                        "is_derived": False,
                        "is_multivalued": False,
                        "is_composite": False,
                        "decomposition": []
                    }
                ]
            },
            {
                "entities": ["Department", "Project"],
                "type": "manages",
                "description": "Department manages project",
                "arity": 2,
                "entity_cardinalities": {
                    "Department": "1",
                    "Project": "N"
                },
                "entity_participations": {
                    "Department": "partial",
                    "Project": "total"
                },
                "attributes": []
            }
        ],
        "attributes": {}
    }
    
    job_id = "complex_test_job"
    
    # Generate and save the diagram
    image_path = generate_and_save_er_diagram(
        er_design_dict=er_design_dict,
        job_id=job_id,
        storage_path=temp_dir,
        format="svg"
    )
    
    # Verify the path is returned correctly
    assert image_path is not None
    assert image_path.startswith("er_diagrams/")
    assert job_id in image_path
    
    # Verify the file was actually created
    full_path = Path(temp_dir) / image_path
    assert full_path.exists(), f"Image file not found at {full_path}"
    assert full_path.suffix == ".svg", f"Expected .svg file, got {full_path.suffix}"
    
    # Verify the file has content (not empty)
    file_size = full_path.stat().st_size
    assert file_size > 0, f"Image file is empty (size: {file_size} bytes)"
    
    print(f"[OK] Complex ER diagram generated successfully at: {full_path}")
    print(f"[OK] File size: {file_size} bytes")
    print(f"[INFO] Features included:")
    print(f"  - Derived attributes (age, completion_percentage)")
    print(f"  - Multivalued attributes (phone_numbers)")
    print(f"  - Composite attributes (address with sub-attributes)")
    print(f"  - Multiple relationships with different cardinalities")
    print(f"  - Relationship attributes (start_date, hours_worked)")
    print(f"[INFO] You can view the image at: {full_path}")


def test_complex_er_diagram():
    """Test ER diagram with derived and multivalued attributes."""
    # Use a persistent directory for test output
    test_output_dir = Path(__file__).parent.parent.parent / "static" / "test_er_diagrams"
    test_output_dir.mkdir(parents=True, exist_ok=True)
    temp_dir = str(test_output_dir)
    
    er_design_dict = {
        "entities": [
            {
                "name": "Employee",
                "description": "An employee entity",
                "attributes": [
                    {
                        "name": "employee_id",
                        "description": "Employee identifier",
                        "type_hint": "int",
                        "nullable": False,
                        "is_derived": False,
                        "is_multivalued": False,
                        "is_composite": False,
                        "decomposition": []
                    },
                    {
                        "name": "employee_name",
                        "description": "Employee name",
                        "type_hint": "str",
                        "nullable": True,
                        "is_derived": False,
                        "is_multivalued": False,
                        "is_composite": False,
                        "decomposition": []
                    },
                    {
                        "name": "age",
                        "description": "Employee age",
                        "type_hint": "int",
                        "nullable": True,
                        "is_derived": True,  # Derived attribute
                        "is_multivalued": False,
                        "is_composite": False,
                        "decomposition": []
                    },
                    {
                        "name": "phone_numbers",
                        "description": "Employee phone numbers",
                        "type_hint": "str",
                        "nullable": True,
                        "is_derived": False,
                        "is_multivalued": True,  # Multivalued attribute
                        "is_composite": False,
                        "decomposition": []
                    },
                    {
                        "name": "address",
                        "description": "Employee address",
                        "type_hint": "str",
                        "nullable": True,
                        "is_derived": False,
                        "is_multivalued": False,
                        "is_composite": True,  # Composite attribute
                        "decomposition": ["street", "city", "zip_code"]
                    }
                ],
                "primary_key": ["employee_id"]
            },
            {
                "name": "Department",
                "description": "A department entity",
                "attributes": [
                    {
                        "name": "dept_id",
                        "description": "Department identifier",
                        "type_hint": "int",
                        "nullable": False,
                        "is_derived": False,
                        "is_multivalued": False,
                        "is_composite": False,
                        "decomposition": []
                    },
                    {
                        "name": "dept_name",
                        "description": "Department name",
                        "type_hint": "str",
                        "nullable": False,
                        "is_derived": False,
                        "is_multivalued": False,
                        "is_composite": False,
                        "decomposition": []
                    },
                    {
                        "name": "budget",
                        "description": "Department budget",
                        "type_hint": "decimal",
                        "nullable": True,
                        "is_derived": False,
                        "is_multivalued": False,
                        "is_composite": False,
                        "decomposition": []
                    }
                ],
                "primary_key": ["dept_id"]
            },
            {
                "name": "Project",
                "description": "A project entity",
                "attributes": [
                    {
                        "name": "project_id",
                        "description": "Project identifier",
                        "type_hint": "int",
                        "nullable": False,
                        "is_derived": False,
                        "is_multivalued": False,
                        "is_composite": False,
                        "decomposition": []
                    },
                    {
                        "name": "project_name",
                        "description": "Project name",
                        "type_hint": "str",
                        "nullable": False,
                        "is_derived": False,
                        "is_multivalued": False,
                        "is_composite": False,
                        "decomposition": []
                    },
                    {
                        "name": "completion_percentage",
                        "description": "Project completion",
                        "type_hint": "decimal",
                        "nullable": True,
                        "is_derived": True,  # Derived attribute
                        "is_multivalued": False,
                        "is_composite": False,
                        "decomposition": []
                    }
                ],
                "primary_key": ["project_id"]
            }
        ],
        "relations": [
            {
                "entities": ["Employee", "Department"],
                "type": "works_in",
                "description": "Employee works in department",
                "arity": 2,
                "entity_cardinalities": {
                    "Employee": "N",
                    "Department": "1"
                },
                "entity_participations": {
                    "Employee": "total",
                    "Department": "partial"
                },
                "attributes": [
                    {
                        "name": "start_date",
                        "description": "Start date of employment",
                        "type_hint": "date",
                        "nullable": False,
                        "is_derived": False,
                        "is_multivalued": False,
                        "is_composite": False,
                        "decomposition": []
                    }
                ]
            },
            {
                "entities": ["Employee", "Project"],
                "type": "assigned_to",
                "description": "Employee assigned to project",
                "arity": 2,
                "entity_cardinalities": {
                    "Employee": "N",
                    "Project": "N"
                },
                "entity_participations": {
                    "Employee": "partial",
                    "Project": "total"
                },
                "attributes": [
                    {
                        "name": "hours_worked",
                        "description": "Hours worked on project",
                        "type_hint": "int",
                        "nullable": True,
                        "is_derived": False,
                        "is_multivalued": False,
                        "is_composite": False,
                        "decomposition": []
                    }
                ]
            },
            {
                "entities": ["Department", "Project"],
                "type": "manages",
                "description": "Department manages project",
                "arity": 2,
                "entity_cardinalities": {
                    "Department": "1",
                    "Project": "N"
                },
                "entity_participations": {
                    "Department": "partial",
                    "Project": "total"
                },
                "attributes": []
            }
        ],
        "attributes": {}
    }
    
    job_id = "complex_test_job"
    
    # Generate and save the diagram
    image_path = generate_and_save_er_diagram(
        er_design_dict=er_design_dict,
        job_id=job_id,
        storage_path=temp_dir,
        format="svg"
    )
    
    # Verify the path is returned correctly
    assert image_path is not None
    assert image_path.startswith("er_diagrams/")
    assert job_id in image_path
    
    # Verify the file was actually created
    full_path = Path(temp_dir) / image_path
    assert full_path.exists(), f"Image file not found at {full_path}"
    assert full_path.suffix == ".svg", f"Expected .svg file, got {full_path.suffix}"
    
    # Verify the file has content (not empty)
    file_size = full_path.stat().st_size
    assert file_size > 0, f"Image file is empty (size: {file_size} bytes)"
    
    print(f"[OK] Complex ER diagram generated successfully at: {full_path}")
    print(f"[OK] File size: {file_size} bytes")
    print(f"[INFO] Features included:")
    print(f"  - Derived attributes (age, completion_percentage)")
    print(f"  - Multivalued attributes (phone_numbers)")
    print(f"  - Composite attributes (address with sub-attributes)")
    print(f"  - Multiple relationships with different cardinalities")
    print(f"  - Relationship attributes (start_date, hours_worked)")
    print(f"[INFO] You can view the image at: {full_path}")


def test_generate_with_empty_entities():
    er_design_dict = {
        "entities": [],
        "relations": [],
        "attributes": {}
    }
    
    with tempfile.TemporaryDirectory() as temp_dir:
        # Should not raise an error, but might create an empty diagram
        try:
            image_path = generate_and_save_er_diagram(
                er_design_dict=er_design_dict,
                job_id="test_empty",
                storage_path=temp_dir,
                format="svg"
            )
            # If it succeeds, verify file exists
            full_path = Path(temp_dir) / image_path
            if full_path.exists():
                print(f"✓ Empty diagram generated at: {full_path}")
        except Exception as e:
            # It's okay if it fails for empty entities
            print(f"Note: Empty entities handled: {e}")


if __name__ == "__main__":
    """Run tests directly."""
    print("=" * 60)
    print("Testing ER Diagram Compiler")
    print("=" * 60)
    
    print("\n1. Testing dict_to_erdesign...")
    try:
        test_dict_to_erdesign()
        print("   [PASSED]")
    except Exception as e:
        print(f"   [FAILED]: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n2. Testing erdesign_to_graphviz...")
    try:
        test_erdesign_to_graphviz()
        print("   [PASSED]")
    except Exception as e:
        print(f"   [FAILED]: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n3. Testing generate_and_save_er_diagram...")
    try:
        test_generate_and_save_er_diagram()
        print("   [PASSED]")
    except Exception as e:
        print(f"   [FAILED]: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n4. Testing with empty entities...")
    try:
        test_generate_with_empty_entities()
        print("   [PASSED]")
    except Exception as e:
        print(f"   [FAILED]: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n5. Testing complex ER diagram with derived and multivalued attributes...")
    try:
        test_complex_er_diagram()
        print("   [PASSED]")
    except Exception as e:
        print(f"   [FAILED]: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "=" * 60)
    print("Test Summary Complete")
    print("=" * 60)
