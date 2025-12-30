"""Deterministic unit test: 3NF normalization (no LLM calls).

Complex university enrollment schema test:
- Single universal relation U (denormalized enrollment reporting table)
- 13 functional dependencies covering students, departments, instructors, courses, sections, rooms, timeslots
- Tests transitive dependencies (e.g., RoomID -> Building -> Campus)
- Tests composite keys (StudentID, SectionID)
- Verifies correct 3NF decomposition into normalized tables
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase4.step_4_2_3nf_normalization import step_4_2_3nf_normalization


def _table(schema: dict, name: str) -> dict:
    return next(t for t in schema.get("normalized_tables", []) if t.get("name") == name)


def test_3nf_normalization_deterministic() -> None:
    print("\n" + "=" * 80)
    print("TEST: 3NF Normalization Deterministic")
    print("University Enrollment Schema: Universal Relation U with 13 FDs")
    print("=" * 80)
    print("\n[STEP 1] Setting up universal relation U (denormalized)...")
    
    # Universal relation U with all attributes
    relational_schema = {
        "tables": [
            {
                "name": "U",
                "columns": [
                    # Student attributes
                    {"name": "StudentID", "type_hint": "integer", "nullable": False},
                    {"name": "StudentName", "type_hint": "string"},
                    {"name": "StudentEmail", "type_hint": "string"},
                    {"name": "StudentDOB", "type_hint": "date"},
                    {"name": "MajorDeptID", "type_hint": "integer"},
                    {"name": "MajorDeptName", "type_hint": "string"},
                    {"name": "MajorDeptOfficePhone", "type_hint": "string"},
                    {"name": "MajorChairInstructorID", "type_hint": "integer"},
                    {"name": "AdvisorInstructorID", "type_hint": "integer"},
                    {"name": "AdvisorName", "type_hint": "string"},
                    {"name": "AdvisorEmail", "type_hint": "string"},
                    # Instructor attributes
                    {"name": "InstructorID", "type_hint": "integer"},
                    {"name": "InstructorName", "type_hint": "string"},
                    {"name": "InstructorEmail", "type_hint": "string"},
                    {"name": "InstructorDeptID", "type_hint": "integer"},
                    {"name": "InstructorDeptName", "type_hint": "string"},
                    {"name": "InstructorDeptOfficePhone", "type_hint": "string"},
                    # Course attributes
                    {"name": "CourseID", "type_hint": "integer"},
                    {"name": "CourseTitle", "type_hint": "string"},
                    {"name": "Credits", "type_hint": "integer"},
                    {"name": "CourseDeptID", "type_hint": "integer"},
                    {"name": "CourseDeptName", "type_hint": "string"},
                    {"name": "CourseDeptOfficePhone", "type_hint": "string"},
                    # Section attributes
                    {"name": "SectionID", "type_hint": "integer", "nullable": False},
                    {"name": "Term", "type_hint": "string"},
                    {"name": "Year", "type_hint": "integer"},
                    {"name": "SectionNo", "type_hint": "integer"},
                    # Room attributes
                    {"name": "RoomID", "type_hint": "integer"},
                    {"name": "Building", "type_hint": "string"},
                    {"name": "Campus", "type_hint": "string"},
                    {"name": "RoomNo", "type_hint": "string"},
                    {"name": "Capacity", "type_hint": "integer"},
                    # TimeSlot attributes
                    {"name": "TimeSlotID", "type_hint": "integer"},
                    {"name": "DayPattern", "type_hint": "string"},
                    {"name": "StartTime", "type_hint": "time"},
                    {"name": "EndTime", "type_hint": "time"},
                    # Enrollment attributes
                    {"name": "EnrollDate", "type_hint": "date"},
                    {"name": "Grade", "type_hint": "string"},
                ],
                "primary_key": ["StudentID", "SectionID"],  # Composite key
                "foreign_keys": [],
            },
        ]
    }

    print(f"  Created universal relation U with {len(relational_schema['tables'][0]['columns'])} attributes")
    print(f"  Primary key: {relational_schema['tables'][0]['primary_key']}")
    
    print("\n[STEP 2] Setting up 13 functional dependencies...")
    functional_dependencies = {
        "U": [
            # Student facts
            {"lhs": ["StudentID"], "rhs": ["StudentName", "StudentEmail", "StudentDOB", "MajorDeptID", "AdvisorInstructorID"]},
            {"lhs": ["MajorDeptID"], "rhs": ["MajorDeptName", "MajorDeptOfficePhone", "MajorChairInstructorID"]},
            # Instructor / department facts
            {"lhs": ["InstructorID"], "rhs": ["InstructorName", "InstructorEmail", "InstructorDeptID"]},
            {"lhs": ["InstructorDeptID"], "rhs": ["InstructorDeptName", "InstructorDeptOfficePhone"]},
            # Course facts
            {"lhs": ["CourseID"], "rhs": ["CourseTitle", "Credits", "CourseDeptID"]},
            {"lhs": ["CourseDeptID"], "rhs": ["CourseDeptName", "CourseDeptOfficePhone"]},
            # Section facts
            {"lhs": ["SectionID"], "rhs": ["CourseID", "Term", "Year", "SectionNo", "InstructorID", "RoomID", "TimeSlotID"]},
            {"lhs": ["CourseID", "Term", "Year", "SectionNo"], "rhs": ["SectionID"]},  # Alternate key
            # Room / building facts
            {"lhs": ["RoomID"], "rhs": ["Building", "RoomNo", "Capacity"]},
            {"lhs": ["Building"], "rhs": ["Campus"]},  # Transitive dependency
            # Time slot facts
            {"lhs": ["TimeSlotID"], "rhs": ["DayPattern", "StartTime", "EndTime"]},
            # Enrollment facts
            {"lhs": ["StudentID", "SectionID"], "rhs": ["EnrollDate", "Grade"]},
            # Advisor facts
            {"lhs": ["AdvisorInstructorID"], "rhs": ["AdvisorName", "AdvisorEmail"]},
        ],
    }

    print(f"  Total FDs: {len(functional_dependencies['U'])}")
    for i, fd in enumerate(functional_dependencies["U"], 1):
        print(f"    {i}. {', '.join(fd['lhs'])} -> {', '.join(fd['rhs'])}")
    
    print("\n[STEP 3] Running 3NF normalization...")
    normalized = step_4_2_3nf_normalization(
        relational_schema=relational_schema,
        functional_dependencies=functional_dependencies,
        entity_unique_constraints=None,
    )
    
    normalized_table_names = [t.get("name") for t in normalized.get("normalized_tables", [])]
    print(f"  Normalization complete: {len(normalized_table_names)} tables")
    print(f"  Tables: {', '.join(sorted(normalized_table_names))}")
    
    decomposition_steps = normalized.get("decomposition_steps", [])
    if decomposition_steps:
        print(f"  Decomposition steps: {len(decomposition_steps)}")
        for i, step in enumerate(decomposition_steps[:5], 1):  # Show first 5
            # Windows console compatibility: replace unicode arrows if present
            step_str = str(step).replace("\u2192", "->")
            print(f"    {i}. {step_str}")
        if len(decomposition_steps) > 5:
            print(f"    ... and {len(decomposition_steps) - 5} more")

    print("\n[STEP 4] Verifying decomposed tables produced by the current algorithm...")
    # Current algorithm names decomposed tables as: f"{table_name}_{sorted(lhs)}"
    # For U, we expect U_StudentID, U_SectionID, etc.

    expected_decompositions = {
        "U_StudentID": ({"StudentID"}, {"StudentName", "StudentEmail", "StudentDOB", "MajorDeptID", "AdvisorInstructorID"}),
        "U_MajorDeptID": ({"MajorDeptID"}, {"MajorDeptName", "MajorDeptOfficePhone", "MajorChairInstructorID"}),
        "U_InstructorID": ({"InstructorID"}, {"InstructorName", "InstructorEmail", "InstructorDeptID"}),
        "U_InstructorDeptID": ({"InstructorDeptID"}, {"InstructorDeptName", "InstructorDeptOfficePhone"}),
        "U_CourseID": ({"CourseID"}, {"CourseTitle", "Credits", "CourseDeptID"}),
        "U_CourseDeptID": ({"CourseDeptID"}, {"CourseDeptName", "CourseDeptOfficePhone"}),
        "U_SectionID": ({"SectionID"}, {"CourseID", "Term", "Year", "SectionNo", "InstructorID", "RoomID", "TimeSlotID"}),
        "U_RoomID": ({"RoomID"}, {"Building", "RoomNo", "Capacity"}),
        "U_Building": ({"Building"}, {"Campus"}),
        "U_TimeSlotID": ({"TimeSlotID"}, {"DayPattern", "StartTime", "EndTime"}),
        "U_AdvisorInstructorID": ({"AdvisorInstructorID"}, {"AdvisorName", "AdvisorEmail"}),
    }

    for table_name, (lhs_set, rhs_set) in expected_decompositions.items():
        assert table_name in normalized_table_names, f"Expected decomposition table {table_name} not found"
        t = _table(normalized, table_name)
        pk = set(t.get("primary_key", []))
        assert lhs_set.issubset(pk), f"{table_name} PK should include {lhs_set}, got {pk}"
        cols = {c.get("name") for c in t.get("columns", [])}
        assert lhs_set.issubset(cols), f"{table_name} should contain determinant columns {lhs_set}"
        assert rhs_set.issubset(cols), f"{table_name} should contain dependent columns {rhs_set}"
        print(f"  [OK] {table_name}: PK includes {sorted(lhs_set)}, contains {len(cols)} columns")

    # Enrollment facts should remain associated with the composite key in the remaining U table
    assert "U" in normalized_table_names, "Expected remaining U table to exist"
    u_tbl = _table(normalized, "U")
    u_pk = set(u_tbl.get("primary_key", []))
    assert u_pk == {"StudentID", "SectionID"}, f"U should keep composite PK (StudentID, SectionID), got {u_pk}"
    u_cols = {c.get("name") for c in u_tbl.get("columns", [])}
    assert {"EnrollDate", "Grade"}.issubset(u_cols), f"U should keep enrollment attributes EnrollDate/Grade, got {u_cols}"
    print(f"  [OK] U table retains composite PK and enrollment attributes")
    
    print("\n[STEP 5] Verifying transitive dependency Building -> Campus is decomposed...")
    building_tbl = _table(normalized, "U_Building")
    building_cols = {c.get("name") for c in building_tbl.get("columns", [])}
    assert {"Building", "Campus"}.issubset(building_cols), f"U_Building should contain Building and Campus, got {building_cols}"
    print(f"  [OK] U_Building contains Building and Campus")
    
    print("\n" + "=" * 80)
    print("[PASS] All 3NF normalization checks passed!")
    print(f"Decomposed {len(relational_schema['tables'])} table(s) into {len(normalized_table_names)} normalized tables")
    print("=" * 80)


if __name__ == "__main__":
    test_3nf_normalization_deterministic()
    print("[PASS] test_3nf_normalization_deterministic")


