"""ER Diagram Compiler - Compiles Pydantic ERDesign models to Graphviz diagrams.

This module provides functionality to convert ERDesign Pydantic models into
Graphviz DOT format and render them as images (SVG, PNG, JPEG).

Features:
- Entity = rectangle
- Relationship = diamond
- Attribute = oval
- Primary key = underlined label
- Derived attribute = dashed oval
- Multivalued attribute = double oval
- Cardinality labels at relationship endpoints
- Total participation = double-line edge
"""

from __future__ import annotations

from typing import Dict, List, Optional, Any
from pathlib import Path
import graphviz
from graphviz import Digraph

from NL2DATA.ir.models.er_relational import (
    ERDesign,
    EREntity,
    ERRelation,
    ERAttribute
)


# ---- Helper functions ----

def _eid(name: str) -> str:
    """Generate entity node ID."""
    # Use underscore instead of :: to avoid Graphviz parsing issues
    return f"E_{name}"


def _rid(name: str) -> str:
    """Generate relationship node ID."""
    # Use underscore instead of :: to avoid Graphviz parsing issues
    return f"R_{name}"


def _aid(owner_id: str, attr_name: str) -> str:
    """Generate attribute node ID."""
    # Use underscore instead of :: to avoid Graphviz parsing issues
    return f"A_{owner_id}_{attr_name}"


def _html_underline(text: str) -> str:
    """Wrap text in HTML underline tags for Graphviz HTML-like labels.
    
    Note: <U> is supported in Graphviz HTML-like labels.
    SVG output is most reliable for underlines; PNG/JPEG may vary.
    """
    return f"<<U>{text}</U>>"


def _attr_node_kwargs(is_derived: bool, is_multivalued: bool) -> Dict[str, str]:
    """Generate Graphviz node attributes for an attribute based on its properties.
    
    Args:
        is_derived: If True, attribute is derived (dashed style)
        is_multivalued: If True, attribute is multivalued (double oval via peripheries=2)
        
    Returns:
        Dictionary of Graphviz node attributes
    """
    kw: Dict[str, str] = {"shape": "ellipse"}
    if is_derived:
        kw["style"] = "dashed"
    if is_multivalued:
        kw["peripheries"] = "2"  # double oval
    return kw


def _edge_kwargs_for_participation(participation: Optional[str]) -> Dict[str, str]:
    """Generate edge attributes for participation type.
    
    Total participation = double line using Graphviz color trick: "black:invis:black"
    Partial participation = single line (default)
    
    Args:
        participation: "total" or "partial" or None
        
    Returns:
        Dictionary of Graphviz edge attributes
    """
    if participation == "total":
        return {"color": "black:invis:black", "penwidth": "1.3"}
    return {}


# ---- Main compiler ----

def erdesign_to_graphviz(design: ERDesign) -> Digraph:
    """Compile ERDesign Pydantic model to Graphviz Digraph.
    
    Args:
        design: ERDesign instance containing entities, relations, and attributes
        
    Returns:
        Graphviz Digraph object ready for rendering
        
    Raises:
        ValueError: If primary keys don't exist in attributes, or if relations
                    reference unknown entities
    """
    g = Digraph(
        "ER",
        graph_attr={
            "rankdir": "LR",
            "splines": "ortho",
            "nodesep": "0.6",
            "ranksep": "0.9",
            "pad": "0.25",
            "dpi": "200",
        },
    )
    g.attr("node", fontname="Helvetica", fontsize="12", margin="0.15,0.08")
    g.attr("edge", fontname="Helvetica", fontsize="11")

    # Index entities for validation/lookup
    entities_by_name = {e.name: e for e in design.entities}

    # --- Entities + their attributes ---
    for ent in design.entities:
        ent_id = _eid(ent.name)
        g.node(ent_id, ent.name, shape="box")

        attr_names = {a.name for a in ent.attributes}
        pk_set = set(ent.primary_key)

        # Validation: PKs must exist as attributes
        missing_pk = pk_set - attr_names
        if missing_pk:
            raise ValueError(
                f"Entity '{ent.name}' primary_key not found in attributes: {sorted(missing_pk)}"
            )

        for attr in ent.attributes:
            _emit_attribute_tree(
                g=g,
                owner_id=ent_id,
                owner_label=ent.name,
                attr=attr,
                is_key=(attr.name in pk_set),
            )

    # --- Relationships + their attributes + links to entities ---
    for rel in design.relations:
        # Pick a relationship name: prefer rel.type; otherwise derive one
        rel_name = rel.type.strip() if rel.type else "REL(" + ",".join(rel.entities) + ")"
        rel_id = _rid(rel_name)

        g.node(rel_id, rel_name, shape="diamond", fixedsize="true", width="1.2", height="0.7")

        # Attach relationship attributes
        for attr in rel.attributes:
            _emit_attribute_tree(
                g=g,
                owner_id=rel_id,
                owner_label=rel_name,
                attr=attr,
                is_key=False,  # Typically no PK notion on relationship attributes in Chen ER
            )

        # Connect each participating entity
        for ent_name in rel.entities:
            if ent_name not in entities_by_name:
                raise ValueError(
                    f"Relation '{rel_name}' references unknown entity '{ent_name}'"
                )

            ent_id = _eid(ent_name)

            card = rel.entity_cardinalities.get(ent_name)  # "1" or "N"
            part = rel.entity_participations.get(ent_name)  # "total" or "partial"

            edge_kw = {"dir": "none"}  # Keep Chen-style undirected look
            edge_kw.update(_edge_kwargs_for_participation(part))

            # Endpoint labels: use taillabel when edge goes Entity -> Relationship
            if card:
                edge_kw["taillabel"] = card
                edge_kw["labeldistance"] = "2.0"
                edge_kw["labelangle"] = "25"
                edge_kw["labelfloat"] = "true"
                edge_kw["minlen"] = "2"

            g.edge(ent_id, rel_id, **edge_kw)

    return g


def _emit_attribute_tree(
    g: Digraph,
    owner_id: str,
    owner_label: str,
    attr: ERAttribute,
    is_key: bool
) -> None:
    """Emit attribute node and edges.
    
    Emits:
      - Attribute node with appropriate styling (derived, multivalued, key)
      - Edge from owner -> attribute
      - If composite: edges attribute -> sub-attributes
    
    Args:
        g: Graphviz Digraph to add nodes/edges to
        owner_id: Node ID of the owner (entity or relationship)
        owner_label: Label of the owner (for error messages)
        attr: ERAttribute instance
        is_key: Whether this attribute is part of the primary key
    """
    attr_id = _aid(owner_id, attr.name)

    label = _html_underline(attr.name) if is_key else attr.name

    kw = _attr_node_kwargs(is_derived=attr.is_derived, is_multivalued=attr.is_multivalued)
    g.node(attr_id, label, **kw)
    # Use constraint=false so attribute edges don't affect entity-relationship layout
    g.edge(owner_id, attr_id, dir="none", constraint="false", minlen="1")

    # Composite: create sub-attributes and connect them to the composite attribute node
    if attr.is_composite:
        for subname in attr.decomposition:
            sub_id = _aid(attr_id, subname)
            g.node(sub_id, subname, shape="ellipse")
            # Use constraint=false for sub-attribute edges too
            g.edge(attr_id, sub_id, dir="none", constraint="false", minlen="1")


# ---- Rendering functions ----

def render_er_diagram(
    design: ERDesign,
    output_path: Optional[str] = None,
    format: str = "svg",
    cleanup: bool = True
) -> bytes:
    """Render ERDesign to image bytes.
    
    Args:
        design: ERDesign instance to render
        output_path: Optional path to save the file (without extension).
                     If None, only returns bytes without saving.
        format: Output format - "svg" (recommended for underlines), "png", "jpg", or "pdf"
        cleanup: If True, remove intermediate .dot file after rendering
        
    Returns:
        Image bytes in the specified format
        
    Raises:
        ValueError: If format is not supported
    """
    g = erdesign_to_graphviz(design)
    
    if output_path:
        # Render to file
        result_path = g.render(output_path, format=format, cleanup=cleanup)
        # Read the rendered file and return bytes
        with open(result_path, "rb") as f:
            return f.read()
    else:
        # Return bytes directly without saving to file
        return g.pipe(format=format)


def render_er_diagram_to_file(
    design: ERDesign,
    output_path: str,
    format: str = "svg",
    cleanup: bool = True
) -> str:
    """Render ERDesign to file and return the file path.
    
    Args:
        design: ERDesign instance to render
        output_path: Path to save the file (without extension)
        format: Output format - "svg" (recommended), "png", "jpg", or "pdf"
        cleanup: If True, remove intermediate .dot file after rendering
        
    Returns:
        Path to the rendered file (with extension)
    """
    g = erdesign_to_graphviz(design)
    return g.render(output_path, format=format, cleanup=cleanup)


def dict_to_erdesign(er_design_dict: Dict[str, Any]) -> ERDesign:
    """Convert dictionary ER design to ERDesign Pydantic model.
    
    Args:
        er_design_dict: Dictionary containing er_design structure from state
        
    Returns:
        ERDesign Pydantic model instance
    """
    entities = []
    for ent_dict in er_design_dict.get("entities", []):
        entity_attrs = []
        for attr_dict in ent_dict.get("attributes", []):
            entity_attrs.append(ERAttribute(**attr_dict))
        
        entities.append(EREntity(
            name=ent_dict.get("name", ""),
            attributes=entity_attrs,
            primary_key=ent_dict.get("primary_key", []),
            description=ent_dict.get("description")
        ))
    
    relations = []
    for rel_dict in er_design_dict.get("relations", []):
        rel_attrs = []
        for attr_dict in rel_dict.get("attributes", []):
            rel_attrs.append(ERAttribute(**attr_dict))
        
        relations.append(ERRelation(
            entities=rel_dict.get("entities", []),
            type=rel_dict.get("type", ""),
            description=rel_dict.get("description"),
            arity=rel_dict.get("arity", 0),
            entity_cardinalities=rel_dict.get("entity_cardinalities", {}),
            entity_participations=rel_dict.get("entity_participations", {}),
            attributes=rel_attrs
        ))
    
    return ERDesign(
        entities=entities,
        relations=relations,
        attributes=er_design_dict.get("attributes", {})  # legacy field
    )


def generate_and_save_er_diagram(
    er_design_dict: Dict[str, Any],
    job_id: str,
    storage_path: str,
    format: str = "svg"
) -> str:
    """Generate ER diagram image from dict and save to storage.
    
    Args:
        er_design_dict: Dictionary containing er_design structure from state
        job_id: Job ID to use as filename
        storage_path: Directory path to save the image
        format: Output format - "svg" (recommended), "png", "jpg", or "pdf"
        
    Returns:
        Relative path to the saved image (for use in API URLs)
        
    Raises:
        ValueError: If er_design_dict is invalid
        OSError: If storage directory cannot be created
    """
    # Ensure storage directory exists (create er_diagrams subdirectory)
    storage_dir = Path(storage_path)
    er_diagrams_dir = storage_dir / "er_diagrams"
    er_diagrams_dir.mkdir(parents=True, exist_ok=True)
    
    # Convert dict to Pydantic model
    design = dict_to_erdesign(er_design_dict)
    
    # Generate filename
    filename = f"er_diagram_{job_id}"
    output_path = str(er_diagrams_dir / filename)
    
    # Render to file
    result_path = render_er_diagram_to_file(design, output_path, format=format, cleanup=True)
    
    # Return relative path for API URL
    # Path will be like: er_diagrams/er_diagram_{job_id}.svg
    relative_path = f"er_diagrams/{Path(result_path).name}"
    return relative_path
