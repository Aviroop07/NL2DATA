"""Diagram service - generates ER diagram images."""

from typing import Dict, Any
import graphviz
from io import BytesIO


class DiagramService:
    """Generates ER diagram images."""
    
    async def generate_er_diagram(
        self,
        job_id: str,
        er_state: Dict[str, Any],
        format: str = "png"
    ) -> bytes:
        """Generate ER diagram image from state."""
        dot = graphviz.Digraph(comment="ER Diagram")
        
        # Add entities
        entities = er_state.get("entities", [])
        for entity in entities:
            entity_name = entity.get("name", "")
            dot.node(entity_name, label=entity_name, shape="box")
            
            # Add attributes
            attributes = entity.get("attributes", [])
            for attr in attributes:
                attr_name = attr.get("name", "")
                dot.node(f"{entity_name}_{attr_name}", label=attr_name, shape="ellipse")
                dot.edge(entity_name, f"{entity_name}_{attr_name}")
        
        # Add relations
        relations = er_state.get("relations", [])
        for relation in relations:
            entities_in_relation = relation.get("entities", [])
            if len(entities_in_relation) >= 2:
                dot.edge(
                    entities_in_relation[0],
                    entities_in_relation[1],
                    label=relation.get("name", "")
                )
        
        # Render
        if format == "png":
            return dot.pipe(format="png")
        else:
            return dot.pipe(format="svg")



