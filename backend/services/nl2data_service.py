"""NL2DATA pipeline service wrapper."""

import logging
import time
from typing import Dict, Any, Optional
from NL2DATA.orchestration.graphs.master import get_phase_graph
from NL2DATA.orchestration.state import create_initial_state, IRGenerationState

from backend.utils.job_manager import JobManager

logger = logging.getLogger(__name__)


class NL2DataService:
    """Wraps NL2DATA pipeline for checkpoint-based execution via HTTP."""
    
    def __init__(
        self,
        job_manager: JobManager
    ):
        self.job_manager = job_manager
    
    # DEPRECATED: This method used WebSocket for pipeline events.
    # Pipeline now works via HTTP checkpoints only (execute_to_checkpoint).
    # This method is kept for reference but is NOT USED and will fail if called
    # because WebSocket dependencies have been removed.
    async def process_with_events(
        self,
        job_id: str,
        nl_description: str,
        ws_manager: Any,  # WebSocketManager - not used anymore, kept for signature compatibility
        job_manager: JobManager,
        max_phase: int = None
    ):
        """
        DEPRECATED: This method is not used. Pipeline now uses HTTP checkpoints.
        
        Original purpose: Run pipeline and emit WebSocket events for each step.
        """
        raise NotImplementedError(
            "process_with_events is deprecated. Use execute_to_checkpoint for HTTP-based checkpoint execution."
        )
        pipeline_start_time = time.time()
        
        logger.info("=" * 80)
        logger.info("NL2DATA SERVICE: Starting pipeline processing")
        logger.info("=" * 80)
        logger.info(f"Job ID: {job_id}")
        logger.info(f"NL Description Length: {len(nl_description)} characters")
        logger.info(f"NL Description Preview: {nl_description[:100]}...")
        logger.debug(f"Full NL Description: {nl_description}")
        
        try:
            # Update job status
            logger.info("Updating job status to 'in_progress'...")
            job_manager.update_job(job_id, status="in_progress")
            logger.info("Job status updated")
            
            # Initialize LangGraph state (TypedDict-based, includes current_step)
            state: IRGenerationState = create_initial_state(nl_description)
            logger.info("Initial IRGenerationState created")
            logger.debug(f"Initial IRGenerationState keys: {list(state.keys())}")

            # Stream per-phase graphs so we can emit a tick per substep
            config = {"configurable": {"thread_id": job_id}}
            logger.info("Starting per-phase streaming with config...")
            logger.debug(f"Config: {config}")

            state_update_count = 0
            last_step_sent: str | None = None
            last_state_for_step: IRGenerationState | None = None
            last_phase: int | None = None

            max_phase_to_run = max_phase if max_phase else 13
            for phase in range(1, max_phase_to_run + 1):
                logger.info("=" * 80)
                logger.info(f"STREAMING PHASE {phase}")
                logger.info("=" * 80)

                phase_graph = get_phase_graph(phase)
                phase_last_state: IRGenerationState | None = None

                # Prefer stream_mode="values" (full state per node). If not supported, fall back to updates.
                try:
                    async for full_state in phase_graph.astream(state, config=config, stream_mode="values"):
                        if isinstance(full_state, dict):
                            phase_last_state = full_state  # type: ignore[assignment]
                        else:
                            # Unexpected, but don't crash the stream
                            continue

                        step = phase_last_state.get("current_step") or ""
                        if not step:
                            continue

                        # Avoid spamming duplicates if a node yields multiple times without step change
                        if step == last_step_sent:
                            continue
                        
                        prev_step = last_step_sent
                        state_update_count += 1
                        
                        # WebSocket events removed - pipeline uses HTTP checkpoints only
                        # If there was a previous step, send completion for it
                        # if last_state_for_step is not None and prev_step:
                        #     await self.status_ticker.send_step_complete(...)
                        # # Send start for current step
                        # await self.status_ticker.send_step_start(...)
                        last_step_sent = step

                        logger.info("-" * 80)
                        logger.info(f"STEP UPDATE #{state_update_count}: Phase {phase} Step {step}")

                        # Update job state
                        job_manager.update_job_state(job_id, phase_last_state)
                        job_manager.update_job(job_id, phase=phase, step=step)
                        last_state_for_step = phase_last_state
                        last_phase = phase

                        # WebSocket status tick removed - pipeline uses HTTP checkpoints only
                        # await self.status_ticker.send_tick(...)
                except TypeError:
                    # Fallback for older LangGraph versions / different signature: stream_mode may not exist
                    async for event in phase_graph.astream(state, config=config):
                        if not isinstance(event, dict) or not event:
                            continue
                        node_name = list(event.keys())[-1]
                        update = event.get(node_name)
                        if not isinstance(update, dict):
                            continue

                        # Apply update to state (shallow merge; LangGraph merge semantics are handled by the graph itself)
                        state.update(update)  # type: ignore[arg-type]
                        phase_last_state = state

                        step = state.get("current_step") or ""
                        if not step or step == last_step_sent:
                            continue

                        prev_step = last_step_sent
                        state_update_count += 1
                        
                        # WebSocket events removed - pipeline uses HTTP checkpoints only
                        # if last_state_for_step is not None and prev_step:
                        #     await self.status_ticker.send_step_complete(...)
                        # await self.status_ticker.send_step_start(...)
                        last_step_sent = step

                        logger.info("-" * 80)
                        logger.info(f"STEP UPDATE #{state_update_count}: Phase {phase} Step {step} (node: {node_name})")

                        job_manager.update_job_state(job_id, state)
                        job_manager.update_job(job_id, phase=phase, step=step)
                        last_state_for_step = state
                        last_phase = phase

                        # WebSocket status tick removed - pipeline uses HTTP checkpoints only
                        # await self.status_ticker.send_tick(...)

                # Carry state forward to next phase
                if phase_last_state is not None:
                    state = phase_last_state
                # Update phase marker for UX/logging (phase graphs don't always set it)
                state["phase"] = min(phase + 1, 13)

            # Final state is whatever state we ended with after phase streaming
            final_state_dict = state
            # WebSocket completion event removed - pipeline uses HTTP checkpoints only
            # if last_state_for_step is not None and last_step_sent:
            #     await self.status_ticker.send_step_complete(...)
            logger.info("Updating job to 'completed' status...")
            job_manager.update_job(
                job_id,
                status="completed",
                state=final_state_dict
            )
            
            elapsed_time = time.time() - pipeline_start_time
            logger.info("=" * 80)
            logger.info("NL2DATA SERVICE: Pipeline processing completed successfully")
            logger.info(f"Total processing time: {elapsed_time:.2f} seconds")
            logger.info(f"Step updates emitted: {state_update_count}")
            logger.info("=" * 80)
            
        except Exception as e:
            elapsed_time = time.time() - pipeline_start_time
            logger.error("=" * 80)
            logger.error("NL2DATA SERVICE: Pipeline processing FAILED")
            logger.error("=" * 80)
            logger.error(f"Exception type: {type(e).__name__}")
            logger.error(f"Exception message: {str(e)}")
            logger.error(f"Processing time before failure: {elapsed_time:.2f} seconds")
            logger.exception("Full traceback:")
            
            # Mark as failed
            logger.info("Marking job as 'failed' in JobManager...")
            job_manager.update_job(job_id, status="failed", error=str(e))
            
            # WebSocket error event removed - errors are returned via HTTP responses
            # logger.info("Sending error event via WebSocket...")
            # await ws_manager.send_event(...)
            logger.error("=" * 80)
            raise
    
    async def execute_to_checkpoint(
        self,
        job_id: str,
        nl_description: str,
        checkpoint_type: str,
        job_manager: JobManager,
        current_state: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Execute pipeline up to a specific checkpoint.
        
        Checkpoint types (in order):
        - "domain": Execute steps 1.1 and 1.2, return domain
        - "entities": Execute entity extraction (1.4), return entities
        - "relations": Execute relation extraction (1.9), return relations
        - "attributes": Execute attribute extraction (2.2), return attributes
        - "primary_keys": Execute primary key identification (2.7), return primary keys
        - "multivalued_derived": Execute multivalued/derived detection (2.8, 2.9), return multivalued/derived attributes
        - "nullability": Execute nullability constraints (2.11), return nullability
        - "default_values": Execute default values (2.12), return default values
        - "check_constraints": Execute check constraints (2.13), return check constraints
        - "phase2_final": Execute phase 2 final steps (2.14-2.16), return final attributes
        - "er_diagram": Execute ER design compilation (Phase 3), return ER design
        - "relational_schema": Execute relational schema compilation (Phase 4), return relational schema
        - "datatypes": Execute data type assignment (Phase 5), return data types
        
        Args:
            job_id: Job identifier
            nl_description: Natural language description
            checkpoint_type: Type of checkpoint to reach
            job_manager: Job manager instance
            current_state: Current state (if resuming from previous checkpoint)
            
        Returns:
            State dictionary at the checkpoint
        """
        from NL2DATA.phases.phase1 import (
            step_1_1_domain_detection,
            step_1_2_entity_mention_detection,
            step_1_4_key_entity_extraction,
        )
        from NL2DATA.orchestration.state import create_initial_state
        
        logger.info("=" * 80)
        logger.info(f"NL2DATA SERVICE: Executing to checkpoint '{checkpoint_type}'")
        logger.info("=" * 80)
        logger.info(f"Job ID: {job_id}")
        
        try:
            # Initialize or use existing state
            if current_state:
                state = current_state
            else:
                state = create_initial_state(nl_description)
            
            # Update job status
            job_manager.update_job(job_id, status="in_progress")
            
            if checkpoint_type == "domain":
                # Execute step 1.1 (handles both explicit detection and inference)
                logger.info("Executing Step 1.1: Domain Detection & Inference...")
                result_1_1 = await step_1_1_domain_detection(nl_description)
                state["domain"] = result_1_1.get("domain")
                state["has_explicit_domain"] = result_1_1.get("has_explicit_domain", False)
                state["current_step"] = "1.1"
                state["previous_answers"] = state.get("previous_answers", {})
                state["previous_answers"]["1.1"] = result_1_1
                
                # Step 1.2: Entity Mention Detection (needed for subsequent steps)
                logger.info("Executing Step 1.2: Entity Mention Detection...")
                result_1_2 = await step_1_2_entity_mention_detection(nl_description)
                state["current_step"] = "1.2"
                state["previous_answers"]["1.2"] = result_1_2
                
                # Note: Step 1.3 (domain inference) is deprecated - step 1.1 already handles inference
                # If domain was not explicitly detected, step 1.1 already inferred it
                if not state.get("has_explicit_domain"):
                    logger.info("Domain was inferred by Step 1.1 (Step 1.3 is deprecated and no longer needed)")
                
                # Set checkpoint status
                job_manager.update_job(job_id, status="checkpoint_domain", state=state)
                logger.info(f"Checkpoint 'domain' reached. Domain: {state.get('domain')}")
                
            elif checkpoint_type == "entities":
                # Execute entity extraction (1.4)
                logger.info("Executing Step 1.4: Key Entity Extraction...")
                result_1_4 = await step_1_4_key_entity_extraction(
                    nl_description,
                    domain=state.get("domain"),
                    domain_detection_result=state.get("previous_answers", {}).get("1.1"),
                    entity_mention_result=state.get("previous_answers", {}).get("1.2")
                )
                
                entities = result_1_4.get("entities", [])
                entity_dicts = [
                    e.dict() if hasattr(e, "dict") else e
                    for e in entities
                ]
                state["entities"] = entity_dicts
                state["current_step"] = "1.4"
                state["previous_answers"] = state.get("previous_answers", {})
                state["previous_answers"]["1.4"] = result_1_4
                
                # Set checkpoint status
                job_manager.update_job(job_id, status="checkpoint_entities", state=state)
                logger.info(f"Checkpoint 'entities' reached. Found {len(entity_dicts)} entities")
                
            elif checkpoint_type == "relations":
                # Execute relation extraction (1.9)
                from NL2DATA.phases.phase1 import step_1_9_key_relations_extraction
                from NL2DATA.phases.phase1.step_1_11_relation_cardinality import step_1_11_relation_cardinality
                
                logger.info("Executing Step 1.9: Key Relations Extraction...")
                result_1_9 = await step_1_9_key_relations_extraction(
                    entities=state.get("entities", []),
                    nl_description=nl_description,
                    domain=state.get("domain"),
                    mentioned_relations=None  # Can be enhanced later with step 1.5 results
                )
                
                relations = result_1_9.get("relations", [])
                relation_dicts = [
                    r.dict() if hasattr(r, "dict") else r
                    for r in relations
                ]
                
                # Execute Step 1.11: Relation Cardinality to get entity_cardinalities and entity_participations
                logger.info("Executing Step 1.11: Relation Cardinality & Participation...")
                result_1_11 = await step_1_11_relation_cardinality(
                    relations=relation_dicts,
                    entities=state.get("entities", []),
                    nl_description=nl_description
                )
                
                # Merge cardinality and participation data into relations
                relation_cardinalities = result_1_11.get("relation_cardinalities", [])
                # Create a lookup map: (sorted entity tuple) -> cardinality data
                cardinality_map = {}
                for rel_card in relation_cardinalities:
                    if isinstance(rel_card, dict):
                        entities_in_rel = rel_card.get("entities", [])
                        if entities_in_rel:
                            # Normalize entity names for matching (case-insensitive, sorted)
                            key = tuple(sorted(str(e).lower() for e in entities_in_rel))
                            cardinality_map[key] = {
                                "entity_cardinalities": rel_card.get("entity_cardinalities", {}),
                                "entity_participations": rel_card.get("entity_participations", {})
                            }
                
                # Merge cardinality data into each relation
                for relation in relation_dicts:
                    entities_in_rel = relation.get("entities", [])
                    if entities_in_rel:
                        # Normalize entity names for matching (case-insensitive, sorted)
                        key = tuple(sorted(str(e).lower() for e in entities_in_rel))
                        if key in cardinality_map:
                            relation["entity_cardinalities"] = cardinality_map[key]["entity_cardinalities"]
                            relation["entity_participations"] = cardinality_map[key]["entity_participations"]
                        else:
                            # Initialize empty if not found (will default to "1" and "partial" in UI)
                            relation["entity_cardinalities"] = {}
                            relation["entity_participations"] = {}
                
                state["relations"] = relation_dicts
                state["current_step"] = "1.11"
                state["previous_answers"] = state.get("previous_answers", {})
                state["previous_answers"]["1.9"] = result_1_9
                state["previous_answers"]["1.11"] = result_1_11
                
                # Set checkpoint status
                job_manager.update_job(job_id, status="checkpoint_relations", state=state)
                logger.info(f"Checkpoint 'relations' reached. Found {len(relation_dicts)} relations with cardinalities")
                
            elif checkpoint_type == "attributes":
                # Execute attribute extraction (2.2) for all entities
                from NL2DATA.phases.phase2.step_2_2_intrinsic_attributes import step_2_2_intrinsic_attributes_batch
                
                logger.info("Executing Step 2.2: Intrinsic Attributes Extraction...")
                entities = state.get("entities", [])
                relations = state.get("relations", [])
                
                result_2_2 = await step_2_2_intrinsic_attributes_batch(
                    entities=entities,
                    nl_description=nl_description,
                    attribute_count_results=None,  # Step 2.1 not executed in checkpoint flow
                    domain=state.get("domain"),
                    relations=relations,
                    primary_keys=None  # Step 2.7 not executed in checkpoint flow
                )
                
                # Convert results to the format expected by state
                # result_2_2 has structure: {"entity_results": {entity_name: {"attributes": [...]}}}
                entity_results = result_2_2.get("entity_results", {})
                attributes_dict = {}
                for entity_name, entity_result in entity_results.items():
                    attrs = entity_result.get("attributes", [])
                    # Convert AttributeInfo objects to dicts
                    attr_dicts = [
                        a.dict() if hasattr(a, "dict") else a
                        for a in attrs
                    ]
                    attributes_dict[entity_name] = attr_dicts
                
                state["attributes"] = attributes_dict
                state["current_step"] = "2.2"
                state["previous_answers"] = state.get("previous_answers", {})
                state["previous_answers"]["2.2"] = result_2_2
                
                # Set checkpoint status
                job_manager.update_job(job_id, status="checkpoint_attributes", state=state)
                total_attrs = sum(len(attrs) for attrs in attributes_dict.values())
                logger.info(f"Checkpoint 'attributes' reached. Found {total_attrs} attributes across {len(attributes_dict)} entities")
                
            elif checkpoint_type == "primary_keys":
                # Execute primary key identification (2.7) for all entities
                from NL2DATA.phases.phase2.step_2_7_primary_key_identification import step_2_7_primary_key_identification_batch
                
                logger.info("Executing Step 2.7: Primary Key Identification...")
                entities = state.get("entities", [])
                attributes = state.get("attributes", {})
                
                # Convert attributes dict to entity -> attribute names list
                entity_attributes = {}
                for entity_name, attrs in attributes.items():
                    entity_attributes[entity_name] = [
                        a.get("name") if isinstance(a, dict) else getattr(a, "name", "")
                        for a in attrs
                    ]
                
                result_2_7 = await step_2_7_primary_key_identification_batch(
                    entities=entities,
                    entity_attributes=entity_attributes,
                    nl_description=nl_description,
                    domain=state.get("domain")
                )
                
                # Convert results to the format expected by state
                entity_results = result_2_7.get("entity_results", {})
                primary_keys_dict = {}
                for entity_name, entity_result in entity_results.items():
                    pk = entity_result.get("primary_key", [])
                    if pk:
                        primary_keys_dict[entity_name] = pk
                
                # CRITICAL: If Step 2.7 chose a surrogate key not present in the attribute list,
                # propagate it into attributes now. This prevents downstream steps from operating
                # on a PK column that doesn't exist.
                for ent_name, pk_list in primary_keys_dict.items():
                    if not isinstance(pk_list, list):
                        continue
                    for pk_attr in pk_list:
                        if not isinstance(pk_attr, str) or not pk_attr.strip():
                            continue
                        # Check if PK attribute exists in attributes
                        attrs_obj_list = attributes.get(ent_name, [])
                        existing_names = {
                            (a.get("name") if isinstance(a, dict) else getattr(a, "name", ""))
                            for a in attrs_obj_list
                        }
                        if pk_attr not in existing_names:
                            # Add surrogate key as a new attribute
                            attrs_obj_list.append({
                                "name": pk_attr,
                                "description": "Surrogate primary key (auto-added)",
                                "type_hint": "integer"
                            })
                            attributes[ent_name] = attrs_obj_list
                            logger.info(f"Added surrogate key '{pk_attr}' to attributes for entity '{ent_name}'")
                
                # Update state with both primary keys AND updated attributes
                state["primary_keys"] = primary_keys_dict
                state["attributes"] = attributes
                state["current_step"] = "2.7"
                state["previous_answers"] = state.get("previous_answers", {})
                state["previous_answers"]["2.7"] = result_2_7
                
                # Set checkpoint status
                job_manager.update_job(job_id, status="checkpoint_primary_keys", state=state)
                logger.info(f"Checkpoint 'primary_keys' reached. Identified primary keys for {len(primary_keys_dict)} entities")
                
            elif checkpoint_type == "multivalued_derived":
                # Execute multivalued/derived detection (2.8) and derived formulas (2.9)
                # Note: These steps are now in Phase 2 (after primary key identification)
                from NL2DATA.phases.phase2.step_2_8_multivalued_derived_detection import step_2_8_multivalued_derived_detection_batch
                from NL2DATA.phases.phase2.step_2_9_derived_attribute_formulas import step_2_9_derived_attribute_formulas_batch
                
                logger.info("Executing Step 2.8: Multivalued/Derived Detection...")
                entities = state.get("entities", [])
                attributes = state.get("attributes", {})
                primary_keys = state.get("primary_keys", {})
                
                # Convert attributes dict to entity -> attribute names list (for step 2.8)
                entity_attributes_names = {}
                for entity_name, attrs in attributes.items():
                    entity_attributes_names[entity_name] = [
                        a.get("name") if isinstance(a, dict) else getattr(a, "name", "")
                        for a in attrs
                    ]
                
                result_2_8 = await step_2_8_multivalued_derived_detection_batch(
                    entities=entities,
                    entity_attributes=entity_attributes_names,
                    primary_keys=primary_keys,
                    nl_description=nl_description,
                    domain=state.get("domain")
                )
                
                # Extract derived attributes and derivation rules for Step 2.9
                entity_results_2_8 = result_2_8.get("entity_results", {})
                entity_derived_attributes = {}
                derivation_rules = {}
                entity_descriptions = {
                    (e.get("name") if isinstance(e, dict) else getattr(e, "name", "")): (
                        e.get("description", "") if isinstance(e, dict) else getattr(e, "description", "")
                    )
                    for e in entities
                }
                
                for entity_name, result in entity_results_2_8.items():
                    derived = result.get("derived", [])
                    if derived:
                        entity_derived_attributes[entity_name] = derived
                    rules = result.get("derivation_rules", {})
                    if rules:
                        derivation_rules[entity_name] = rules
                
                # Execute Step 2.9: Derived Attribute Formulas (DSL)
                # Note: Step 2.9 needs full attribute objects (with descriptions, types), not just names
                derived_formulas = {}
                if entity_derived_attributes:
                    logger.info("Executing Step 2.9: Derived Attribute Formulas (DSL)...")
                    result_2_9 = await step_2_9_derived_attribute_formulas_batch(
                        entity_derived_attributes=entity_derived_attributes,
                        entity_attributes=attributes,  # Pass full attribute objects, not just names
                        entity_descriptions=entity_descriptions,
                        derivation_rules=derivation_rules,
                        nl_description=nl_description
                    )
                    
                    # Store derived formulas in state (format: "Entity.attr" -> formula info)
                    entity_results_2_9 = result_2_9.get("entity_results", {})
                    for entity_name, attr_formulas in entity_results_2_9.items():
                        for attr_name, formula_info in attr_formulas.items():
                            key = f"{entity_name}.{attr_name}"
                            derived_formulas[key] = formula_info
                
                state["multivalued_derived"] = entity_results_2_8
                state["derived_formulas"] = derived_formulas
                state["current_step"] = "2.9"
                state["previous_answers"] = state.get("previous_answers", {})
                state["previous_answers"]["2.8"] = result_2_8
                if entity_derived_attributes:
                    state["previous_answers"]["2.9"] = result_2_9
                
                # Set checkpoint status
                job_manager.update_job(job_id, status="checkpoint_multivalued_derived", state=state)
                total_multivalued = sum(len(r.get("multivalued", [])) for r in entity_results_2_8.values())
                total_derived = sum(len(r.get("derived", [])) for r in entity_results_2_8.values())
                logger.info(f"Checkpoint 'multivalued_derived' reached. Found {total_multivalued} multivalued and {total_derived} derived attributes")
                
            elif checkpoint_type == "nullability":
                # Execute nullability detection (Step 5.5) - after datatypes assignment
                from NL2DATA.phases.phase5.step_5_5_nullability_detection import step_5_5_nullability_detection_batch
                
                logger.info("Executing Step 5.5: Nullability Detection...")
                
                # Get relational schema (should already be compiled during datatypes checkpoint)
                relational_schema = state.get("relational_schema", {})
                if not relational_schema or not relational_schema.get("tables"):
                    raise ValueError("Relational schema must be compiled before nullability detection. Please execute 'datatypes' checkpoint first.")
                
                # Get primary keys - need to map from entity names to table names
                # For now, assume table names match entity names
                primary_keys = state.get("primary_keys", {})
                foreign_keys = state.get("foreign_keys", [])
                
                result_5_5 = await step_5_5_nullability_detection_batch(
                    relational_schema=relational_schema,
                    primary_keys=primary_keys,  # table_name -> PK list
                    foreign_keys=foreign_keys,
                    nl_description=nl_description,
                    domain=state.get("domain")
                )
                
                # Store nullability results - convert from table_results to entity-organized format
                table_results = result_5_5.get("table_results", {})
                nullability_dict = {}
                for table_name, result in table_results.items():
                    nullability_dict[table_name] = {
                        "nullable": result.get("nullable_columns", []),
                        "non_nullable": result.get("not_nullable_columns", []),
                        "reasoning": result.get("reasoning", "")
                    }
                
                state["nullability"] = nullability_dict
                state["current_step"] = "5.5"
                state["previous_answers"] = state.get("previous_answers", {})
                state["previous_answers"]["5.5"] = result_5_5
                
                # Set checkpoint status
                job_manager.update_job(job_id, status="checkpoint_nullability", state=state)
                logger.info(f"Checkpoint 'nullability' reached. Processed {len(nullability_dict)} tables")
                
            elif checkpoint_type == "default_values":
                # Execute default values (7.4)
                # TODO: step_7_4_default_values_batch doesn't exist - this checkpoint may need to be removed or implemented
                # from NL2DATA.phases.phase7.step_7_4_default_values import step_7_4_default_values_batch
                raise NotImplementedError("Default values checkpoint is not yet implemented. step_7_4_default_values_batch function does not exist.")
                
                logger.info("Executing Step 7.4: Default Values...")
                entities = state.get("entities", [])
                attributes = state.get("attributes", {})
                nullability = state.get("nullability", {})
                
                # Convert attributes dict to entity -> attribute names list
                entity_attributes = {}
                for entity_name, attrs in attributes.items():
                    entity_attributes[entity_name] = [
                        a.get("name") if isinstance(a, dict) else getattr(a, "name", "")
                        for a in attrs
                    ]
                
                # Prepare nullability data for Step 7.4 (convert to format expected by function)
                entity_nullability = {}
                for entity_name, null_info in nullability.items():
                    entity_nullability[entity_name] = {
                        "nullable_attributes": null_info.get("nullable", []),
                        "non_nullable_attributes": null_info.get("non_nullable", [])
                    }
                
                result_7_4 = await step_7_4_default_values_batch(
                    entities=entities,
                    entity_attributes=entity_attributes,
                    entity_nullability=entity_nullability,
                    nl_description=nl_description,
                    domain=state.get("domain")
                )
                
                # Store default values
                entity_results_7_4 = result_7_4.get("entity_results", {})
                default_values_dict = {}
                for entity_name, result in entity_results_7_4.items():
                    default_values_dict[entity_name] = {
                        "default_values": result.get("default_values", {}),
                        "reasoning": result.get("reasoning", {})
                    }
                
                state["default_values"] = default_values_dict
                state["current_step"] = "7.4"
                state["previous_answers"] = state.get("previous_answers", {})
                state["previous_answers"]["7.4"] = result_7_4
                
                # Set checkpoint status
                job_manager.update_job(job_id, status="checkpoint_default_values", state=state)
                logger.info(f"Checkpoint 'default_values' reached. Processed {len(default_values_dict)} entities")
                
            elif checkpoint_type == "check_constraints":
                # Execute check constraints (7.5)
                # TODO: step_7_3_categorical_detection_batch and step_7_5_check_constraint_detection_batch don't exist
                # These functions may have been moved or removed. This checkpoint may need to be removed or use Phase 8 functions instead.
                # from NL2DATA.phases.phase7.step_7_3_categorical_detection import step_7_3_categorical_detection_batch
                # from NL2DATA.phases.phase7.step_7_5_check_constraint_detection import step_7_5_check_constraint_detection_batch
                raise NotImplementedError("Check constraints checkpoint is not yet implemented. step_7_3_categorical_detection_batch and step_7_5_check_constraint_detection_batch functions do not exist.")
                
                logger.info("Executing Step 7.3: Categorical Detection (required for Step 7.5)...")
                entities = state.get("entities", [])
                attributes = state.get("attributes", {})
                data_types = state.get("data_types", {})
                
                # Step 7.3: Categorical Detection (needed for Step 7.5)
                result_7_3 = await step_7_3_categorical_detection_batch(
                    entities=entities,
                    entity_attributes=attributes,  # Full attribute dicts with descriptions
                    entity_attribute_types=data_types,  # From Phase 3 if available
                    nl_description=nl_description,
                    domain=state.get("domain")
                )
                
                # Extract categorical attributes
                categorical_attributes = {}
                entity_results_7_3 = result_7_3.get("entity_results", {})
                for entity_name, result in entity_results_7_3.items():
                    categorical_attributes[entity_name] = result.get("categorical_attributes", [])
                
                state["previous_answers"] = state.get("previous_answers", {})
                state["previous_answers"]["7.3"] = result_7_3
                state["categorical_attributes"] = categorical_attributes
                
                logger.info("Executing Step 7.5: Check Constraint Detection...")
                
                # Step 7.5: Check Constraint Detection (requires categorical attributes from 7.3)
                result_7_5 = await step_7_5_check_constraint_detection_batch(
                    entity_categorical_attributes=categorical_attributes,
                    entity_attributes=attributes,  # Full attribute dicts with descriptions
                    entity_attribute_types=data_types,  # From Phase 3 if available
                    nl_description=nl_description,
                    domain=state.get("domain")
                )
                
                # Store check constraints
                entity_results_7_5 = result_7_5.get("entity_results", {})
                check_constraints_dict = {}
                for entity_name, result in entity_results_7_5.items():
                    constraints = result.get("check_constraints", {})
                    if constraints:
                        check_constraints_dict[entity_name] = constraints
                
                state["check_constraints"] = check_constraints_dict
                state["current_step"] = "7.5"
                state["previous_answers"] = state.get("previous_answers", {})
                state["previous_answers"]["7.5"] = result_7_5
                
                # Set checkpoint status
                job_manager.update_job(job_id, status="checkpoint_check_constraints", state=state)
                total_constraints = sum(len(c) for c in check_constraints_dict.values())
                logger.info(f"Checkpoint 'check_constraints' reached. Found {total_constraints} check constraints")
                
            elif checkpoint_type == "phase2_final":
                # Execute steps 2.15-2.16 (relation attributes, cross-entity reconciliation)
                # Note: Step 2.14 (entity cleanup) doesn't exist - it was removed/merged into other steps
                from NL2DATA.phases.phase2.step_2_15_relation_intrinsic_attributes import step_2_15_relation_intrinsic_attributes_batch
                from NL2DATA.phases.phase2.step_2_16_cross_entity_attribute_reconciliation import step_2_16_cross_entity_attribute_reconciliation_batch
                
                logger.info("Executing Steps 2.15-2.16: Phase 2 Final Steps...")
                entities = state.get("entities", [])
                relations = state.get("relations", [])
                attributes = state.get("attributes", {})
                
                # Step 2.15: Relation Intrinsic Attributes
                logger.info("Executing Step 2.15: Relation Intrinsic Attributes...")
                result_2_15 = await step_2_15_relation_intrinsic_attributes_batch(
                    relations=relations,
                    entity_intrinsic_attributes=attributes,
                    nl_description=nl_description,
                    domain=state.get("domain")
                )
                
                relation_attributes = result_2_15.get("relation_results", {})
                
                # Step 2.16: Cross-Entity Attribute Reconciliation
                logger.info("Executing Step 2.16: Cross-Entity Attribute Reconciliation...")
                result_2_16 = await step_2_16_cross_entity_attribute_reconciliation_batch(
                    entities=entities,
                    attributes=attributes,
                    relations=relations,
                    nl_description=nl_description,
                    domain=state.get("domain")
                )
                
                # Update attributes after reconciliation
                updated_attrs_2_16 = result_2_16.get("updated_attributes", {})
                if updated_attrs_2_16:
                    attributes = updated_attrs_2_16
                
                state["attributes"] = attributes
                state["relation_attributes"] = relation_attributes
                state["current_step"] = "2.16"
                state["previous_answers"] = state.get("previous_answers", {})
                state["previous_answers"]["2.15"] = result_2_15
                state["previous_answers"]["2.16"] = result_2_16
                
                # Set checkpoint status
                job_manager.update_job(job_id, status="checkpoint_phase2_final", state=state)
                logger.info(f"Checkpoint 'phase2_final' reached. Completed Phase 2 final steps")
                
            elif checkpoint_type == "er_diagram":
                # Execute ER design compilation (Phase 3.1)
                from NL2DATA.phases.phase3.step_3_1_er_design_compilation import step_3_1_er_design_compilation
                
                logger.info("Executing Step 3.1: ER Design Compilation...")
                entities = state.get("entities", [])
                relations = state.get("relations", [])
                attributes = state.get("attributes", {})
                primary_keys = state.get("primary_keys", {})
                foreign_keys = state.get("foreign_keys", [])
                constraints = state.get("constraints", [])
                
                # Validate required data
                if not entities:
                    raise ValueError("Cannot compile ER design: no entities found in state")
                if not attributes:
                    raise ValueError("Cannot compile ER design: no attributes found in state")
                if not primary_keys:
                    logger.warning("No primary keys found in state. ER design will be compiled without primary keys.")
                
                er_design = step_3_1_er_design_compilation(
                    entities=entities,
                    relations=relations,
                    attributes=attributes,
                    primary_keys=primary_keys,
                    foreign_keys=foreign_keys,
                    constraints=constraints
                )
                
                state["er_design"] = er_design
                state["current_step"] = "3.1"
                state["previous_answers"] = state.get("previous_answers", {})
                state["previous_answers"]["3.1"] = er_design
                
                # Set checkpoint status
                job_manager.update_job(job_id, status="checkpoint_er_diagram", state=state)
                logger.info(f"Checkpoint 'er_diagram' reached. Compiled ER design with {len(er_design.get('entities', []))} entities")
                
                
            elif checkpoint_type == "relational_schema":
                # Show relational schema checkpoint (Phase 4)
                # Note: Relational schema should already be compiled during datatypes checkpoint
                # This checkpoint is just for review/editing by the user
                logger.info("Displaying relational schema checkpoint (should already be compiled)...")
                
                relational_schema = state.get("relational_schema", {})
                
                # If relational schema doesn't exist, compile it now
                if not relational_schema or not relational_schema.get("tables"):
                    logger.info("Relational schema not found or empty. Compiling it now...")
                    from NL2DATA.phases.phase4.step_4_1_relational_schema_compilation import step_4_1_relational_schema_compilation
                    
                    # Get or compile ER design first
                    er_design = state.get("er_design", {})
                    er_entities = er_design.get("entities", []) if isinstance(er_design, dict) else []
                    if not er_design or not er_entities:
                        logger.info("ER design not found or empty. Compiling ER design from Phase 2 outputs...")
                        from NL2DATA.phases.phase3.step_3_1_er_design_compilation import step_3_1_er_design_compilation
                        er_design = step_3_1_er_design_compilation(
                            entities=state.get("entities", []),
                            relations=state.get("relations", []),
                            attributes=state.get("attributes", {}),
                            primary_keys=state.get("primary_keys", {}),
                            foreign_keys=state.get("foreign_keys", []),
                            constraints=state.get("constraints", [])
                        )
                        state["er_design"] = er_design
                        state["previous_answers"] = state.get("previous_answers", {})
                        state["previous_answers"]["3.1"] = er_design
                        logger.info(f"ER design compiled with {len(er_design.get('entities', []))} entities")
                    
                    # Validate er_design has entities before proceeding
                    er_entities = er_design.get("entities", []) if isinstance(er_design, dict) else []
                    if not er_entities:
                        raise ValueError("Cannot compile relational schema: ER design has no entities. Please ensure ER diagram checkpoint was completed successfully.")
                    
                    # Compile relational schema (structure only, datatypes will be added in Phase 5)
                    relational_schema = step_4_1_relational_schema_compilation(
                        er_design=er_design,
                        foreign_keys=state.get("foreign_keys", []),
                        primary_keys=state.get("primary_keys", {}),
                        constraints=state.get("constraints", []),
                        junction_table_names=state.get("junction_table_names", {})
                    )
                    state["relational_schema"] = relational_schema
                    state["previous_answers"] = state.get("previous_answers", {})
                    state["previous_answers"]["4.1"] = relational_schema
                    logger.info(f"Relational schema compiled with {len(relational_schema.get('tables', []))} tables")
                else:
                    logger.info(f"Using existing relational schema with {len(relational_schema.get('tables', []))} tables")
                
                # Update state and set checkpoint status
                state["relational_schema"] = relational_schema
                state["current_step"] = "4.1"
                if "4.1" not in state.get("previous_answers", {}):
                    state["previous_answers"] = state.get("previous_answers", {})
                    state["previous_answers"]["4.1"] = relational_schema
                
                job_manager.update_job(job_id, status="checkpoint_relational_schema", state=state)
                tables = relational_schema.get("tables", [])
                logger.info(f"Checkpoint 'relational_schema' reached. Schema has {len(tables)} tables")
                
            elif checkpoint_type == "datatypes":
                # Execute Phase 5: Data Type Assignment (after relational schema is compiled)
                from NL2DATA.phases.phase5.step_5_1_attribute_dependency_graph import step_5_1_attribute_dependency_graph
                from NL2DATA.phases.phase5.step_5_2_independent_attribute_data_types import step_5_2_independent_attribute_data_types_batch
                from NL2DATA.phases.phase5.step_5_3_deterministic_fk_data_types import step_5_3_deterministic_fk_data_types
                from NL2DATA.phases.phase5.step_5_4_dependent_attribute_data_types import step_5_4_dependent_attribute_data_types_batch
                
                logger.info("Executing Phase 5: Data Type Assignment (after relational schema compilation)...")
                
                # Get relational schema that was compiled in Phase 4
                # If not present, compile it now (needed for datatypes assignment)
                relational_schema = state.get("relational_schema", {})
                if not relational_schema or not relational_schema.get("tables"):
                    logger.info("Relational schema not found or empty. Compiling it now before datatypes assignment...")
                    from NL2DATA.phases.phase4.step_4_1_relational_schema_compilation import step_4_1_relational_schema_compilation
                    
                    # Get or compile ER design first
                    er_design = state.get("er_design", {})
                    er_entities = er_design.get("entities", []) if isinstance(er_design, dict) else []
                    if not er_design or not er_entities:
                        logger.info("ER design not found or empty. Compiling ER design from Phase 2 outputs...")
                        from NL2DATA.phases.phase3.step_3_1_er_design_compilation import step_3_1_er_design_compilation
                        er_design = step_3_1_er_design_compilation(
                            entities=state.get("entities", []),
                            relations=state.get("relations", []),
                            attributes=state.get("attributes", {}),
                            primary_keys=state.get("primary_keys", {}),
                            foreign_keys=state.get("foreign_keys", []),
                            constraints=state.get("constraints", [])
                        )
                        state["er_design"] = er_design
                    
                    # Compile relational schema
                    relational_schema = step_4_1_relational_schema_compilation(
                        er_design=er_design,
                        foreign_keys=state.get("foreign_keys", []),
                        primary_keys=state.get("primary_keys", {}),
                        constraints=state.get("constraints", []),
                        junction_table_names=state.get("junction_table_names", {})
                    )
                    state["relational_schema"] = relational_schema
                    state["previous_answers"] = state.get("previous_answers", {})
                    state["previous_answers"]["4.1"] = relational_schema
                    logger.info(f"Relational schema compiled with {len(relational_schema.get('tables', []))} tables")
                
                entities = state.get("entities", [])
                attributes = state.get("attributes", {})
                primary_keys = state.get("primary_keys", {})
                relations = state.get("relations", [])
                derived_formulas = state.get("derived_formulas", {})
                domain = state.get("domain")
                nl_description = state.get("nl_description", "")
                
                # Extract and normalize relation_cardinalities from state (may be nested)
                relation_cardinalities_raw = state.get("relation_cardinalities", {})
                relation_cardinalities: Dict[str, Dict[str, str]] = {}
                if relation_cardinalities_raw:
                    for rel_id, payload in relation_cardinalities_raw.items():
                        if isinstance(payload, dict):
                            entity_cards = payload.get("entity_cardinalities", {})
                            if isinstance(entity_cards, dict):
                                relation_cardinalities[str(rel_id)] = {str(k): str(v) for k, v in entity_cards.items() if k and v}
                        elif isinstance(payload, dict) and not payload.get("entity_cardinalities"):
                            # Already in flat format
                            relation_cardinalities[str(rel_id)] = {str(k): str(v) for k, v in payload.items() if k and v}
                
                # Step 5.1: Build attribute dependency graph (creates FKs, identifies independent/dependent attributes)
                logger.info("Executing Step 5.1: Attribute Dependency Graph Construction...")
                result_5_1 = step_5_1_attribute_dependency_graph(
                    entities=entities,
                    attributes=attributes,
                    primary_keys=primary_keys,
                    relations=relations,
                    relation_cardinalities=relation_cardinalities,
                    foreign_keys=state.get("foreign_keys"),  # Optional - will be created if not provided
                    derived_formulas=derived_formulas,
                )
                
                # Store created FKs if any
                created_fks = result_5_1.get("created_foreign_keys", [])
                if created_fks:
                    state["foreign_keys"] = created_fks
                
                # Extract dependency information
                dependency_graph = result_5_1.get("dependency_graph", {})
                independent_attributes = result_5_1.get("independent_attributes", [])
                dependent_attributes = result_5_1.get("dependent_attributes", [])
                fk_dependencies = result_5_1.get("fk_dependencies", {})
                derived_dependencies = result_5_1.get("derived_dependencies", {})
                
                state["previous_answers"] = state.get("previous_answers", {})
                state["previous_answers"]["5.1"] = result_5_1
                state["metadata"] = state.get("metadata", {})
                state["metadata"]["dependency_graph"] = dependency_graph
                state["metadata"]["independent_attributes"] = independent_attributes
                state["metadata"]["dependent_attributes"] = dependent_attributes
                state["metadata"]["fk_dependencies"] = fk_dependencies
                state["metadata"]["derived_dependencies"] = derived_dependencies
                
                # Extract entity descriptions
                entity_descriptions = {}
                for entity in entities:
                    entity_name = entity.get("name", "") if isinstance(entity, dict) else getattr(entity, "name", "")
                    entity_desc = entity.get("description", "") if isinstance(entity, dict) else getattr(entity, "description", "")
                    if entity_name:
                        entity_descriptions[entity_name] = entity_desc
                
                # Step 5.2: Assign types to independent attributes
                logger.info("Executing Step 5.2: Independent Attribute Data Types...")
                result_5_2 = await step_5_2_independent_attribute_data_types_batch(
                    independent_attributes=independent_attributes,
                    attributes=attributes,
                    primary_keys=primary_keys,
                    domain=domain,
                    entity_descriptions=entity_descriptions,
                    nl_description=nl_description,
                )
                independent_types = result_5_2.get("data_types", {})
                state["previous_answers"]["5.2"] = result_5_2
                
                # Step 5.3: Derive FK types from PK types
                logger.info("Executing Step 5.3: Deterministic FK Data Types...")
                result_5_3 = step_5_3_deterministic_fk_data_types(
                    foreign_keys=created_fks if created_fks else state.get("foreign_keys", []),
                    independent_attribute_data_types=independent_types,
                )
                fk_types = result_5_3.get("fk_data_types", {})
                state["previous_answers"]["5.3"] = result_5_3
                
                # Step 5.4: Assign types to dependent attributes (excluding FKs which are already done)
                # Filter out FK attributes from dependent_attributes
                fk_keys = set(fk_dependencies.keys())
                non_fk_dependent = [(e, a) for e, a in dependent_attributes if f"{e}.{a}" not in fk_keys]
                
                dependent_types = {}
                if non_fk_dependent:
                    logger.info("Executing Step 5.4: Dependent Attribute Data Types...")
                    result_5_4 = await step_5_4_dependent_attribute_data_types_batch(
                        dependent_attributes=non_fk_dependent,
                        attributes=attributes,
                        dependency_graph=dependency_graph,
                        fk_dependencies=fk_dependencies,
                        derived_dependencies=derived_dependencies,
                        independent_types=independent_types,
                        fk_types=fk_types,
                        primary_keys=primary_keys,
                        derived_formulas=derived_formulas if derived_formulas else None,
                        domain=domain,
                        nl_description=nl_description,
                    )
                    dependent_types = result_5_4.get("data_types", {})
                    state["previous_answers"]["5.4"] = result_5_4
                
                # Combine all type assignments
                all_data_types = {**independent_types, **fk_types, **dependent_types}
                
                logger.info(f"Combined data types: {len(all_data_types)} total attributes")
                if all_data_types:
                    logger.info(f"Sample data types keys: {list(all_data_types.keys())[:5]}")
                
                # Convert to entity-organized format for checkpoint
                entity_data_types = {}
                for attr_key, type_info in all_data_types.items():
                    if "." in attr_key:
                        entity_name, attr_name = attr_key.split(".", 1)
                        if entity_name not in entity_data_types:
                            entity_data_types[entity_name] = {"attribute_types": {}}
                        entity_data_types[entity_name]["attribute_types"][attr_name] = type_info
                    else:
                        logger.warning(f"Data type key '{attr_key}' does not contain '.', skipping entity organization")
                
                logger.info(f"Entity-organized data types: {len(entity_data_types)} entities")
                for entity_name, entity_data in entity_data_types.items():
                    attr_count = len(entity_data.get("attribute_types", {}))
                    logger.info(f"  - {entity_name}: {attr_count} attributes")
                
                state["data_types"] = entity_data_types
                state["current_step"] = "5.4"
                
                # Enrich relational schema with datatypes
                if relational_schema and entity_data_types:
                    for table in relational_schema.get("tables", []):
                        table_name = table.get("name", "")
                        # Try to match table to entity (table name might be different from entity name)
                        # For now, use table name directly
                        entity_name = table_name
                        if entity_name in entity_data_types:
                            entity_data_types_dict = entity_data_types[entity_name]
                            if isinstance(entity_data_types_dict, dict) and "attribute_types" in entity_data_types_dict:
                                attribute_types = entity_data_types_dict["attribute_types"]
                            else:
                                attribute_types = entity_data_types_dict
                            
                            # Update columns with datatypes
                            for column in table.get("columns", []):
                                col_name = column.get("name", "")
                                if col_name in attribute_types:
                                    type_info = attribute_types[col_name]
                                    column["data_type"] = type_info.get("type", "")
                                    column["data_type_size"] = type_info.get("size")
                                    column["data_type_precision"] = type_info.get("precision")
                                    column["data_type_scale"] = type_info.get("scale")
                                    column["data_type_reasoning"] = type_info.get("reasoning", "")
                    
                    # Update state with enriched schema
                    state["relational_schema"] = relational_schema
                
                # Set checkpoint status
                job_manager.update_job(job_id, status="checkpoint_datatypes", state=state)
                logger.info(f"Checkpoint 'datatypes' reached. Phase 5 completed. Assigned types to {len(all_data_types)} attributes and enriched relational schema.")
                
            elif checkpoint_type == "information_mining":
                # Execute Phase 7: Information Mining (SQL validation only)
                from NL2DATA.phases.phase7.step_7_1_information_need_identification import step_7_1_information_need_identification_with_loop
                from NL2DATA.phases.phase7.step_7_2_sql_generation_and_validation import step_7_2_sql_generation_and_validation_batch
                
                logger.info("Executing Phase 7: Information Mining...")
                
                entities = state.get("entities", [])
                relations = state.get("relations", [])
                attributes = state.get("attributes", {})
                primary_keys = state.get("primary_keys", {})
                foreign_keys = state.get("foreign_keys", [])
                domain = state.get("domain")
                relational_schema = state.get("relational_schema", {})
                
                # Step 7.1: Information Need Identification
                logger.info("Executing Step 7.1: Information Need Identification...")
                result_7_1 = await step_7_1_information_need_identification_with_loop(
                    nl_description=nl_description,
                    entities=entities,
                    relations=relations,
                    attributes=attributes,
                    primary_keys=primary_keys,
                    foreign_keys=foreign_keys,
                    domain=domain,
                )
                final_result_7_1 = result_7_1.get("final_result", {})
                information_needs = final_result_7_1.get("information_needs", [])
                
                # Step 7.2: SQL Generation and Validation
                validated_needs = []
                if information_needs:
                    logger.info("Executing Step 7.2: SQL Generation and Validation...")
                    result_7_2 = await step_7_2_sql_generation_and_validation_batch(
                        information_needs=information_needs,
                        relational_schema=relational_schema,
                        nl_description=nl_description
                    )
                    
                    # Only include information needs with valid SQL
                    validated_results = result_7_2.get("validated_info_needs", [])
                    validated_needs = [need for need in validated_results if need.get("is_valid", False)]
                else:
                    result_7_2 = {"validated_info_needs": [], "total_info_needs": 0, "valid_count": 0}
                
                state["information_needs"] = validated_needs
                state["current_step"] = "7.2"
                state["previous_answers"] = state.get("previous_answers", {})
                state["previous_answers"]["7.1"] = final_result_7_1
                state["previous_answers"]["7.2"] = result_7_2
                
                # Set checkpoint status
                job_manager.update_job(job_id, status="checkpoint_information_mining", state=state)
                logger.info(f"Checkpoint 'information_mining' reached. {len(validated_needs)} valid information needs with SQL queries.")
                
            elif checkpoint_type == "functional_dependencies":
                # Execute Phase 8: Functional Dependencies
                from NL2DATA.phases.phase8.step_8_1_functional_dependency_analysis import step_8_1_functional_dependency_analysis_batch
                
                logger.info("Executing Phase 8: Functional Dependencies...")
                
                entities = state.get("entities", [])
                attributes = state.get("attributes", {})
                relations = state.get("relations", [])
                primary_keys = state.get("primary_keys", {})
                relational_schema = state.get("relational_schema", {})
                domain = state.get("domain")
                derived_formulas = state.get("derived_formulas", {})
                
                # Convert attributes to entity -> attributes dict
                entity_attributes = {}
                for entity_name, attrs in attributes.items():
                    entity_attributes[entity_name] = attrs
                
                # Convert derived_formulas to entity -> derived_attr -> formula
                entity_derived_attributes = {}
                for attr_key, formula_info in derived_formulas.items():
                    if "." in attr_key:
                        entity_name, attr_name = attr_key.split(".", 1)
                        if entity_name not in entity_derived_attributes:
                            entity_derived_attributes[entity_name] = {}
                        entity_derived_attributes[entity_name][attr_name] = formula_info.get("formula", "")
                
                # Step 8.1: Functional Dependency Analysis
                logger.info("Executing Step 8.1: Functional Dependency Analysis...")
                result_8_1 = await step_8_1_functional_dependency_analysis_batch(
                    entities=entities,
                    entity_attributes=entity_attributes,
                    entity_primary_keys=primary_keys,
                    relational_schema=relational_schema,
                    entity_derived_attributes=entity_derived_attributes if entity_derived_attributes else None,
                    relations=relations,
                    nl_description=nl_description,
                    domain=domain,
                )
                
                # Extract functional dependencies from batch result
                entity_results = result_8_1.get("entity_results", {})
                all_fds = []
                for entity_name, entity_result in entity_results.items():
                    final_result = entity_result.get("final_result", {})
                    fds = final_result.get("functional_dependencies", [])
                    for fd in fds:
                        # Convert Pydantic model to dict if needed
                        if hasattr(fd, "dict"):
                            fd_dict = fd.dict()
                        elif hasattr(fd, "model_dump"):
                            fd_dict = fd.model_dump()
                        else:
                            fd_dict = fd if isinstance(fd, dict) else {"lhs": [], "rhs": []}
                        all_fds.append(fd_dict)
                
                state["functional_dependencies"] = all_fds
                state["current_step"] = "8.1"
                state["previous_answers"] = state.get("previous_answers", {})
                state["previous_answers"]["8.1"] = result_8_1
                
                # Set checkpoint status
                job_manager.update_job(job_id, status="checkpoint_functional_dependencies", state=state)
                logger.info(f"Checkpoint 'functional_dependencies' reached. {len(all_fds)} functional dependencies identified.")
                
            elif checkpoint_type == "constraints":
                # Execute Phase 8: Categorical Identification and Constraints Detection
                # Note: These functions are actually in Phase 8, not Phase 9
                from NL2DATA.phases.phase8.step_8_2_categorical_column_identification import step_8_2_categorical_column_identification_batch
                from NL2DATA.phases.phase8.step_8_4_constraint_detection import step_8_4_constraint_detection_with_loop
                from NL2DATA.phases.phase8.step_8_5_constraint_scope_analysis import step_8_5_constraint_scope_analysis_batch
                from NL2DATA.phases.phase8.step_8_6_constraint_enforcement_strategy import step_8_6_constraint_enforcement_strategy_batch
                
                logger.info("Executing Phase 9: Categorical Identification and Constraints Detection...")
                
                entities = state.get("entities", [])
                attributes = state.get("attributes", {})
                data_types = state.get("data_types", {})
                relational_schema = state.get("relational_schema", {})
                
                # Step 8.2: Categorical Column Identification
                logger.info("Executing Step 8.2: Categorical Column Identification...")
                result_8_2 = await step_8_2_categorical_column_identification_batch(
                    entities=entities,
                    entity_attributes=attributes,
                    entity_attribute_types=data_types,
                    relational_schema=relational_schema,
                    nl_description=nl_description,
                    domain=state.get("domain")
                )
                
                # Extract categorical attributes from result
                categorical_attributes = {}
                if hasattr(result_8_2, "entity_results"):
                    entity_results_8_2 = result_8_2.entity_results
                else:
                    entity_results_8_2 = result_8_2.get("entity_results", {})
                for entity_name, cat_result in entity_results_8_2.items():
                    if hasattr(cat_result, "categorical_attributes"):
                        categorical_attributes[entity_name] = cat_result.categorical_attributes
                    else:
                        categorical_attributes[entity_name] = cat_result.get("categorical_attributes", [])
                
                state["previous_answers"] = state.get("previous_answers", {})
                state["previous_answers"]["8.2"] = result_8_2
                state["categorical_attributes"] = categorical_attributes
                
                logger.info(f"Step 8.2 completed: Identified categorical attributes for {len(categorical_attributes)} entities")
                
                # Step 8.4: Constraint Detection (with loop)
                logger.info("Executing Step 8.4: Constraint Detection...")
                result_8_4 = await step_8_4_constraint_detection_with_loop(
                    nl_description=nl_description,
                    normalized_schema=relational_schema
                )
                
                # Extract constraints from result (flatten all categories)
                constraints = []
                for category in ["statistical_constraints", "structural_constraints", "distribution_constraints", "other_constraints"]:
                    category_constraints = result_8_4.get(category, [])
                    for constraint in category_constraints:
                        constraint["constraint_category"] = category
                        constraints.append(constraint)
                
                # Step 8.5: Constraint Scope Analysis
                logger.info("Executing Step 8.5: Constraint Scope Analysis...")
                result_8_5 = await step_8_5_constraint_scope_analysis_batch(
                    constraints=constraints,
                    normalized_schema=relational_schema
                )
                constraints_with_scope = []
                for constraint, scope_result in zip(constraints, result_8_5):
                    merged = {**constraint, **scope_result}
                    constraints_with_scope.append(merged)
                
                # Step 8.6: Constraint Enforcement Strategy
                logger.info("Executing Step 8.6: Constraint Enforcement Strategy...")
                result_8_6 = await step_8_6_constraint_enforcement_strategy_batch(
                    constraints_with_scope=constraints_with_scope,
                    normalized_schema=relational_schema
                )
                
                state["constraints"] = result_8_6  # Constraints with enforcement strategies
                state["current_step"] = "8.6"
                state["previous_answers"] = state.get("previous_answers", {})
                state["previous_answers"]["8.4"] = result_8_4
                state["previous_answers"]["8.5"] = result_8_5
                state["previous_answers"]["8.6"] = result_8_6
                
                # Set checkpoint status
                job_manager.update_job(job_id, status="checkpoint_constraints", state=state)
                logger.info(f"Checkpoint 'constraints' reached. {len(result_8_6)} constraints identified.")
                
            elif checkpoint_type == "generation_strategies":
                # Execute Phase 9: Generation Strategies (steps 9.1-9.6, after constraints)
                # Note: This should run the Phase 9 graph from numerical_ranges to distribution_compilation
                # For now, we'll use the graph directly
                from NL2DATA.orchestration.graphs.phase9 import create_phase_9_graph
                
                logger.info("Executing Phase 9: Generation Strategies (steps 9.1-9.6)...")
                
                # Ensure constraints checkpoint was reached first
                if not state.get("constraints"):
                    raise ValueError("Constraints must be detected before generation strategies. Please execute 'constraints' checkpoint first.")
                
                # Create and run Phase 9 graph starting from numerical_ranges
                # Note: The graph will run steps 9.6-9.11 automatically
                phase_9_graph = create_phase_9_graph()
                
                # Run the graph - it will execute from constraint_compilation through distribution_compilation
                # But we need to start from numerical_ranges, so we'll manually execute the steps
                # Note: Step numbers are 9.1-9.6, not 9.6-9.11
                from NL2DATA.phases.phase9.step_9_1_numerical_range_definition import step_9_1_numerical_range_definition_batch
                from NL2DATA.phases.phase9.step_9_2_text_generation_strategy import step_9_2_text_generation_strategy_batch
                from NL2DATA.phases.phase9.step_9_3_boolean_dependency_analysis import step_9_3_boolean_dependency_analysis_batch
                from NL2DATA.phases.phase9.step_9_4_data_volume_specifications import step_9_4_data_volume_specifications
                from NL2DATA.phases.phase9.step_9_5_partitioning_strategy import step_9_5_partitioning_strategy_batch
                from NL2DATA.phases.phase9.step_9_6_distribution_compilation import step_9_6_distribution_compilation
                
                relational_schema = state.get("relational_schema", {})
                data_types = state.get("data_types", {})
                derived_formulas = state.get("derived_formulas", {})
                constraints = state.get("constraints", [])
                entities = state.get("entities", [])
                
                # Extract attributes from relational schema (similar to graph logic)
                numerical_attributes = []
                text_attributes = []
                boolean_attributes = []
                
                for table in relational_schema.get("tables", []):
                    table_name = table.get("name", "")
                    entity_desc = ""
                    for entity in entities:
                        if (entity.get("name") if isinstance(entity, dict) else getattr(entity, "name", "")) == table_name:
                            entity_desc = entity.get("description", "") if isinstance(entity, dict) else getattr(entity, "description", "")
                            break
                    
                    for col in table.get("columns", []):
                        col_name = col.get("name", "")
                        sql_type = col.get("data_type") or col.get("type", "TEXT")
                        attr_key = f"{table_name}.{col_name}"
                        
                        # Skip if derived or constrained
                        if attr_key in derived_formulas:
                            continue
                        is_constrained = any(attr_key in c.get("affected_attributes", []) for c in constraints)
                        if is_constrained:
                            continue
                        
                        attr_meta = {
                            "entity_name": table_name,
                            "attribute_name": col_name,
                            "attribute_type": sql_type,
                            "attribute_description": col.get("description", ""),
                            "entity_description": entity_desc,
                            "domain": state.get("domain"),
                            "nl_description": nl_description,
                        }
                        
                        sql_type_upper = sql_type.upper()
                        if sql_type_upper in ("INTEGER", "INT", "BIGINT", "SMALLINT", "TINYINT", "DECIMAL", "NUMERIC", "FLOAT", "REAL", "DOUBLE"):
                            numerical_attributes.append(attr_meta)
                        elif sql_type_upper.startswith(("VARCHAR", "CHAR", "TEXT", "STRING")):
                            text_attributes.append(attr_meta)
                        elif sql_type_upper in ("BOOLEAN", "BOOL"):
                            boolean_attributes.append(attr_meta)
                
                # Step 9.1: Numerical Range Definition
                numerical_strategies = {}
                if numerical_attributes:
                    logger.info("Executing Step 9.1: Numerical Range Definition...")
                    result_9_1 = await step_9_1_numerical_range_definition_batch(
                        numerical_attributes=numerical_attributes,
                        constraints_map=None
                    )
                    numerical_strategies = result_9_1
                    state["previous_answers"] = state.get("previous_answers", {})
                    state["previous_answers"]["9.1"] = result_9_1
                
                # Step 9.2: Text Generation Strategy
                text_strategies = {}
                if text_attributes:
                    logger.info("Executing Step 9.2: Text Generation Strategy...")
                    result_9_2 = await step_9_2_text_generation_strategy_batch(
                        text_attributes=text_attributes,
                        generator_catalog=None
                    )
                    text_strategies = result_9_2
                    state["previous_answers"]["9.2"] = result_9_2
                
                # Step 9.3: Boolean Dependency Analysis
                boolean_strategies = {}
                if boolean_attributes:
                    logger.info("Executing Step 9.3: Boolean Dependency Analysis...")
                    result_9_3 = await step_9_3_boolean_dependency_analysis_batch(
                        boolean_attributes=boolean_attributes,
                        related_attributes_map=None,
                        dsl_grammar=None
                    )
                    boolean_strategies = result_9_3
                    state["previous_answers"]["9.3"] = result_9_3
                
                # Step 9.4: Data Volume Specifications
                logger.info("Executing Step 9.4: Data Volume Specifications...")
                result_9_4 = await step_9_4_data_volume_specifications(
                    entities=entities,
                    nl_description=nl_description
                )
                entity_volumes = result_9_4.get("entity_volumes", {})
                state["previous_answers"]["9.4"] = result_9_4
                
                # Step 9.5: Partitioning Strategy
                partitioning_strategies = {}
                if entity_volumes:
                    logger.info("Executing Step 9.5: Partitioning Strategy...")
                    result_9_5 = await step_9_5_partitioning_strategy_batch(
                        entities=entities,
                        entity_volumes=entity_volumes,
                        relational_schema=relational_schema,
                        nl_description=nl_description
                    )
                    partitioning_strategies = result_9_5.get("partitioning_strategies", {})
                    state["previous_answers"]["9.5"] = result_9_5
                
                # Step 9.6: Distribution Compilation
                logger.info("Executing Step 9.6: Distribution Compilation...")
                result_9_6 = await step_9_6_distribution_compilation(
                    numerical_strategies=numerical_strategies,
                    text_strategies=text_strategies,
                    boolean_strategies=boolean_strategies,
                    categorical_strategies=None,
                    entity_volumes=entity_volumes,
                    partitioning_strategies=partitioning_strategies
                )
                
                # Convert column_gen_specs to generation_strategies format
                generation_strategies = {}
                column_gen_specs = result_9_6.get("column_gen_specs", [])
                for spec in column_gen_specs:
                    table = spec.get("table", "")
                    column = spec.get("column", "")
                    if table and column:
                        if table not in generation_strategies:
                            generation_strategies[table] = {}
                        generation_strategies[table][column] = spec
                
                state["generation_strategies"] = generation_strategies
                state["numerical_strategies"] = numerical_strategies
                state["text_strategies"] = text_strategies
                state["boolean_strategies"] = boolean_strategies
                state["partitioning_strategies"] = partitioning_strategies
                state["current_step"] = "9.6"
                state["previous_answers"]["9.6"] = result_9_6
                
                # Set checkpoint status
                job_manager.update_job(job_id, status="checkpoint_generation_strategies", state=state)
                logger.info(f"Checkpoint 'generation_strategies' reached. Generation strategies prepared for {len(generation_strategies)} entities.")
                
            else:
                raise ValueError(f"Unknown checkpoint type: {checkpoint_type}")
            
            logger.info("=" * 80)
            logger.info(f"Checkpoint '{checkpoint_type}' execution completed")
            logger.info("=" * 80)
            
            return state
            
        except Exception as e:
            logger.error("=" * 80)
            logger.error(f"Error executing to checkpoint '{checkpoint_type}': {e}")
            logger.error("=" * 80)
            logger.exception("Full traceback:")
            job_manager.update_job(job_id, status="failed", error=str(e))
            raise

