"""NL2DATA pipeline service wrapper."""

import logging
import time
from typing import Dict, Any
from NL2DATA.orchestration.graphs.master import get_phase_graph
from NL2DATA.orchestration.state import create_initial_state, IRGenerationState

from backend.utils.websocket_manager import WebSocketManager
from backend.utils.job_manager import JobManager
from backend.services.status_ticker_service import StatusTickerService

logger = logging.getLogger(__name__)


class NL2DataService:
    """Wraps NL2DATA pipeline and emits WebSocket events."""
    
    def __init__(
        self,
        ws_manager: WebSocketManager,
        job_manager: JobManager,
        status_ticker: StatusTickerService
    ):
        self.ws_manager = ws_manager
        self.job_manager = job_manager
        self.status_ticker = status_ticker
        # We stream per-phase graphs directly so we can emit a status tick per step (1.1, 1.2, ...)
    
    async def process_with_events(
        self,
        job_id: str,
        nl_description: str,
        ws_manager: WebSocketManager,
        job_manager: JobManager
    ):
        """
        Run pipeline and emit WebSocket events for each step.
        
        IMPORTANT: The pipeline is a compiled LangGraph that uses astream().
        We intercept at the phase level (each phase is a node) and emit events.
        For step-level events, we need to hook into the standardized_llm_call wrapper.
        """
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

            for phase in range(1, 8):
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
                        
                        # If there was a previous step, send completion for it
                        if last_state_for_step is not None and prev_step:
                            await self.status_ticker.send_step_complete(
                                job_id=job_id,
                                phase=last_phase or phase,
                                step=prev_step,
                                state=last_state_for_step,
                                ws_manager=ws_manager,
                            )
                        # Send start for current step
                        await self.status_ticker.send_step_start(
                            job_id=job_id,
                            phase=phase,
                            step=step,
                            state=phase_last_state,
                            ws_manager=ws_manager,
                        )
                        last_step_sent = step

                        logger.info("-" * 80)
                        logger.info(f"STEP UPDATE #{state_update_count}: Phase {phase} Step {step}")

                        # Update job state
                        job_manager.update_job_state(job_id, phase_last_state)
                        job_manager.update_job(job_id, phase=phase, step=step)
                        last_state_for_step = phase_last_state
                        last_phase = phase

                        # Send status tick
                        await self.status_ticker.send_tick(
                            job_id=job_id,
                            phase=phase,
                            step=step,
                            state=phase_last_state,
                            ws_manager=ws_manager,
                        )
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
                        
                        if last_state_for_step is not None and prev_step:
                            await self.status_ticker.send_step_complete(
                                job_id=job_id,
                                phase=last_phase or phase,
                                step=prev_step,
                                state=last_state_for_step,
                                ws_manager=ws_manager,
                            )
                        await self.status_ticker.send_step_start(
                            job_id=job_id,
                            phase=phase,
                            step=step,
                            state=state,
                            ws_manager=ws_manager,
                        )
                        last_step_sent = step

                        logger.info("-" * 80)
                        logger.info(f"STEP UPDATE #{state_update_count}: Phase {phase} Step {step} (node: {node_name})")

                        job_manager.update_job_state(job_id, state)
                        job_manager.update_job(job_id, phase=phase, step=step)
                        last_state_for_step = state
                        last_phase = phase

                        await self.status_ticker.send_tick(
                            job_id=job_id,
                            phase=phase,
                            step=step,
                            state=state,
                            ws_manager=ws_manager,
                        )

                # Carry state forward to next phase
                if phase_last_state is not None:
                    state = phase_last_state
                # Update phase marker for UX/logging (phase graphs don't always set it)
                state["phase"] = min(phase + 1, 7)

            # Final state is whatever state we ended with after phase streaming
            final_state_dict = state
            # Send completion for the last step observed
            if last_state_for_step is not None and last_step_sent:
                await self.status_ticker.send_step_complete(
                    job_id=job_id,
                    phase=last_phase or 7,
                    step=last_step_sent,
                    state=last_state_for_step,
                    ws_manager=ws_manager,
                )
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
            
            # Send error event
            logger.info("Sending error event via WebSocket...")
            await ws_manager.send_event(job_id, {
                "type": "error",
                "data": {
                    "job_id": job_id,
                    "message": f"Processing failed: {str(e)}",
                    "error_type": "processing_error"
                }
            })
            logger.error("=" * 80)
            raise

