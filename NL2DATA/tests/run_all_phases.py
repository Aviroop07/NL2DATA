"""Execute phases 1-N for a single NL description using the workflow graph.

Outputs:
- Pipeline log (LLM request/response pairs) under NL2DATA/runs/
- Run log under NL2DATA/runs/
- State JSON under NL2DATA/runs/
- Summary text file under NL2DATA/runs/

Command-line arguments:
- --desc-index: 1-based description index from nl_descriptions.txt (default: 1)
- --max-phase: Maximum phase to execute (1-9, default: 9 for all phases)
- --output-dir: Override output directory (optional)
"""

import argparse
import asyncio
import json
import re
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

try:
    # Ensure local .env is loaded when running this script directly.
    # This is required for LangSmith tracing and API keys when not exported in the shell environment.
    from dotenv import load_dotenv

    _repo_root = Path(__file__).parent.parent.parent
    load_dotenv(_repo_root / ".env")
except Exception:
    # Fail-open: pipeline should still run without dotenv.
    pass

from NL2DATA.orchestration.graphs.master import create_complete_workflow_graph, create_workflow_up_to_phase
from NL2DATA.orchestration.state import create_initial_state
from NL2DATA.utils.logging import get_logger, setup_logging
from NL2DATA.config import get_config
from NL2DATA.tests.utils.pipeline_logger import get_pipeline_logger
from NL2DATA.utils.observability import setup_langsmith


def read_nl_descriptions(file_path: str) -> list[str]:
    """Read all NL descriptions from a text file, separated by double newlines."""
    content = Path(file_path).read_text(encoding="utf-8")
    return [d.strip() for d in re.split(r"\r?\n\r?\n", content) if d.strip()]


def _ascii_safe(s: object) -> str:
    """Convert string to ASCII-safe format for Windows console."""
    try:
        txt = str(s or "")
    except Exception:
        txt = ""
    txt = txt.replace("\u2192", "->").replace("\u00d7", "x")
    try:
        return txt.encode("ascii", errors="replace").decode("ascii")
    except Exception:
        return str(txt)


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Run NL2DATA pipeline (phases 1-N) for a single NL description",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--desc-index",
        type=int,
        default=1,
        help="1-based description index from nl_descriptions.txt (default: 1)",
    )
    parser.add_argument(
        "--max-phase",
        type=int,
        default=9,
        help="Maximum phase to execute (1-9, default: 9 for all phases)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Override output directory (default: auto-generated timestamped directory)",
    )
    return parser.parse_args()


async def main() -> None:
    """Main execution function."""
    args = parse_args()
    
    # Get project root (3 levels up from NL2DATA/tests/)
    repo_root = Path(__file__).parent.parent.parent
    descriptions_file = repo_root / "nl_descriptions.txt"
    
    if not descriptions_file.exists():
        print(f"Error: nl_descriptions.txt not found at {descriptions_file}")
        sys.exit(1)
    
    descriptions = read_nl_descriptions(str(descriptions_file))
    
    desc_index_1b = max(1, args.desc_index)
    desc_index = desc_index_1b - 1
    
    if desc_index >= len(descriptions):
        print(f"Error: Description index {desc_index_1b} is out of range. Available: 1-{len(descriptions)}")
        sys.exit(1)
    
    nl_description = descriptions[desc_index]
    
    # Validate max_phase
    max_phase = max(1, min(9, args.max_phase))  # Clamp between 1 and 9
    if args.max_phase != max_phase:
        print(f"Warning: max-phase {args.max_phase} is out of range. Using {max_phase} instead.")
    
    print("=" * 80)
    if max_phase == 9:
        print(f"NL2DATA COMPLETE PIPELINE (PHASES 1-9) - NL DESCRIPTION #{desc_index_1b}")
    else:
        print(f"NL2DATA PIPELINE (PHASES 1-{max_phase}) - NL DESCRIPTION #{desc_index_1b}")
    print("=" * 80)
    print("\nNL Description:")
    # Print description in chunks to avoid console wrapping issues
    desc_lines = _ascii_safe(nl_description).split('\n')
    for line in desc_lines:
        print(line)
    print("\n" + "=" * 80)
    
    # Determine output directory
    if args.output_dir:
        run_dir = Path(args.output_dir)
        run_dir.mkdir(parents=True, exist_ok=True)
    else:
        # Auto-generate timestamped directory
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        if max_phase == 9:
            run_dir = repo_root / "NL2DATA" / "runs" / f"all_phases_desc_{desc_index_1b:03d}_{ts}"
        else:
            run_dir = repo_root / "NL2DATA" / "runs" / f"phases_1_{max_phase}_desc_{desc_index_1b:03d}_{ts}"
        run_dir.mkdir(parents=True, exist_ok=True)
    
    # Setup logging
    log_config = get_config('logging')
    setup_logging(
        level=log_config['level'],
        format_type=log_config['format'],
        log_to_file=True,
        log_file=str(run_dir / "run.log"),
        clear_existing=True,
    )
    
    # Setup pipeline logger
    pipeline_logger = get_pipeline_logger()
    pipeline_logger.initialize(output_dir=str(run_dir), filename="pipeline.log")
    
    # Set environment variable for database path generation
    import os
    os.environ["NL2DATA_RUN_DIR"] = str(run_dir)
    
    logger = get_logger(__name__)
    if max_phase == 9:
        logger.info("Starting complete workflow execution (Phases 1-9)")
    else:
        logger.info(f"Starting workflow execution (Phases 1-{max_phase})")

    # Ensure LangSmith is enabled if a key exists (loaded from .env above).
    # Project name is set per run so traces are easy to filter.
    setup_langsmith(project_name=f"nl2data_desc_{desc_index_1b:03d}_phases_1_{max_phase}")
    
    try:
        # Create initial state
        initial_state = create_initial_state(nl_description)
        logger.info("Initial state created")
        
        # Create workflow graph up to max_phase
        if max_phase == 9:
            workflow = create_complete_workflow_graph()
            logger.info("Complete workflow graph created (Phases 1-9)")
        else:
            workflow = create_workflow_up_to_phase(max_phase)
            logger.info(f"Workflow graph created (Phases 1-{max_phase})")
        
        # Execute workflow
        print("\nExecuting pipeline...")
        if max_phase == 9:
            print("This may take several minutes depending on the complexity of the description.\n")
        else:
            print(f"Executing phases 1-{max_phase}. This may take several minutes depending on the complexity.\n")
        
        # Create config with thread_id for checkpointer
        if max_phase == 9:
            thread_id = f"all_phases_desc_{desc_index_1b:03d}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        else:
            thread_id = f"phases_1_{max_phase}_desc_{desc_index_1b:03d}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        # Also increase recursion_limit so Phase 1 loops (and other graphs) don't crash
        # while we add deterministic convergence guardrails.
        config = {
            "configurable": {"thread_id": thread_id},
            "recursion_limit": 200,
        }
        
        start_time = datetime.now()
        final_state = await workflow.ainvoke(initial_state, config=config)
        end_time = datetime.now()
        
        duration = (end_time - start_time).total_seconds()
        
        logger.info(f"Pipeline execution completed in {duration:.2f} seconds")
        
        # Print summary
        print("\n" + "=" * 80)
        if max_phase == 9:
            print("PIPELINE EXECUTION COMPLETE (ALL PHASES)")
        else:
            print(f"PIPELINE EXECUTION COMPLETE (PHASES 1-{max_phase})")
        print("=" * 80)
        print(f"Duration: {duration:.2f} seconds ({duration/60:.2f} minutes)")
        print(f"Final Phase: {final_state.get('phase', 'Unknown')}")
        
        # Extract key results
        entities = final_state.get("entities", [])
        relations = final_state.get("relations", [])
        ddl_statements = final_state.get("ddl_statements", [])
        attributes = final_state.get("attributes", {})
        data_types = final_state.get("data_types", {})
        
        print(f"\nResults Summary:")
        print(f"  - Entities discovered: {len(entities)}")
        print(f"  - Relations discovered: {len(relations)}")
        print(f"  - DDL statements generated: {len(ddl_statements)}")
        
        if entities:
            print(f"\nEntities:")
            for entity in entities[:10]:  # Show first 10
                name = entity.get("name", "Unknown") if isinstance(entity, dict) else getattr(entity, "name", "Unknown")
                print(f"  - {name}")
            if len(entities) > 10:
                print(f"  ... and {len(entities) - 10} more")
        
        # Save full state
        state_file = run_dir / "state.json"
        with open(state_file, "w", encoding="utf-8") as f:
            json.dump(final_state, f, indent=2, ensure_ascii=True, default=str)
        print(f"\nFull state saved to: {state_file}")
        
        # Save summary
        summary_file = run_dir / "summary.txt"
        with open(summary_file, "w", encoding="utf-8") as f:
            if max_phase == 9:
                f.write("NL2DATA Pipeline Execution Summary (All Phases 1-9)\n")
            else:
                f.write(f"NL2DATA Pipeline Execution Summary (Phases 1-{max_phase})\n")
            f.write("=" * 80 + "\n\n")
            f.write(f"NL Description Index: {desc_index_1b}\n")
            f.write(f"Max Phase: {max_phase}\n")
            f.write(f"NL Description:\n{nl_description}\n\n")
            f.write(f"Execution Duration: {duration:.2f} seconds\n")
            f.write(f"Final Phase: {final_state.get('phase', 'Unknown')}\n\n")
            f.write(f"Entities: {len(entities)}\n")
            f.write(f"Relations: {len(relations)}\n")
            f.write(f"DDL Statements: {len(ddl_statements)}\n\n")
            
            if entities:
                f.write("Entities:\n")
                f.write("-" * 80 + "\n")
                for entity in entities:
                    name = entity.get("name", "Unknown") if isinstance(entity, dict) else getattr(entity, "name", "Unknown")
                    desc = entity.get("description", "") if isinstance(entity, dict) else getattr(entity, "description", "")
                    f.write(f"  - {name}")
                    if desc:
                        f.write(f": {desc}")
                    f.write("\n")
                f.write("\n")
            
            if ddl_statements:
                f.write("DDL Statements:\n")
                f.write("-" * 80 + "\n")
                for i, ddl in enumerate(ddl_statements, 1):
                    f.write(f"\n-- Statement {i}\n")
                    f.write(f"{ddl}\n")
        
        print(f"Summary saved to: {summary_file}")
        
        print("\n" + "=" * 80)
        print("SUCCESS")
        print("=" * 80)
        print(f"Run directory: {run_dir}")
        
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}", exc_info=True)
        print(f"\n{'=' * 80}")
        print("PIPELINE EXECUTION FAILED")
        print("=" * 80)
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
