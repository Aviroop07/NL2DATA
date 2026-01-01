"""Run Phase 1 + Phase 2 for a single NL description (debug runner).

Outputs:
- Pipeline log (LLM request/response pairs) under NL2DATA/runs/
- Optional state JSON under NL2DATA/runs/
"""

import asyncio
import json
import re
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.tests.integration_test import test_phases_1_2_3_4_5_6_7_integration
from NL2DATA.tests.utils.pipeline_logger import get_pipeline_logger


def read_nl_descriptions(file_path: str) -> list[str]:
    content = Path(file_path).read_text(encoding="utf-8")
    return [d.strip() for d in re.split(r"\r?\n\r?\n", content) if d.strip()]


def _ascii_safe(s: object) -> str:
    try:
        txt = str(s or "")
    except Exception:
        txt = ""
    txt = txt.replace("\u2192", "->").replace("\u00d7", "x")
    try:
        return txt.encode("ascii", errors="replace").decode("ascii")
    except Exception:
        return str(txt)


async def main() -> None:
    repo_root = Path(__file__).parent.parent.parent.parent
    descriptions_file = repo_root / "nl_descriptions.txt"
    descriptions = read_nl_descriptions(str(descriptions_file))

    desc_index = 0
    nl_description = descriptions[desc_index]

    print("=" * 80)
    print(f"DEBUG PHASE 1 + 2 FOR NL DESCRIPTION #{desc_index + 1}")
    print("=" * 80)
    print(f"\nNL Description:\n{_ascii_safe(nl_description)}\n")

    # Option A: always write to a fresh run directory to avoid appending to old logs.
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = repo_root / "NL2DATA" / "runs" / f"debug_desc_{desc_index + 1:03d}_{ts}"
    run_dir.mkdir(parents=True, exist_ok=True)

    pipeline_logger = get_pipeline_logger()
    pipeline_logger.initialize(output_dir=str(run_dir), filename="pipeline.log")

    result = await test_phases_1_2_3_4_5_6_7_integration(
        nl_description=nl_description,
        description_index=desc_index + 1,
        max_phase=2,
        log_file_override=str(run_dir / "run.log"),
        return_state=True,
    )

    success = bool(result.get("success", False)) if isinstance(result, dict) else bool(result)
    state = (result.get("state") or {}) if isinstance(result, dict) else {}

    # Save full state for inspection (not a log)
    with open(run_dir / "state.json", "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=True, default=str)

    print("\n" + "=" * 80)
    print("DONE")
    print("=" * 80)
    print(f"Success: {success}")
    print(f"Run dir: {run_dir}")


if __name__ == "__main__":
    asyncio.run(main())

