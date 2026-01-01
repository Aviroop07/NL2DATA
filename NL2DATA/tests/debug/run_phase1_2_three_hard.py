"""Run Phase 1 + Phase 2 for three difficult NL descriptions with standardized logging.

Outputs are written under NL2DATA/runs/ (ignored by git):
- desc_###/pipeline.log  (LLM request/response pairs)
- desc_###/run.log       (phase timing + minimal run logs)
- desc_###/state.json    (full state snapshot for inspection)
"""

import asyncio
import json
import re
import sys
from pathlib import Path
from typing import List

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.tests.integration_test import test_phases_1_2_3_4_5_6_7_integration
from NL2DATA.tests.utils.pipeline_logger import get_pipeline_logger


def read_nl_descriptions(file_path: Path) -> List[str]:
    content = file_path.read_text(encoding="utf-8")
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


async def _run_one(*, repo_root: Path, idx: int, nl_description: str, base_dir: Path) -> None:
    out_dir = base_dir / f"desc_{idx:03d}"
    out_dir.mkdir(parents=True, exist_ok=True)

    pipeline_logger = get_pipeline_logger()
    pipeline_logger.initialize(output_dir=str(out_dir), filename="pipeline.log")

    result = await test_phases_1_2_3_4_5_6_7_integration(
        nl_description=nl_description,
        description_index=idx,
        max_phase=2,
        log_file_override=str(out_dir / "run.log"),
        return_state=True,
    )

    state = (result.get("state") or {}) if isinstance(result, dict) else {}
    with open(out_dir / "state.json", "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=True, default=str)


async def main() -> None:
    repo_root = Path(__file__).parent.parent.parent.parent
    descriptions = read_nl_descriptions(repo_root / "nl_descriptions.txt")

    # Three "hard" ones (varied difficulty):
    # 10: multi-touch journeys (many entities + temporal/event complexity)
    # 33: global supply chain shipments with disruptions (many entities + processes)
    # 34: longitudinal healthcare with readmissions (domain constraints + many entities)
    picks = [10, 33, 34]

    run_dir = repo_root / "NL2DATA" / "runs" / "phase1_2_three_hard"
    run_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 80)
    print("Running Phase 1+2 for 3 difficult NL descriptions")
    print("=" * 80)
    print(f"Run dir: {run_dir}")
    print("")

    for idx in picks:
        nl = descriptions[idx - 1]
        print("-" * 80)
        print(f"NL #{idx} preview: {_ascii_safe(nl[:140])}...")
        await _run_one(repo_root=repo_root, idx=idx, nl_description=nl, base_dir=run_dir)
        print(f"Done NL #{idx}. Outputs: {run_dir / f'desc_{idx:03d}'}")

    print("-" * 80)
    print("Done.")


if __name__ == "__main__":
    asyncio.run(main())

