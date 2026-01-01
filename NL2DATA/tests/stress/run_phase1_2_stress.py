"""Stress runner for Phase 1 + Phase 2 over all NL descriptions.

Standard outputs go under NL2DATA/runs/ (ignored by git).
LLM logs are captured as request/response pairs via PipelineLogger.
"""

import asyncio
import re
import sys
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any

repo_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(repo_root))

from NL2DATA.tests.integration_test import test_phases_1_2_3_4_5_6_7_integration
from NL2DATA.tests.utils.phase_timing import timer_start, timer_elapsed_seconds
from NL2DATA.tests.utils.pipeline_logger import get_pipeline_logger


def read_nl_descriptions(file_path: Path) -> List[str]:
    content = file_path.read_text(encoding="utf-8")
    return [d.strip() for d in re.split(r"\r?\n\r?\n", content) if d.strip()]


async def main() -> None:
    descriptions = read_nl_descriptions(repo_root / "nl_descriptions.txt")
    run_dir = repo_root / "NL2DATA" / "runs" / f"phase1_2_stress_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    run_dir.mkdir(parents=True, exist_ok=True)

    results: List[Dict[str, Any]] = []
    overall_t0 = timer_start()

    for idx, desc in enumerate(descriptions, start=1):
        desc_dir = run_dir / f"desc_{idx:03d}"
        desc_dir.mkdir(parents=True, exist_ok=True)

        pipeline_logger = get_pipeline_logger()
        pipeline_logger.initialize(output_dir=str(desc_dir), filename="pipeline.log")

        t0 = timer_start()
        try:
            out = await test_phases_1_2_3_4_5_6_7_integration(
                nl_description=desc,
                description_index=idx,
                max_phase=2,
                log_file_override=str(desc_dir / "run.log"),
                return_state=True,
            )
            ok = bool(out.get("success", False)) if isinstance(out, dict) else bool(out)
            results.append({"index": idx, "success": ok, "seconds": timer_elapsed_seconds(t0)})
        except Exception as e:
            results.append({"index": idx, "success": False, "seconds": timer_elapsed_seconds(t0), "error": str(e)})

    total_sec = timer_elapsed_seconds(overall_t0)
    (run_dir / "summary.json").write_text(
        __import__("json").dumps(
            {
                "total": len(descriptions),
                "passed": sum(1 for r in results if r["success"]),
                "failed": sum(1 for r in results if not r["success"]),
                "total_seconds": total_sec,
                "results": results,
            },
            indent=2,
            ensure_ascii=True,
        ),
        encoding="utf-8",
    )

    print(f"Done. Run dir: {run_dir}")


if __name__ == "__main__":
    asyncio.run(main())

