"""Deprecated shim for Step 1.3: Domain Inference.

This module now forwards to the archived implementation under
`NL2DATA.phases.phase1.deprecated` and emits a deprecation warning.
"""

import warnings
from NL2DATA.phases.phase1.deprecated.step_1_3_domain_inference import (
    step_1_3_domain_inference as _archived_step_1_3,
)

warnings.warn(
    "step_1_3_domain_inference has been archived under phase1.deprecated; "
    "domain inference is handled by step_1_1_domain_detection. Importing the archived implementation.",
    DeprecationWarning,
)

async def step_1_3_domain_inference(*args, **kwargs):
    return await _archived_step_1_3(*args, **kwargs)

