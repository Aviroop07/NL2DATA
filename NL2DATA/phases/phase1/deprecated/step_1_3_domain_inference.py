"""Phase 1, Step 1.3: Domain Inference.

Infers the domain when not explicitly stated in the description.
"""

from typing import Dict, Any, Optional, List
from pydantic import BaseModel, Field, ConfigDict

from NL2DATA.phases.phase1.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger
from NL2DATA.utils.tools.validation_tools import _verify_evidence_substring_impl

logger = get_logger(__name__)


class DomainAlternative(BaseModel):
    domain: str = Field(description="Alternative domain label (1–3 words)")
    confidence: float = Field(ge=0.0, le=1.0, description="Confidence for this alternative (0.0 to 1.0)")
    evidence: List[str] = Field(default_factory=list, description="Verbatim evidence phrases supporting this alternative")

    model_config = ConfigDict(extra="forbid")


class DomainInferenceOutput(BaseModel):
    """Output structure for domain inference."""
    primary_domain: str = Field(description="Primary inferred domain label (1–3 words)")
    confidence: float = Field(ge=0.0, le=1.0, description="Confidence score (0.0 to 1.0)")
    evidence: List[str] = Field(default_factory=list, description="2–5 verbatim evidence phrases copied from input")
    alternatives: List[DomainAlternative] = Field(default_factory=list, description="Up to 2 alternative domains")
    reasoning: str = Field(description="Reasoning (<= 35 words) referencing evidence")

    model_config = ConfigDict(extra="forbid")


@traceable_step("1.3", phase=1, tags=["domain_inference"])
async def step_1_3_domain_inference(
    nl_description: str,
    domain_detection_result: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Step 1.3: Infer the domain when not explicitly stated.
    
    This step provides context for understanding entity relationships and
    business rules when the domain is not explicitly mentioned.
    
    Note: This step should only be called if Step 1.1 determined that no
    explicit domain was found (has_explicit_domain == False).
    
    Args:
        nl_description: Natural language description of the database requirements
        domain_detection_result: Optional result from Step 1.1 for context
        
    Returns:
        dict: Domain inference result with domain, confidence, and reasoning
        
    Example:
        >>> result = await step_1_3_domain_inference("I need to track customer orders and products")
        >>> result["domain"]
        "e-commerce"
        >>> result["confidence"]
        0.85
    """
    logger.info("Starting Step 1.3: Domain Inference")
    logger.debug(f"Input description length: {len(nl_description)} characters")
    if domain_detection_result and domain_detection_result.get("has_explicit_domain"):
        logger.warning("Step 1.3 called but domain was already explicitly detected. Proceeding anyway.")
    
    system_prompt = """You are a database design assistant.

Task
Infer the most likely business domain(s) from the given natural-language description when the domain is not explicitly stated.

Key rule: evidence-grounded inference
- You may infer the domain from context, but you MUST justify the inference using 2–5 verbatim evidence phrases copied from the input.
- Do NOT use prior analysis or external knowledge about the user/request beyond the input text.

Output (STRICT JSON ONLY; no extra keys; no extra text)
{
  "primary_domain": string,
  "confidence": number,
  "evidence": string[],
  "alternatives": [{"domain": string, "confidence": number, "evidence": string[]}],
  "reasoning": string
}

Labeling rules
- primary_domain must be a short, stable label (1–3 words). Avoid slashes. Avoid parentheses.
- If you want to add nuance, put it in reasoning (not in the domain label).
- Provide up to 2 alternatives if ambiguity exists.

Confidence rubric (must follow)
- 0.90–1.00: multiple strong, domain-specific cues; little plausible ambiguity
- 0.70–0.89: strong cues but at least one plausible alternate domain
- 0.40–0.69: mixed or generic cues; several plausible alternates
- 0.00–0.39: weak evidence; domain largely speculative → use "general" if needed

Constraints
- confidence must be between 0.0 and 1.0 inclusive.
- evidence phrases must be exact substrings from the input.
- reasoning must be <= 35 words and must reference the evidence.

No extra text. No markdown. No code fences."""
    
    # Human prompt template
    human_prompt = "Natural language description:\n{nl_description}"
    
    # Initialize model
    llm = get_model_for_step("1.3")  # Step 1.3 maps to "important" task type
    
    try:
        logger.debug("Invoking LLM for domain inference")
        config = get_trace_config("1.3", phase=1, tags=["domain_inference"])
        result: DomainInferenceOutput = await standardized_llm_call(
            llm=llm,
            output_schema=DomainInferenceOutput,
            system_prompt=system_prompt,
            human_prompt_template=human_prompt,
            input_data={"nl_description": nl_description},
            tools=None,
            use_agent_executor=False,
            config=config,
        )

        # Deterministic evidence enforcement:
        # Keep only evidence phrases that are verbatim substrings. If none remain, fall back to "general".
        cleaned_evidence: List[str] = []
        seen: set[str] = set()
        for ev in result.evidence or []:
            evs = (ev or "").strip()
            if not evs:
                continue
            if not _verify_evidence_substring_impl(evs, nl_description).get("is_substring", False):
                continue
            key = evs.lower()
            if key in seen:
                continue
            seen.add(key)
            cleaned_evidence.append(evs)

        cleaned_alts: List[DomainAlternative] = []
        for alt in result.alternatives or []:
            alt_evidence: List[str] = []
            seen_alt: set[str] = set()
            for ev in alt.evidence or []:
                evs = (ev or "").strip()
                if not evs:
                    continue
                if not _verify_evidence_substring_impl(evs, nl_description).get("is_substring", False):
                    continue
                key = evs.lower()
                if key in seen_alt:
                    continue
                seen_alt.add(key)
                alt_evidence.append(evs)
            cleaned_alts.append(alt.model_copy(update={"evidence": alt_evidence}))

        if not cleaned_evidence:
            # If the model can't ground evidence, keep the pipeline stable and conservative.
            result = result.model_copy(
                update={
                    "primary_domain": "general",
                    "confidence": min(float(result.confidence or 0.0), 0.39),
                    "evidence": [],
                    "alternatives": [],
                    "reasoning": "Insufficient grounded evidence phrases; defaulting to general.",
                }
            )
        else:
            result = result.model_copy(update={"evidence": cleaned_evidence, "alternatives": cleaned_alts})
        
        # Work with Pydantic model directly
        logger.info(f"Domain inference completed: primary_domain={result.primary_domain}, confidence={result.confidence:.2f}")
        if result.reasoning:
            logger.debug(f"Reasoning: {result.reasoning}")
        
        # Convert to dict only at return boundary
        return result.model_dump()
        
    except Exception as e:
        logger.error(f"Error in domain inference: {e}", exc_info=True)
        raise
