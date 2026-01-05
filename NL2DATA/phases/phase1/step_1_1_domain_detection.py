"""Phase 1, Step 1.1: Domain Detection & Inference.

Identifies if the NL description explicitly mentions a business domain,
or infers the domain from context if not explicitly stated.
"""

from typing import List
from pydantic import BaseModel, Field, ConfigDict

from NL2DATA.phases.phase1.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger
from NL2DATA.utils.tools.validation_tools import _verify_evidence_substring_impl
from NL2DATA.utils.prompt_helpers import generate_output_structure_section_with_custom_requirements

logger = get_logger(__name__)


class DomainAlternative(BaseModel):
    """Alternative domain option with confidence and evidence."""
    domain: str = Field(description="Alternative domain label (1–3 words)")
    confidence: float = Field(ge=0.0, le=1.0, description="Confidence for this alternative (0.0 to 1.0)")
    evidence: List[str] = Field(default_factory=list, description="Verbatim evidence phrases supporting this alternative")

    model_config = ConfigDict(extra="forbid")


class DomainDetectionAndInferenceOutput(BaseModel):
    """Output structure for domain detection and inference."""
    has_explicit_domain: bool = Field(description="Whether domain is explicitly mentioned")
    domain: str = Field(description="Final domain (explicit if found, inferred otherwise)")
    explicit_domain: str = Field(default="", description="Exact domain substring if explicitly found, empty string otherwise")
    explicit_evidence: str = Field(default="", description="Exact evidence substring from input if explicitly found, empty string otherwise")
    confidence: float = Field(default=1.0, ge=0.0, le=1.0, description="Confidence score (1.0 for explicit, 0.0-1.0 for inferred)")
    inference_evidence: List[str] = Field(default_factory=list, description="2–5 verbatim evidence phrases for inference (if inferred)")
    alternatives: List[DomainAlternative] = Field(default_factory=list, description="Up to 2 alternative domains (if inferred)")
    reasoning: str = Field(description="Reasoning for the detection/inference (<= 35 words)")

    model_config = ConfigDict(extra="forbid")


@traceable_step("1.1", phase=1, tags=["domain_detection", "domain_inference"])
async def step_1_1_domain_detection(nl_description: str) -> DomainDetectionAndInferenceOutput:
    """
    Step 1.1: Detect if the natural language description explicitly mentions a business domain,
    or infer the domain from context if not explicitly stated.
    
    This merged step combines the functionality of the previous steps 1.1 (explicit detection)
    and 1.3 (inference) into a single step that handles both cases.
    
    Args:
        nl_description: Natural language description of the database requirements
        
    Returns:
        dict: Domain detection/inference result with:
            - has_explicit_domain: bool
            - domain: str (explicit domain if found, inferred domain otherwise)
            - explicit_domain: str (only set if explicitly found)
            - explicit_evidence: str (only set if explicitly found)
            - confidence: float (1.0 for explicit, 0.0-1.0 for inferred)
            - inference_evidence: List[str] (only set if inferred)
            - alternatives: List[DomainAlternative] (only set if inferred)
            - reasoning: str
        
    Example:
        >>> result = await step_1_1_domain_detection("I need a database for an e-commerce store")
        >>> result["has_explicit_domain"]
        True
        >>> result["domain"]
        "e-commerce"
        >>> result["confidence"]
        1.0
        
        >>> result = await step_1_1_domain_detection("I need to track customer orders and products")
        >>> result["has_explicit_domain"]
        False
        >>> result["domain"]
        "e-commerce"
        >>> result["confidence"] < 1.0
        True
    """
    logger.info("Starting Step 1.1: Domain Detection & Inference")
    logger.debug(f"Input description length: {len(nl_description)} characters")
    
    # Generate output structure section from Pydantic model
    output_structure_section = generate_output_structure_section_with_custom_requirements(
        output_schema=DomainDetectionAndInferenceOutput,
        additional_requirements=[
            "Grounding rules (critical):",
            "  - If has_explicit_domain = true: explicit_evidence MUST be exact substring from input, domain MUST equal explicit_domain, confidence MUST be 1.0",
            "  - If has_explicit_domain = false: inference_evidence phrases MUST be exact substrings from input, explicit_domain and explicit_evidence MUST be empty strings",
            "Reasoning must be <= 35 words and quote exact phrases when explicit"
        ]
    )
    
    # System prompt that handles both explicit detection and inference
    system_prompt = """You are a database design assistant.

Goal
Determine the business domain from the user's description. First, check if the domain is EXPLICITLY NAMED. If not, INFER it from context.

Step 1: Explicit Detection
- Check if the description EXPLICITLY NAMES a business domain.
- "Explicitly names" means the domain is present as a VERBATIM phrase in the input text.
- You MUST NOT infer a domain from context at this stage.
- Examples of explicit domain phrases: "e-commerce", "healthcare", "IoT", "manufacturing", "banking", "insurance", "hospital", "university", etc.
- If has_explicit_domain = true:
  - domain MUST be an exact substring copied from the input (case preserved).
  - explicit_domain MUST be the same as domain.
  - explicit_evidence MUST be the exact substring copied verbatim from the input.
  - confidence MUST be 1.0.
  - inference_evidence and alternatives should be empty lists.

Step 2: Inference (only if has_explicit_domain = false)
- Infer the most likely business domain(s) from context.
- You MUST justify the inference using 2–5 verbatim evidence phrases copied from the input.
- domain should be the primary_domain (short, stable label, 1–3 words).
- confidence should follow this rubric:
  - 0.90–1.00: multiple strong, domain-specific cues; little plausible ambiguity
  - 0.70–0.89: strong cues but at least one plausible alternate domain
  - 0.40–0.69: mixed or generic cues; several plausible alternates
  - 0.00–0.39: weak evidence; domain largely speculative → use "general" if needed
- inference_evidence must contain 2–5 verbatim evidence phrases from the input.
- Provide up to 2 alternatives if ambiguity exists.
- explicit_domain and explicit_evidence should be empty strings.

""" + output_structure_section
    
    # Human prompt template
    human_prompt = "Natural language description:\n{nl_description}"
    
    # Initialize model
    llm = get_model_for_step("1.1")  # Step 1.1 maps to "simple" task type
    
    try:
        logger.debug("Invoking LLM for domain detection and inference")
        config = get_trace_config("1.1", phase=1, tags=["domain_detection", "domain_inference"])
        result: DomainDetectionAndInferenceOutput = await standardized_llm_call(
            llm=llm,
            output_schema=DomainDetectionAndInferenceOutput,
            system_prompt=system_prompt,
            human_prompt_template=human_prompt,
            input_data={"nl_description": nl_description},
            tools=None,
            use_agent_executor=False,
            config=config,
        )

        # Deterministic grounding enforcement
        if result.has_explicit_domain:
            # Verify explicit evidence is a substring
            if result.explicit_evidence:
                check = _verify_evidence_substring_impl(result.explicit_evidence, nl_description)
                if not check.get("is_substring", False):
                    # Evidence not found, fall back to inference
                    logger.warning("Explicit domain evidence not found in input, falling back to inference")
                    result = result.model_copy(
                        update={
                            "has_explicit_domain": False,
                            "explicit_domain": "",
                            "explicit_evidence": "",
                            "confidence": 0.5,  # Lower confidence since explicit detection failed
                        }
                    )
                else:
                    # Ensure domain matches explicit_domain when explicitly found
                    if result.domain != result.explicit_domain:
                        result = result.model_copy(update={"domain": result.explicit_domain})
                    # Ensure confidence is 1.0 for explicit domains
                    if result.confidence != 1.0:
                        result = result.model_copy(update={"confidence": 1.0})
                    # Clear inference fields
                    result = result.model_copy(update={
                        "inference_evidence": [],
                        "alternatives": []
                    })
        else:
            # Inference case: verify evidence phrases are substrings
            cleaned_evidence: List[str] = []
            seen: set[str] = set()
            for ev in result.inference_evidence or []:
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
                        "domain": "general",
                        "confidence": min(float(result.confidence or 0.0), 0.39),
                        "inference_evidence": [],
                        "alternatives": [],
                        "reasoning": "Insufficient grounded evidence phrases; defaulting to general.",
                    }
                )
            else:
                result = result.model_copy(update={
                    "inference_evidence": cleaned_evidence,
                    "alternatives": cleaned_alts
                })
            
            # Ensure explicit fields are empty for inference case
            if result.explicit_domain or result.explicit_evidence:
                result = result.model_copy(update={
                    "explicit_domain": "",
                    "explicit_evidence": ""
                })
        
        # Work with Pydantic model directly
        logger.info(f"Domain detection/inference completed: has_explicit_domain={result.has_explicit_domain}, domain={result.domain}, confidence={result.confidence:.2f}")
        if result.has_explicit_domain:
            logger.info(f"Explicit domain detected: {result.domain}")
        else:
            logger.info(f"Inferred domain: {result.domain} (confidence: {result.confidence:.2f})")
        if result.reasoning:
            logger.debug(f"Reasoning: {result.reasoning}")
        
        return result
        
    except Exception as e:
        logger.error(f"Error in domain detection and inference: {e}", exc_info=True)
        raise

