"""Phase 1, Step 1.1: Domain Detection.

Identifies if the NL description explicitly mentions a business domain.
"""

from typing import Dict, Any
from pydantic import BaseModel, Field, ConfigDict

from NL2DATA.phases.phase1.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger
from NL2DATA.utils.tools import verify_evidence_substring

logger = get_logger(__name__)


class DomainDetectionOutput(BaseModel):
    """Output structure for domain detection."""
    has_explicit_domain: bool = Field(description="Whether domain is explicitly mentioned")
    domain: str = Field(default="", description="Exact domain substring if found, empty string otherwise")
    evidence: str = Field(default="", description="Exact evidence substring from input if found, empty string otherwise")
    reasoning: str = Field(description="Reasoning for the detection (<= 25 words)")

    model_config = ConfigDict(extra="forbid")


@traceable_step("1.1", phase=1, tags=["domain_detection"])
async def step_1_1_domain_detection(nl_description: str) -> Dict[str, Any]:
    """
    Step 1.1: Detect if the natural language description explicitly mentions a business domain.
    
    Args:
        nl_description: Natural language description of the database requirements
        
    Returns:
        dict: Domain detection result with has_explicit_domain, domain, and reasoning
        
    Example:
        >>> result = await step_1_1_domain_detection("I need a database for an e-commerce store")
        >>> result["has_explicit_domain"]
        True
        >>> result["domain"]
        "e-commerce"
    """
    logger.info("Starting Step 1.1: Domain Detection")
    logger.debug(f"Input description length: {len(nl_description)} characters")
    
    # System prompt (strict, evidence-grounded)
    system_prompt = """You are a database design assistant.

Goal
Determine whether the user's description EXPLICITLY NAMES a business domain.

Definition (critical)
"Explicitly names" means the domain is present as a VERBATIM phrase in the input text.
You MUST NOT infer a domain from context. For example, phrases like "industrial sensors", "plants", "telemetry", "IoT" do NOT count as explicit domain names unless the input literally contains a domain name term (e.g., "manufacturing", "healthcare", "e-commerce", "banking", "insurance", "hospital", "university", etc.).

Decision rule
- has_explicit_domain = true ONLY if you can copy an exact domain phrase from the input.
- If has_explicit_domain = true, domain MUST be an exact substring copied from the input (case preserved).
- If multiple domain phrases appear, choose the most specific one that best represents the primary business domain.
- If none appear, set has_explicit_domain = false and domain = "" (empty string).

Output constraints
Return ONLY a single JSON object, with EXACTLY these keys and types:
{
  "has_explicit_domain": boolean,
  "domain": string,
  "evidence": string,
  "reasoning": string
}

Grounding rule (critical)
- If has_explicit_domain = true, evidence MUST be the exact substring copied verbatim from the input (preserve casing/spaces).
- If has_explicit_domain = false, evidence MUST be "" (empty string).

Tool usage (mandatory when has_explicit_domain = true)
You have access to: verify_evidence_substring(evidence: str, nl_description: str) -> {is_substring: bool, error: str|null}
Before finalizing your response:
1) If has_explicit_domain = true, call verify_evidence_substring with:
   {"evidence": "<your_evidence>", "nl_description": "<full_nl_description>"}
2) If is_substring = false, correct your evidence to be an exact substring from nl_description, then re-check.

Reasoning constraints
- Keep reasoning <= 25 words.
- If has_explicit_domain = true, reasoning MUST quote the exact phrase used for domain (verbatim).
- If has_explicit_domain = false, reasoning MUST state that no domain was explicitly named.

No extra text. No markdown. No code fences."""
    
    # Human prompt template
    human_prompt = "Natural language description:\n{nl_description}"
    
    # Initialize model
    llm = get_model_for_step("1.1")  # Step 1.1 maps to "simple" task type
    
    try:
        logger.debug("Invoking LLM for domain detection")
        # Get trace config with metadata (LangChain best practice)
        config = get_trace_config("1.1", phase=1, tags=["domain_detection"])
        result: DomainDetectionOutput = await standardized_llm_call(
            llm=llm,
            output_schema=DomainDetectionOutput,
            system_prompt=system_prompt,
            human_prompt_template=human_prompt,
            input_data={"nl_description": nl_description},
            tools=[verify_evidence_substring],
            use_agent_executor=True,
            config=config,
        )
        
        # Work with Pydantic model directly
        logger.info(f"Domain detection completed: has_explicit_domain={result.has_explicit_domain}")
        if result.domain:
            logger.info(f"Detected domain: {result.domain}")
        if result.reasoning:
            logger.debug(f"Reasoning: {result.reasoning}")
        
        # Convert to dict only at return boundary
        return result.model_dump()
        
    except Exception as e:
        logger.error(f"Error in domain detection: {e}", exc_info=True)
        raise

