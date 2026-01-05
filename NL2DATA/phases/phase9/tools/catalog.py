"""Tool catalog definitions - JSON schema for LLM tool calling."""

from typing import List, Literal, Optional, Any, Dict
from pydantic import BaseModel, Field


class ToolParameter(BaseModel):
    """Parameter definition for a tool."""
    name: str = Field(description="Parameter name")
    type: Literal["string", "integer", "number", "boolean", "array", "object"] = Field(description="Parameter type")
    description: str = Field(description="Parameter description")
    required: bool = Field(default=True, description="Whether parameter is required")
    default: Optional[Any] = Field(default=None, description="Default value if not required")
    enum: Optional[List[Any]] = Field(default=None, description="Allowed values (for constrained choices)")


class GenerationToolDefinition(BaseModel):
    """Complete tool definition for the catalog."""
    name: str = Field(description="Tool name (stable identifier)")
    description: str = Field(description="What this tool does and when to use it")
    kind: Literal["distribution", "string", "location", "datetime"] = Field(description="Strategy kind")
    parameters: List[ToolParameter] = Field(description="Function parameters as tool arguments")
    returns: str = Field(description="What this tool returns (for LLM context)")


# ============================================================================
# TOOL CATALOG (All Available Generation Tools)
# ============================================================================

GENERATION_TOOL_CATALOG: Dict[str, GenerationToolDefinition] = {
    # ========================================================================
    # NUMERIC DISTRIBUTIONS
    # ========================================================================
    "generate_normal": GenerationToolDefinition(
        name="generate_normal",
        description=(
            "Generate values from a normal (Gaussian) distribution. "
            "Use for attributes that follow a bell curve (e.g., heights, test scores, measurement errors). "
            "Values cluster around the mean with symmetric tails."
        ),
        kind="distribution",
        parameters=[
            ToolParameter(name="mu", type="number", description="Mean (center) of the distribution", required=True),
            ToolParameter(name="sigma", type="number", description="Standard deviation (spread), must be > 0", required=True),
            ToolParameter(name="min", type="number", description="Optional minimum clamp", required=False),
            ToolParameter(name="max", type="number", description="Optional maximum clamp", required=False),
        ],
        returns="List of numeric values following normal distribution"
    ),
    
    "generate_lognormal": GenerationToolDefinition(
        name="generate_lognormal",
        description=(
            "Generate values from a log-normal distribution. "
            "Use for positive attributes with multiplicative effects (e.g., transaction amounts, incomes, prices). "
            "Most values are small, but a few are very large. The logarithm of values follows a normal distribution."
        ),
        kind="distribution",
        parameters=[
            ToolParameter(name="mu", type="number", description="Mean of underlying normal (log-space)", required=True),
            ToolParameter(name="sigma", type="number", description="Standard deviation of underlying normal (log-space), must be > 0", required=True),
            ToolParameter(name="min", type="number", description="Minimum value (default 0, must be >= 0)", required=False, default=0.0),
            ToolParameter(name="max", type="number", description="Optional maximum clamp", required=False),
        ],
        returns="List of positive numeric values following log-normal distribution"
    ),
    
    "generate_uniform": GenerationToolDefinition(
        name="generate_uniform",
        description=(
            "Generate values from a uniform distribution. "
            "Use for attributes with no natural clustering (e.g., random IDs, quantities, percentages). "
            "All values in the range are equally probable."
        ),
        kind="distribution",
        parameters=[
            ToolParameter(name="min", type="number", description="Minimum value (inclusive)", required=True),
            ToolParameter(name="max", type="number", description="Maximum value (inclusive, must be > min)", required=True),
        ],
        returns="List of numeric values uniformly distributed in [min, max]"
    ),
    
    "generate_pareto": GenerationToolDefinition(
        name="generate_pareto",
        description=(
            "Generate values from a Pareto (power-law) distribution. "
            "Use for attributes following power-law (e.g., wealth distribution, file sizes, city populations). "
            "Most values are small, but tail extends very far. 80/20 rule applies."
        ),
        kind="distribution",
        parameters=[
            ToolParameter(name="alpha", type="number", description="Shape parameter (lower = heavier tail), must be > 0", required=True),
            ToolParameter(name="scale", type="number", description="Scale parameter (minimum value), must be > 0", required=True),
            ToolParameter(name="max", type="number", description="Optional maximum clamp", required=False),
        ],
        returns="List of numeric values following Pareto distribution"
    ),
    
    "generate_zipf": GenerationToolDefinition(
        name="generate_zipf",
        description=(
            "Generate discrete values from a Zipfian distribution. "
            "Use for discrete numeric attributes with popularity rankings (e.g., product rankings, page views). "
            "Rank 1 is most common, rank 2 is half as common, etc."
        ),
        kind="distribution",
        parameters=[
            ToolParameter(name="n", type="integer", description="Number of distinct values (ranks 1..n), must be > 0", required=True),
            ToolParameter(name="s", type="number", description="Exponent (typically 1.0-2.0, higher = more skewed), must be > 0", required=True),
        ],
        returns="List of discrete integer values (ranks) following Zipfian distribution"
    ),
    
    "generate_exponential": GenerationToolDefinition(
        name="generate_exponential",
        description=(
            "Generate values from an exponential distribution. "
            "Use for time-based attributes (e.g., inter-arrival times, session durations). "
            "Memoryless property: probability of next event doesn't depend on history."
        ),
        kind="distribution",
        parameters=[
            ToolParameter(name="lambda_", type="number", description="Rate parameter (1/mean), must be > 0", required=True),
            ToolParameter(name="min", type="number", description="Minimum value (default 0)", required=False, default=0.0),
            ToolParameter(name="max", type="number", description="Optional maximum clamp", required=False),
        ],
        returns="List of numeric values following exponential distribution"
    ),
    
    # ========================================================================
    # CATEGORICAL & BOOLEAN DISTRIBUTIONS
    # ========================================================================
    
    "generate_categorical": GenerationToolDefinition(
        name="generate_categorical",
        description=(
            "Generate values from a categorical (discrete) distribution. "
            "Use for attributes with a fixed set of discrete values (e.g., status, category, type). "
            "Each value has an associated probability. Probabilities must sum to 1.0."
        ),
        kind="distribution",
        parameters=[
            ToolParameter(
                name="pmf",
                type="object",
                description="Probability mass function: dictionary mapping value (string) -> probability (float). Must sum to ~1.0.",
                required=True
            ),
        ],
        returns="List of categorical values sampled according to the PMF"
    ),
    
    "generate_bernoulli": GenerationToolDefinition(
        name="generate_bernoulli",
        description=(
            "Generate boolean values from a Bernoulli distribution. "
            "Use for boolean attributes (true/false, yes/no, 1/0). "
            "Generates independent random boolean values with specified probability of true."
        ),
        kind="distribution",
        parameters=[
            ToolParameter(name="p_true", type="number", description="Probability of generating true (0.0 to 1.0, default 0.5)", required=False, default=0.5),
        ],
        returns="List of boolean values (True/False) sampled from Bernoulli distribution"
    ),
    
    # ========================================================================
    # FAKER STRING GENERATORS
    # ========================================================================
    
    "generate_faker_name": GenerationToolDefinition(
        name="generate_faker_name",
        description=(
            "Generate person names using Faker. "
            "Use for person name attributes (e.g., customer_name, employee_name, author_name). "
            "Generates realistic full names, first names, or last names based on locale."
        ),
        kind="string",
        parameters=[
            ToolParameter(name="locale", type="string", description="Locale for name generation (e.g., 'en_US', 'es_ES')", required=False, default="en_US"),
            ToolParameter(name="name_type", type="string", description="Type of name: 'full' (default), 'first', or 'last'", required=False, default="full", enum=["full", "first", "last"]),
        ],
        returns="List of person names (strings)"
    ),
    
    "generate_faker_email": GenerationToolDefinition(
        name="generate_faker_email",
        description=(
            "Generate email addresses using Faker. "
            "Use for email address attributes. Generates realistic email addresses with valid domain names."
        ),
        kind="string",
        parameters=[
            ToolParameter(name="locale", type="string", description="Locale for email generation", required=False, default="en_US"),
            ToolParameter(name="domain", type="string", description="Optional fixed domain (e.g., 'example.com')", required=False),
        ],
        returns="List of email addresses (strings)"
    ),
    
    "generate_faker_address": GenerationToolDefinition(
        name="generate_faker_address",
        description=(
            "Generate addresses using Faker. "
            "Use for address-related attributes (e.g., street_address, city, state, zipcode). "
            "Generates realistic addresses matching the specified locale."
        ),
        kind="string",
        parameters=[
            ToolParameter(name="locale", type="string", description="Locale for address generation", required=False, default="en_US"),
            ToolParameter(name="component", type="string", description="Which component: 'full' (default), 'street', 'city', 'state', 'zipcode', 'country'", required=False, default="full", enum=["full", "street", "city", "state", "zipcode", "country"]),
        ],
        returns="List of address strings"
    ),
    
    "generate_faker_company": GenerationToolDefinition(
        name="generate_faker_company",
        description=(
            "Generate company names using Faker. "
            "Use for company/business name attributes. Generates realistic business names with common suffixes."
        ),
        kind="string",
        parameters=[
            ToolParameter(name="locale", type="string", description="Locale for company name generation", required=False, default="en_US"),
        ],
        returns="List of company names (strings)"
    ),
    
    "generate_faker_text": GenerationToolDefinition(
        name="generate_faker_text",
        description=(
            "Generate random text using Faker. "
            "Use for free-text attributes (e.g., description, comment, notes, bio). "
            "Generates random text content: words, sentences, or paragraphs."
        ),
        kind="string",
        parameters=[
            ToolParameter(name="locale", type="string", description="Locale for text generation", required=False, default="en_US"),
            ToolParameter(name="text_type", type="string", description="Type: 'word', 'sentence', 'paragraph', or 'text' (multiple paragraphs)", required=False, default="sentence", enum=["word", "sentence", "paragraph", "text"]),
            ToolParameter(name="max_nb_chars", type="integer", description="Maximum characters (for paragraph/text)", required=False),
        ],
        returns="List of text strings"
    ),
    
    "generate_faker_url": GenerationToolDefinition(
        name="generate_faker_url",
        description=(
            "Generate URLs using Faker. "
            "Use for URL/URI attributes. Generates realistic URLs with valid domain names and paths."
        ),
        kind="string",
        parameters=[
            ToolParameter(name="locale", type="string", description="Locale for URL generation", required=False, default="en_US"),
            ToolParameter(name="url_type", type="string", description="Type: 'url' (full URL), 'domain' (domain only), 'uri' (path only)", required=False, default="url", enum=["url", "domain", "uri"]),
        ],
        returns="List of URL strings"
    ),
    
    "generate_faker_phone": GenerationToolDefinition(
        name="generate_faker_phone",
        description=(
            "Generate phone numbers using Faker. "
            "Use for phone number attributes. Generates realistic phone numbers matching the locale's format."
        ),
        kind="string",
        parameters=[
            ToolParameter(name="locale", type="string", description="Locale for phone number generation", required=False, default="en_US"),
        ],
        returns="List of phone number strings"
    ),
    
    # ========================================================================
    # MIMESIS STRING GENERATORS
    # ========================================================================
    
    "generate_mimesis_name": GenerationToolDefinition(
        name="generate_mimesis_name",
        description=(
            "Generate person names using Mimesis (international locale support). "
            "Better than Faker for non-English locales. Generates culturally appropriate names."
        ),
        kind="string",
        parameters=[
            ToolParameter(name="locale", type="string", description="Locale code (e.g., 'en', 'ru', 'ja', 'zh')", required=False, default="en"),
            ToolParameter(name="name_type", type="string", description="Type of name: 'full' (default), 'first', or 'last'", required=False, default="full", enum=["full", "first", "last"]),
        ],
        returns="List of person names (strings)"
    ),
    
    "generate_mimesis_email": GenerationToolDefinition(
        name="generate_mimesis_email",
        description=(
            "Generate email addresses using Mimesis (international locale support). "
            "Generates emails matching the locale's naming conventions."
        ),
        kind="string",
        parameters=[
            ToolParameter(name="locale", type="string", description="Locale code", required=False, default="en"),
            ToolParameter(name="domains", type="array", description="Optional list of allowed domains (array of strings)", required=False),
        ],
        returns="List of email addresses (strings)"
    ),
    
    "generate_mimesis_text": GenerationToolDefinition(
        name="generate_mimesis_text",
        description=(
            "Generate text content using Mimesis (international locale support). "
            "Generates text in the specified language/locale."
        ),
        kind="string",
        parameters=[
            ToolParameter(name="locale", type="string", description="Locale code", required=False, default="en"),
            ToolParameter(name="text_type", type="string", description="Type: 'word', 'sentence', or 'title'", required=False, default="sentence", enum=["word", "sentence", "title"]),
        ],
        returns="List of text strings"
    ),
    
    # ========================================================================
    # MIMESIS LOCATION GENERATORS
    # ========================================================================
    
    "generate_mimesis_address": GenerationToolDefinition(
        name="generate_mimesis_address",
        description=(
            "Generate addresses using Mimesis (international locale support). "
            "Generates addresses matching the locale's address format (e.g., postal codes, street formats)."
        ),
        kind="location",
        parameters=[
            ToolParameter(name="locale", type="string", description="Locale code", required=False, default="en"),
            ToolParameter(name="component", type="string", description="Which component: 'full' (default), 'street', 'city', 'state', 'postal_code', 'country'", required=False, default="full", enum=["full", "street", "city", "state", "postal_code", "country"]),
        ],
        returns="List of address strings"
    ),
    
    "generate_mimesis_coordinates": GenerationToolDefinition(
        name="generate_mimesis_coordinates",
        description=(
            "Generate geographic coordinates (latitude/longitude pairs) using Mimesis. "
            "Use for geographic coordinate attributes. Generates realistic coordinate pairs within specified bounds or globally."
        ),
        kind="location",
        parameters=[
            ToolParameter(name="min_lat", type="number", description="Minimum latitude (default: -90)", required=False),
            ToolParameter(name="max_lat", type="number", description="Maximum latitude (default: 90)", required=False),
            ToolParameter(name="min_lon", type="number", description="Minimum longitude (default: -180)", required=False),
            ToolParameter(name="max_lon", type="number", description="Maximum longitude (default: 180)", required=False),
        ],
        returns="List of coordinate pairs (dicts with 'lat' and 'lon')"
    ),
    
    "generate_mimesis_country": GenerationToolDefinition(
        name="generate_mimesis_country",
        description=(
            "Generate country names or codes using Mimesis. "
            "Use for country attributes. Generates country names or ISO country codes."
        ),
        kind="location",
        parameters=[
            ToolParameter(name="locale", type="string", description="Locale code", required=False, default="en"),
            ToolParameter(name="code_type", type="string", description="Type: 'name' (country name), 'code'/'alpha2' (2-letter), 'alpha3' (3-letter)", required=False, default="name", enum=["name", "code", "alpha2", "alpha3"]),
        ],
        returns="List of country names or codes (strings)"
    ),
    
    # ========================================================================
    # REGEX GENERATOR
    # ========================================================================
    
    "generate_regex": GenerationToolDefinition(
        name="generate_regex",
        description=(
            "Generate strings matching a regex pattern using rstr.xeger. "
            "Use for string attributes with specific pattern requirements (e.g., SKU codes, license plates, IDs with format constraints). "
            "Supports bounded quantifiers, character classes, alternation. Does NOT support lookarounds, backreferences, or conditional groups."
        ),
        kind="string",
        parameters=[
            ToolParameter(name="pattern", type="string", description="Regex pattern used for generation", required=True),
            ToolParameter(name="bounds", type="object", description="Explosion control: max repetitions for *, +, .*, .+ (prevents infinite loops). Default: {'star': 16, 'plus': 16, 'dot': 16}", required=False),
            ToolParameter(name="unique", type="boolean", description="If true, deduplicate generated values using hash-based deduplication", required=False, default=True),
            ToolParameter(name="max_attempts_per_value", type="integer", description="If set, fail fast when regex language seems exhausted (generation rate drops)", required=False),
        ],
        returns="List of strings matching the regex pattern"
    ),
}

