"""Token bucket rate limiter for LLM API calls.

Provides request-based and token-based rate limiting with per-step-type
concurrency control.
"""

from asyncio import Semaphore, Lock
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple
import asyncio

from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)


class RateLimiter:
    """
    Token bucket rate limiter for LLM API calls with per-step-type concurrency limits.
    
    CRITICAL: Uses asynccontextmanager to hold semaphores for the full duration
    of the API call, not just during acquisition. This prevents concurrency bursts
    that would trigger throttling.
    
    Also includes token-based rate limiting (tokens/minute) in addition to request-based.
    """
    
    def __init__(
        self,
        requests_per_minute: int = 500,
        tokens_per_minute: int = 1_000_000,
        max_concurrent: int = 10,
        max_concurrency_per_step_type: Optional[Dict[str, int]] = None
    ):
        """
        Initialize rate limiter.
        
        Args:
            requests_per_minute: Maximum requests per minute
            tokens_per_minute: Maximum tokens per minute
            max_concurrent: Global maximum concurrent requests
            max_concurrency_per_step_type: Per-step-type limits (e.g., {"per-entity": 5})
        """
        # Concurrency control: semaphores held for full API call duration
        self.semaphore = Semaphore(max_concurrent)
        self.step_type_semaphores: Dict[str, Semaphore] = {}
        if max_concurrency_per_step_type:
            for step_type, limit in max_concurrency_per_step_type.items():
                self.step_type_semaphores[step_type] = Semaphore(limit)
        
        # Request rate limiting (requests/minute)
        self.request_times: List[datetime] = []
        self._request_lock = Lock()
        self.rpm = requests_per_minute
        
        # Token rate limiting (tokens/minute)
        self.token_times: List[Tuple[datetime, int]] = []
        self._token_lock = Lock()
        self.tpm = tokens_per_minute
    
    @asynccontextmanager
    async def acquire(
        self,
        step_type: Optional[str] = None,
        estimated_tokens: int = 0
    ):
        """
        Acquire permission to make an API call. Returns a context manager that holds
        the permit for the full duration of the API call.
        
        Args:
            step_type: Optional step type (e.g., "per-entity", "per-relation") for per-type limits
            estimated_tokens: Estimated tokens for this call (for token-based rate limiting)
        
        Usage:
            async with rate_limiter.acquire(step_type="per-entity", estimated_tokens=2000):
                result = await llm_call(...)
        """
        # Acquire per-step-type semaphore if applicable
        step_semaphore = self.step_type_semaphores.get(step_type) if step_type else None
        
        # Acquire rate limit permits (requests/minute and tokens/minute)
        await self._acquire_request_permit()
        await self._acquire_token_permit(estimated_tokens)
        
        # Acquire concurrency semaphores (held for full duration via context manager)
        async with self.semaphore:  # Global concurrency limit
            if step_semaphore:
                async with step_semaphore:  # Per-step-type limit
                    yield  # Permit held here - API call happens inside this block
            else:
                yield  # Permit held here - API call happens inside this block
    
    async def _acquire_request_permit(self):
        """Acquire request rate limit permit (requests/minute)."""
        async with self._request_lock:
            now = datetime.now()
            # Clean old entries (older than 1 minute)
            cutoff = now - timedelta(minutes=1)
            self.request_times = [t for t in self.request_times if t > cutoff]
            
            # Check rate limit
            if len(self.request_times) >= self.rpm:
                # Wait until oldest request is 1 minute old
                wait_time = (self.request_times[0] + timedelta(minutes=1) - now).total_seconds()
                if wait_time > 0:
                    await asyncio.sleep(wait_time)
                    # Re-clean after sleep
                    now = datetime.now()
                    cutoff = now - timedelta(minutes=1)
                    self.request_times = [t for t in self.request_times if t > cutoff]
            
            # Record this request
            self.request_times.append(datetime.now())
    
    async def _acquire_token_permit(self, estimated_tokens: int):
        """Acquire token rate limit permit (tokens/minute)."""
        if estimated_tokens <= 0:
            return  # Skip token limiting if not estimated
        
        async with self._token_lock:
            now = datetime.now()
            # Clean old entries (older than 1 minute)
            cutoff = now - timedelta(minutes=1)
            self.token_times = [(t, tokens) for t, tokens in self.token_times if t > cutoff]
            
            # Calculate current token usage in last minute
            current_tokens = sum(tokens for _, tokens in self.token_times)
            
            # Check if we would exceed limit
            if current_tokens + estimated_tokens > self.tpm:
                # Wait until we can fit this request
                tokens_to_wait_for = current_tokens + estimated_tokens - self.tpm
                cumulative = 0
                wait_until = None
                for timestamp, tokens in self.token_times:
                    cumulative += tokens
                    if cumulative >= tokens_to_wait_for:
                        wait_until = timestamp + timedelta(minutes=1)
                        break
                
                if wait_until:
                    wait_time = (wait_until - now).total_seconds()
                    if wait_time > 0:
                        await asyncio.sleep(wait_time)
                        # Re-clean after sleep
                        now = datetime.now()
                        cutoff = now - timedelta(minutes=1)
                        self.token_times = [(t, tokens) for t, tokens in self.token_times if t > cutoff]
            
            # Record this token usage (will be cleaned up after 1 minute)
            self.token_times.append((datetime.now(), estimated_tokens))


async def run_with_rate_limit(
    func,
    rate_limiter: RateLimiter,
    step_type: Optional[str] = None,
    estimated_tokens: int = 0,
    *args,
    **kwargs
):
    """
    Run function with rate limiting. The permit is held for the full duration
    of the function call, ensuring true concurrency control.
    
    Args:
        func: Async function to execute
        rate_limiter: RateLimiter instance
        step_type: Optional step type for per-type limits
        estimated_tokens: Estimated tokens for this call
        *args, **kwargs: Arguments to pass to func
        
    Returns:
        Result of func(*args, **kwargs)
    """
    async with rate_limiter.acquire(step_type=step_type, estimated_tokens=estimated_tokens):
        return await func(*args, **kwargs)

