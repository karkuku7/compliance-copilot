"""Retry logic with exponential backoff.

Retries only on QueryTimeoutError — all other exceptions propagate immediately.
Default delays: 1s → 2s → 4s (3 retries, base=1s, multiplier=2x).
"""

from __future__ import annotations

import logging
import time
from functools import wraps
from typing import Any, Callable, TypeVar

from compliance_extractor.errors import QueryTimeoutError

T = TypeVar("T")
logger = logging.getLogger(__name__)


class RetryConfig:
    """Configuration for retry behavior.

    With defaults, delays are: 1s, 2s, 4s.
    """

    def __init__(
        self,
        max_retries: int = 3,
        base_delay_seconds: float = 1.0,
        backoff_multiplier: float = 2.0,
    ):
        self.max_retries = max_retries
        self.base_delay_seconds = base_delay_seconds
        self.backoff_multiplier = backoff_multiplier

    def get_delay(self, attempt: int) -> float:
        """Calculate delay for a given retry attempt (1-indexed)."""
        return self.base_delay_seconds * (self.backoff_multiplier ** (attempt - 1))


def retry_on_timeout(
    max_retries: int = 3,
    base_delay_seconds: float = 1.0,
    backoff_multiplier: float = 2.0,
    on_retry: Callable[[int, float, Exception], None] | None = None,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator that retries a function on QueryTimeoutError.

    Only retries on QueryTimeoutError; other exceptions propagate immediately.

    Args:
        max_retries: Maximum retry attempts.
        base_delay_seconds: Initial delay before first retry.
        backoff_multiplier: Multiplier for exponential backoff.
        on_retry: Optional callback(attempt, delay, exception) called before each retry.
    """
    config = RetryConfig(max_retries, base_delay_seconds, backoff_multiplier)

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            last_exception: Exception | None = None
            for attempt in range(1, config.max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except QueryTimeoutError as exc:
                    last_exception = exc
                    if attempt == config.max_retries:
                        logger.error(
                            "All %d retries exhausted for %s: %s",
                            config.max_retries,
                            func.__name__,
                            exc,
                        )
                        raise
                    delay = config.get_delay(attempt)
                    logger.warning(
                        "Attempt %d/%d for %s timed out. Retrying in %.1fs...",
                        attempt,
                        config.max_retries,
                        func.__name__,
                        delay,
                    )
                    if on_retry:
                        on_retry(attempt, delay, exc)
                    time.sleep(delay)
            raise last_exception  # type: ignore[misc]

        return wrapper

    return decorator


def execute_with_retry(
    func: Callable[..., T],
    args: tuple = (),
    kwargs: dict | None = None,
    config: RetryConfig | None = None,
) -> T:
    """Functional alternative to the retry_on_timeout decorator.

    Args:
        func: Function to call.
        args: Positional arguments.
        kwargs: Keyword arguments.
        config: Retry configuration (uses defaults if None).

    Returns:
        The function's return value.

    Raises:
        QueryTimeoutError: If all retries are exhausted.
    """
    config = config or RetryConfig()
    kwargs = kwargs or {}
    last_exception: Exception | None = None

    for attempt in range(1, config.max_retries + 1):
        try:
            return func(*args, **kwargs)
        except QueryTimeoutError as exc:
            last_exception = exc
            if attempt == config.max_retries:
                raise
            delay = config.get_delay(attempt)
            logger.warning(
                "Attempt %d/%d timed out. Retrying in %.1fs...",
                attempt,
                config.max_retries,
                delay,
            )
            time.sleep(delay)

    raise last_exception  # type: ignore[misc]
