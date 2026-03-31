# utility/resilience.py
import time
import random
from functools import wraps
from typing import Callable, Tuple, Type


class TransientError(Exception):
    """An error that may disappear after a retry (race/IO/archiving not yet complete)."""
    pass


def retry_with_backoff(
    max_retries: int = 5,
    initial_delay: float = 0.5,
    backoff_factor: float = 2.0,
    jitter: float = 0.1,
    retry_exceptions: Tuple[Type[Exception], ...] = (TransientError,),
):
    """
    Exponential backoff + jitter.
    """

    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            delay = initial_delay
            last_exc = None

            logger = None
            if args:
                logger = getattr(args[0], "_logger", None)

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except retry_exceptions as e:
                    last_exc = e
                    if attempt == max_retries:
                        break

                    sleep_time = delay * (1 + random.uniform(-jitter, jitter))
                    if logger:
                        logger.warning(
                            f"Transient error in {func.__name__}: {e}. "
                            f"Retrying in {sleep_time:.2f}s "
                            f"(attempt {attempt + 1}/{max_retries})"
                        )

                    time.sleep(sleep_time)
                    delay *= backoff_factor

            if logger:
                logger.error(
                    f"Operation {func.__name__} failed after {max_retries} retries."
                )
            raise last_exc

        return wrapper

    return decorator
