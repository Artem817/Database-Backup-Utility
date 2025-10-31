from functools import wraps
from inspect import signature
from typing import Any, Callable, TypeVar, cast

F = TypeVar('F', bound=Callable[..., Any])

def not_none(*not_none_args: str) -> Callable[[F], F]:
    """Decorator to ensure specified arguments are not None."""
    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            sig = signature(func)
            bound_args = sig.bind(*args, **kwargs)
            bound_args.apply_defaults()

            for arg_name in not_none_args:
                if bound_args.arguments.get(arg_name) is None:
                    raise ValueError(f"Argument '{arg_name}' cannot be None")
            
            return func(*args, **kwargs)
        return cast(F, wrapper)
    return decorator