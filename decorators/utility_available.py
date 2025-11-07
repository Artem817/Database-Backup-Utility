from functools import wraps
import shutil

def check_utility_available(utility_name):
    """
    Decorator to check if a required utility is available in the system PATH.
    If not available, logs an error and aborts the decorated function.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            if not shutil.which(utility_name):
                self._messenger.error(f"{utility_name} utility not found in PATH. Please install it.")
                self._logger.error(f"{utility_name} utility not found")
                return False
            return func(self, *args, **kwargs)
        return wrapper
    return decorator