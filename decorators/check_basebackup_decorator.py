from functools import wraps
import shutil


def check_basebackup(func):
    """Decorator to check if a base backup exists before performing certain operations."""
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        try:
           if not shutil.which("pg_basebackup"):
               self._mesanger.error("pg_basebackup utility is not installed or not found in PATH.")
               self._logger.error("pg_basebackup utility is missing.")
           else:
                self._logger.info("pg_basebackup utility check passed.")
        except Exception as e:
            raise RuntimeError(f"Failed to check base backup: {e}")
        return func(self, *args, **kwargs)
    return wrapper