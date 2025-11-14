from functools import wraps
import shutil


def check_basebackup(func):
    """Decorator to check if pg_basebackup binary is available before running backup."""
    @wraps(func)
    def wrapper(self, *args, **kwargs) -> bool:
        try:
            if not shutil.which("pg_basebackup"):
                self._messenger.error("pg_basebackup utility is not installed or not found in PATH.")
                self._logger.error("pg_basebackup utility is missing.")
                return False
            else:
                self._logger.info("pg_basebackup utility check passed.")
        except Exception as e:
            self._messenger.error(f"Failed to check pg_basebackup: {e}")
            self._logger.error(f"Failed to check pg_basebackup: {e}")
            return False

        return func(self, *args, **kwargs)
    return wrapper
