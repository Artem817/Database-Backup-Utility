from functools import wraps

def requires_replication_privilege(func):
    """Decorator to ensure the user has replication privileges."""
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        query = f"SELECT rolreplication FROM pg_roles WHERE rolname = '{self._user}';"
        
        has_privilege = False
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchone()
                
                if result and result[0] is True:
                    has_privilege = True
                    self._messenger.success("Replication privilege confirmed.")
                    self._logger.info(f"User '{self._user}' has replication privileges.")
                else:
                    self._messenger.error(
                        f"User '{self._user}' does not have REPLICATION privilege."
                        f"\nINSTRUCTION: Run 'ALTER ROLE {self._user} WITH REPLICATION;' as a superuser."
                    )
                    self._logger.error(f"Replication privilege check failed for user '{self._user}'.")

        except Exception as e:
            self._messenger.error(f"Failed to check replication privilege: {e}")
            self._logger.error(f"Failed to check replication privilege: {e}")
            return None

        if has_privilege:
            return func(self, *args, **kwargs)
        else:
            return None
    
    return wrapper

def _check_wal_level(func):
    """Decorator to check if wal_level is set to 'replica' or higher."""
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        query = "SHOW wal_level;"
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchone()
                
                if result and result[0] in ('replica', 'logical', 'archive'):
                    self._messenger.success(f"wal_level is set to '{result[0]}'.")
                    self._logger.info(f"wal_level check passed: '{result[0]}'.")
                    return func(self, *args, **kwargs)
                else:
                    self._messenger.error(
                        f"wal_level is set to '{result[0]}'. It must be 'replica' or higher for replication."
                        "\nINSTRUCTION: Set wal_level to 'replica' or higher in postgresql.conf and restart the server."
                    )
                    self._logger.error(f"wal_level check failed: '{result[0]}'.")
                    return None

        except Exception as e:
            self._messenger.error(f"Failed to check wal_level: {e}")
            self._logger.error(f"Failed to check wal_level: {e}")
            return None
    
    return wrapper  
    