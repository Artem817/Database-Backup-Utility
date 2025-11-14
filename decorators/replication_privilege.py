from functools import wraps

def requires_replication_privilege(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs) -> bool:
        query = "SELECT rolreplication FROM pg_roles WHERE rolname = %s;"
        
        has_privilege = False
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, (self._user,))
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
            return False

        if not has_privilege:
            return False
        
        return func(self, *args, **kwargs)
    
    return wrapper

def _check_wal_level(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs) -> bool:
        query = "SHOW wal_level;"
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchone()
                level = result[0] if result else None
                
                if level in ('replica', 'logical', 'archive'):
                    self._messenger.success(f"wal_level is set to '{level}'.")
                    self._logger.info(f"wal_level check passed: '{level}'.")
                    return func(self, *args, **kwargs)
                else:
                    self._messenger.error(
                        f"wal_level is set to '{level}'. It must be 'replica' or higher for replication."
                        "\nINSTRUCTION: Set wal_level to 'replica' or higher in postgresql.conf and restart the server."
                    )
                    self._logger.error(f"wal_level check failed: '{level}'.")
                    return False

        except Exception as e:
            self._messenger.error(f"Failed to check wal_level: {e}")
            self._logger.error(f"Failed to check wal_level: {e}")
            return False
    
    return wrapper



def _check_archive_mode(func):
    """
    Decorator to check if archive_mode is set to 'on' or 'always'.
    This is a critical prerequisite for any PITR-capable backup strategy.
    """
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        query = "SHOW archive_mode;"
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchone()
                
                if result and result[0] in ('on', 'always'):
                    self._messenger.success(f"✓ Prerequisite check: archive_mode is '{result[0]}'.")
                    self._logger.info(f"archive_mode check passed: '{result[0]}'.")
                    return func(self, *args, **kwargs)
                else:
                    self._messenger.error(
                    f"archive_mode is set to '{result[0]}'. It must be 'on' or 'always' for PITR."
                    "\n\nINSTRUCTION: To enable PITR, please configure **TWO** parameters in postgresql.conf:"
                    "\n1. wal_level = replica (if not already set)"
                    "\n2. archive_mode = on (or 'always' for PG13+)"
                    "\n3. archive_command = 'cp %p /path/to/wal_archive/%f' (Choose your secure path!)"
                    "\n\nAfter making changes, you MUST restart PostgreSQL."
                    "\nBackup cannot proceed without this setting."

                    )
                    self._logger.error(f"archive_mode check failed: '{result[0]}'. Backup aborted.")
                    return False 

        except Exception as e:
            self._messenger.error(f"Failed to check archive_mode: {e}")
            self._logger.error(f"Failed to check archive_mode: {e}")
            return False 
        
    return wrapper