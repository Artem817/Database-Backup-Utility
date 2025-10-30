import sys

import sqlparse

from services.interfaces import IConnectionProvider, ILogger, IMessenger


def analyze_sql(query: str) -> tuple[bool, str]:
    """Analyze SQL for destructive operations."""
    if not query or not query.strip():
        return True, "Empty query."
    dangerous = {"DROP", "DELETE", "TRUNCATE", "ALTER"}
    try:
        parsed = sqlparse.parse(query)
        if not parsed:
            return True, "Empty query."
        tokens = [t.value.upper() for t in parsed[0].tokens if not t.is_whitespace]
        found = [w for w in dangerous if w in tokens]
        if found:
            return False, f"The query contains dangerous keywords: {', '.join(found)}"
        return True, "Looks safe."
    except Exception as e:
        return False, f"SQL analysis failed: {e}"


class QueryExecutor:
    def __init__(self,
                 connection_provider: IConnectionProvider,
                 logger: ILogger,
                 messenger: IMessenger):
        self._connection_provider = connection_provider
        self._logger = logger
        self._messenger = messenger

    def execute_query(self, query: str):
        is_safe, message = analyze_sql(query)
        if not is_safe:
            self._messenger.warning(message)
            self._logger.warning(f"Dangerous query detected: {message}")
            if sys.stdin.isatty():
                confirmation = input("Continue? (Y/n): ")
                if confirmation.upper() != "Y":
                    self._logger.info("Query execution cancelled by user")
                    return None
            else:
                self._logger.warning("Non-interactive mode: dangerous query skipped.")
                return None

        try:
            connection = self._connection_provider.get_connection()
            with connection.cursor() as cur:
                self._logger.info(f"Executing query: {query[:100]}...")
                cur.execute(query)
                if cur.description:
                    rows = cur.fetchall()
                    columns = [d[0] for d in cur.description]
                    self._logger.info(f"Query returned {len(rows)} rows")
                    return (rows, columns)
                else:
                    connection.commit()
                    affected = cur.rowcount
                    self._messenger.success(f"Query executed. {affected} rows affected.")
                    self._logger.info(f"Query executed, {affected} rows affected")
                    return ([], [])
        except Exception as e:
            self._messenger.error(f"Query failed: {e}")
            self._logger.error(f"Query failed: {e}")
            connection.rollback()
            return None

    def extract_sql_query(self, query: str, outpath, query_result_exporter):
        self._logger.info(f"Starting query extraction to: {outpath}")
        execute_result = self.execute_query(query)
        if execute_result is None:
            self._logger.warning("Query extraction cancelled or failed")
            return False
        result = query_result_exporter.export_csv(execute_result, outpath, query)
        if result:
            self._logger.info(f"Query extraction completed: {result}")
        else:
            self._logger.error("Query extraction failed")
        return result

