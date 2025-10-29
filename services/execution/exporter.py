import csv
from pathlib import Path

from services.interfaces import ILogger, IMessenger
from datetime import datetime

class QueryResultExporter:
    def __init__(self,
                 logger: ILogger,
                 messenger: IMessenger,
                 database_name: str):
        self._logger = logger
        self._messenger = messenger
        self._database_name = database_name

    def export_csv(self, rows, outpath, query: str = None):
        try:
            if not rows or (isinstance(rows, tuple) and not rows[0]):
                self._messenger.warning("No data to export")
                self._logger.warning("No data to export")
                return False

            outpath = Path(outpath) if isinstance(outpath, str) else outpath
            outpath.mkdir(parents=True, exist_ok=True)

            if query:
                query_upper = query.upper().strip()
                if "FROM" in query_upper:
                    table_part = query_upper.split("FROM")[1].split()[0]
                    table_name = table_part.strip('"').strip("'").replace(".", "_")
                    filename = f"query_{table_name}_{self._database_name}.csv"
                else:
                    filename = f"query_result_{self._database_name}.csv"
            else:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"query_{timestamp}_{self._database_name}.csv"

            file_path = outpath / filename
            if isinstance(rows, tuple) and len(rows) == 2:
                data, columns = rows
            else:
                self._messenger.error("Invalid data format for CSV export")
                self._logger.error("Invalid CSV export data format")
                return False

            with file_path.open("w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(columns)
                writer.writerows(data)

            file_size = file_path.stat().st_size
            self._messenger.success(f"Saved: {file_path} ({len(data)} rows, {file_size / 1024:.2f} KB)")
            self._logger.info(f"Query result exported: {file_path} ({len(data)} rows, {file_size} bytes)")
            return str(file_path)
        except Exception as e:
            self._messenger.error(f"Failed to save query result: {e}")
            self._logger.error(f"CSV export failed: {e}")
            return False
