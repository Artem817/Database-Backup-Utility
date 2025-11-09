import json
from pathlib import Path

from services.backup.metadata import BackupMetadataReader
from services.backup.differential.strategy_base import IDifferentialBackupStrategy
from services.interfaces import IConnectionProvider, ILogger, IMessenger


class DifferentialBackupService:
    """
   Coordinator for differential backups (CLOSED FOR MODIFICATION).
    Delegates work to specific strategies via IDifferentialBackupStrategy.
    
    Open/Closed principle: open for extension (new strategies), 
    closed for modification (we do not change this class when adding new databases).
    """
    
    def __init__(self,
                 connection_provider: IConnectionProvider,
                 logger: ILogger,
                 messenger: IMessenger,
                 strategy: IDifferentialBackupStrategy = None):
        self._connection_provider = connection_provider
        self._logger = logger
        self._messenger = messenger
        self._strategy = strategy
        
    def set_strategy(self, strategy: IDifferentialBackupStrategy) -> None:
        """Sets the strategy for differential backup(Strategy Pattern)"""
        self._strategy = strategy
        
    def write_metadata_file(self, metadata: dict, output_path: Path) -> bool:
        """Writes the backup metadata to a JSON file in the specified output path"""
        try:
            metadata_file = output_path / "metadata.json"
            with open(metadata_file, 'w') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False, default=str)
            self._messenger.info(f"Metadata saved: {metadata_file}")
            return True
        except Exception as e:
            self._messenger.error(f"Failed to write metadata file: {e}")
            self._logger.error(f"Failed to write metadata file: {e}")
            return False
        
    def perform_differential_backup(self, metadata_reader: BackupMetadataReader) -> bool:
        """
       Performs differential backup through strategy delegation.
    The coordinator does not know the implementation details — it only invokes the strategy.
        """
        if not self._strategy:
            self._messenger.error("No differential backup strategy configured!")
            self._logger.error("Differential backup strategy not set")
            return False
        
        try:
            return self._strategy.perform_differential_backup(metadata_reader)
        except Exception as e:
            self._messenger.error(f"Differential backup strategy failed: {e}")
            self._logger.error(f"Strategy execution failed: {e}")
            import traceback
            self._logger.error(traceback.format_exc())
            return False