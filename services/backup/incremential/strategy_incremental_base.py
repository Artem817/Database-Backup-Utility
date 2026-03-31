from abc import ABC, abstractmethod
from services.backup.metadata import BackupMetadataReader

class IIncrementalBackupStrategy(ABC):
    
    @abstractmethod
    def perform_incremental_backup(self,metadata_reader : BackupMetadataReader) -> dict | None:
        '''Performs an incremental backup based on the provided metadata reader.'''
        pass

        