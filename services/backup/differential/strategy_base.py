from abc import ABC, abstractmethod
from services.backup.metadata import BackupMetadataReader

class IDifferentialBackupStrategy(ABC):
    
    @abstractmethod
    def perform_differential_backup(self,metadata_reader : BackupMetadataReader) -> dict | None:
        '''Performs a differential backup based on the provided metadata reader.'''
        pass

        