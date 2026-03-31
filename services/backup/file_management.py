
import json
from pathlib import Path


class BackupFileManager:
    def __init__(self, messenger, logger=None):
        self._messenger = messenger
        self._logger = logger
        
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
            if self._logger is not None:
                self._logger.error(f"Failed to write metadata file: {e}")
            return False
            
 
