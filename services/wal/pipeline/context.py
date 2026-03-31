# services/wal/pipeline/context.py
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, List, Dict, Any


@dataclass
class WalFileContext:
    """
    Context that goes through the pipeline.

    Before AtomicWriteStage, there should be:
    - current_path: source WAL (in the archive)
    - dest_dir: where we copy (incremental backup dir)
    - wal_name: segment name
    - segment_size

    After AtomicWriteStage:
    - current_path: already in backup dir
    - final_path:   final path

    After IntegrityStage:
        - checksum
      - size_bytes
    """
    current_path: Path
    dest_dir: Path
    wal_name: str
    segment_size: int = 16 * 1024 * 1024
    metadata_items: Optional[List[Dict[str, Any]]] = None

    final_path: Optional[Path] = None
    checksum: Optional[str] = None
    size_bytes: Optional[int] = None
