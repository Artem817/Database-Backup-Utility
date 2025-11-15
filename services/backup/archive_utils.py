"""
Archive utilities for backup compression using zstd.

Zstd provides:
- 3-5x faster compression than gzip
- 2-3x faster decompression than gzip
- Better compression ratio
- Ideal for database backups and fast restore operations
"""

import subprocess
import shutil
from pathlib import Path
from typing import Optional
import sys


def check_zstd_available() -> bool:
    """Check if zstd is available on the system"""
    return shutil.which("zstd") is not None and shutil.which("tar") is not None


def create_single_archive(backup_dir: Path, logger, messenger) -> Optional[Path]:
    """
    Create a single tar.zst archive from backup directory.
    
    Args:
        backup_dir: Path to the backup directory to archive
        logger: Logger instance for logging
        messenger: Messenger instance for user messages
        
    Returns:
        Path to created archive, or None if failed
    """
    if not check_zstd_available():
        messenger.warning("⚠ zstd or tar not found - skipping archive creation")
        messenger.info("Install: brew install zstd (macOS) or apt install zstd (Linux)")
        logger.warning("zstd not available on system")
        return None
    
    try:
        archive_name = f"{backup_dir.name}.tar.zst"
        archive_path = backup_dir.parent / archive_name
        
        messenger.info(f"Compressing backup → {archive_name}")
        messenger.info("Using zstd (fast compression & decompression)")
        
        tar_create = [
            "tar",
            "-cf", "-",
            "-C", str(backup_dir.parent),
            backup_dir.name
        ]
        
        zstd_compress = [
            "zstd",
            "-3",  
            "-T4", 
            "-o", str(archive_path)  # Output file
        ]
        
        messenger.info("⏳ Archiving (level 3, 4 threads)...")
        
        tar_process = subprocess.Popen(
            tar_create,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        zstd_process = subprocess.Popen(
            zstd_compress,
            stdin=tar_process.stdout,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        tar_process.stdout.close()
        
        zstd_stdout, zstd_stderr = zstd_process.communicate()
        tar_process.wait()
        
        if tar_process.returncode != 0:
            tar_stderr = tar_process.stderr.read().decode() if tar_process.stderr else ""
            error_msg = f"tar failed: {tar_stderr}"
            messenger.error(f"Archive creation failed: {error_msg}")
            logger.error(f"tar failed: {tar_stderr}")
            return None
        
        if zstd_process.returncode != 0:
            error_msg = zstd_stderr.decode() if zstd_stderr else "Unknown error"
            messenger.error(f"Compression failed: {error_msg}")
            logger.error(f"zstd failed: {error_msg}")
            return None
        
        if not archive_path.exists():
            messenger.error("Archive file not created")
            logger.error(f"Archive not found: {archive_path}")
            return None
        
        original_size = sum(f.stat().st_size for f in backup_dir.rglob('*') if f.is_file())
        archive_size = archive_path.stat().st_size
        compression_ratio = (1 - archive_size / original_size) * 100 if original_size > 0 else 0
        
        messenger.success(f"✓ Archive created: {archive_name}")
        messenger.info(f"  Original: {original_size / (1024**2):.2f} MB")
        messenger.info(f"  Compressed: {archive_size / (1024**2):.2f} MB")
        messenger.info(f"  Saved: {compression_ratio:.1f}%")
        
        logger.info(f"Archive created: {archive_path}")
        logger.info(f"Compression ratio: {compression_ratio:.1f}%")
        
        messenger.info("✓ Original backup directory preserved for differential backups")
        logger.info(f"Keeping original directory for differential backup access: {backup_dir}")
        
        return archive_path
        
    except Exception as e:
        messenger.error(f"Archive creation failed: {e}")
        logger.error(f"Archive creation exception: {e}")
        return None


def extract_archive(archive_path: Path, output_dir: Path, logger, messenger) -> bool:
    """
    Extract a tar.zst archive for restore operations.
    
    Args:
        archive_path: Path to the .tar.zst archive
        output_dir: Directory to extract to
        logger: Logger instance
        messenger: Messenger instance
        
    Returns:
        True if successful, False otherwise
    """
    if not check_zstd_available():
        messenger.error("zstd or tar not found - cannot extract archive")
        logger.error("zstd not available for extraction")
        return False
    
    if not archive_path.exists():
        messenger.error(f"Archive not found: {archive_path}")
        logger.error(f"Archive file missing: {archive_path}")
        return False
    
    try:
        output_dir.mkdir(parents=True, exist_ok=True)
        
        messenger.info(f"Extracting archive → {output_dir}")
        messenger.info("Using zstd (fast decompression)")
        
        # Use pipe for macOS BSD tar compatibility
        zstd_decompress = [
            "zstd",
            "-d", 
            "-c", 
            "-T4",  
            str(archive_path)
        ]
        
        tar_extract = [
            "tar",
            "-xf", "-", 
            "-C", str(output_dir)
        ]
        
        messenger.info("⏳ Extracting...")
        
        zstd_process = subprocess.Popen(
            zstd_decompress,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        tar_process = subprocess.Popen(
            tar_extract,
            stdin=zstd_process.stdout,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        zstd_process.stdout.close()
        
        tar_stdout, tar_stderr = tar_process.communicate()
        zstd_process.wait()
        
        if zstd_process.returncode != 0:
            zstd_stderr = zstd_process.stderr.read().decode() if zstd_process.stderr else ""
            error_msg = f"zstd decompression failed: {zstd_stderr}"
            messenger.error(f"Extraction failed: {error_msg}")
            logger.error(error_msg)
            return False
        
        if tar_process.returncode != 0:
            error_msg = tar_stderr.decode() if tar_stderr else "Unknown error"
            messenger.error(f"Extraction failed: {error_msg}")
            logger.error(f"tar extraction failed: {error_msg}")
            return False
        
        messenger.success(f"✓ Archive extracted successfully")
        logger.info(f"Archive extracted to: {output_dir}")
        return True
        
    except Exception as e:
        messenger.error(f"Extraction failed: {e}")
        logger.error(f"Extraction exception: {e}")
        return False