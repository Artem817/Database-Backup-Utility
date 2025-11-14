import json
from pathlib import Path
from console_utils import get_messenger

class PostgresWalArchiveConfig:
    """Manages PostgreSQL WAL archive directory configuration"""
    
    CONFIG_DIR = Path.home() / ".backup_utility"
    CONFIG_FILE = CONFIG_DIR / "config.json"
    
    def __init__(self):
        self._messenger = get_messenger()
        self._ensure_config_dir()
    
    def _ensure_config_dir(self):
        if not self.CONFIG_DIR.exists():
            self.CONFIG_DIR.mkdir(parents=True, exist_ok=True)
            self._messenger.info(f"Created config directory: {self.CONFIG_DIR}")
    
    def _load_config(self) -> dict:
        if not self.CONFIG_FILE.exists():
            return {}
        try:
            with open(self.CONFIG_FILE, 'r') as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            self._messenger.error(f"Failed to parse config file: {e}")
            return {}
        except Exception as e:
            self._messenger.error(f"Failed to load config: {e}")
            return {}
    
    def _save_config(self, config: dict) -> bool:
        try:
            with open(self.CONFIG_FILE, 'w') as f:
                json.dump(config, f, indent=2, ensure_ascii=False)
            return True
        except Exception as e:
            self._messenger.error(f"Failed to save config: {e}")
            return False
    
    def _validate_directory(self, path_str: str) -> tuple[bool, str]:
        if not path_str or not path_str.strip():
            return False, "Path cannot be empty"

        path = Path(path_str.strip())
        
        if not path.exists():
            return False, f"Directory does not exist: {path}"
        
        if not path.is_dir():
            return False, f"Path is not a directory: {path}"
        
        try:
            test_file = path / ".permission_test"
            test_file.touch()
            test_file.unlink()
        except Exception as e:
            return False, f"Directory is not writable: {e}"
        
        return True, ""
    
    def get_archive_directory(self) -> str | None:
        config = self._load_config()
        return config.get("postgresql", {}).get("wal_archive_directory")
    
    def configure_archive_directory(self, force_reconfigure: bool = False) -> str | None:
        config = self._load_config()
        
        # ---- FIRST TIME ----
        if "postgresql" not in config or not config["postgresql"].get("wal_archive_directory"):
            self._messenger.section_header("PostgreSQL WAL Archiving Configuration")
            self._messenger.info("Hello! Let's configure PostgreSQL WAL Archiving.")
            
            while True:
                archive_path = input("\nAdd your archive_directory (where Postgres copies WAL files):\n> ").strip()
                
                is_valid, error_msg = self._validate_directory(archive_path)
                
                if is_valid:
                    archive_path = str(Path(archive_path).resolve())
                    
                    if "postgresql" not in config:
                        config["postgresql"] = {}
                    
                    config["postgresql"]["wal_archive_directory"] = archive_path
                    
                    if self._save_config(config):
                        self._messenger.success("Config saved successfully!")
                        return archive_path
                    else:
                        self._messenger.error("Failed to save config")
                        return None
                else:
                    self._messenger.error(f"Invalid path: {error_msg}")
                    retry = input("Try again? (1 - Yes, 0 - No): ").strip()
                    if retry != "1":
                        return None
        
        else:
            existing_path = config["postgresql"]["wal_archive_directory"]
            
            if not force_reconfigure:
                self._messenger.section_header("PostgreSQL WAL Archive Configuration")
                self._messenger.info(f"Current archive_directory:\n  {existing_path}\n")
                
                confirmation = input("Confirm the validity of this path:\n  1 - Yes\n  0 - No\n> ").strip()
                
                if confirmation == "1":
                    is_valid, error_msg = self._validate_directory(existing_path)
                    if is_valid:
                        self._messenger.success("Path confirmed!")
                        return existing_path
                    else:
                        self._messenger.warning(f"Path validation failed: {error_msg}")
                        self._messenger.info("Please provide a new path.")
            
            while True:
                archive_path = input("\nEnter a new archive_directory:\n> ").strip()
                
                is_valid, error_msg = self._validate_directory(archive_path)
                
                if is_valid:
                    archive_path = str(Path(archive_path).resolve())
                    config["postgresql"]["wal_archive_directory"] = archive_path
                    
                    if self._save_config(config):
                        self._messenger.success("Path successfully updated!")
                        return archive_path
                    else:
                        self._messenger.error("Failed to save config")
                        return None
                else:
                    self._messenger.error(f"Invalid path: {error_msg}")
                    retry = input("Try again? (1 - Yes, 0 - No): ").strip()
                    if retry != "1":
                        return None
