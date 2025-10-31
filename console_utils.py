import logging
from enum import Enum
from typing import Optional
from colorama import Fore, Style

class MessageLevel(Enum):
    INFO = "info"
    SUCCESS = "success"
    WARNING = "warning"
    ERROR = "error"
    DEBUG = "debug"
    CRITICAL = "critical"

class ConsoleMessenger:
    """Centralized messaging system that handles both colored console output and logging"""
    
    def __init__(self, logger: Optional[logging.Logger] = None, enable_colors: bool = True):
        self.logger = logger
        self.enable_colors = enable_colors
        self._color_map = {
            MessageLevel.INFO: Fore.CYAN,
            MessageLevel.SUCCESS: Fore.GREEN,
            MessageLevel.WARNING: Fore.YELLOW,
            MessageLevel.ERROR: Fore.RED,
            MessageLevel.DEBUG: Fore.MAGENTA,
            MessageLevel.CRITICAL: Fore.RED + Style.BRIGHT,
        }
        self._log_level_map = {
            MessageLevel.INFO: logging.INFO,
            MessageLevel.SUCCESS: logging.INFO,
            MessageLevel.WARNING: logging.WARNING,
            MessageLevel.ERROR: logging.ERROR,
            MessageLevel.DEBUG: logging.DEBUG,
            MessageLevel.CRITICAL: logging.CRITICAL,
        }

    def _get_colored_message(self, message: str, level: MessageLevel) -> str:
        """Apply color formatting to message if colors are enabled"""
        if not self.enable_colors:
            return message
        
        color = self._color_map.get(level, "")
        return f"{color}{message}{Style.RESET_ALL}"

    def _log_to_file(self, message: str, level: MessageLevel) -> None:
        """Log message to file if logger is available"""
        if not self.logger:
            return
            
        log_level = self._log_level_map.get(level, logging.INFO)
        self.logger.log(log_level, message)

    def print_colored(self, message: str, level: MessageLevel = MessageLevel.INFO) -> None:
        """Print colored message to console and optionally log to file"""
        colored_message = self._get_colored_message(message, level)
        print(colored_message)
        
        self._log_to_file(message, level)

    def info(self, message: str) -> None:
        """Print info message"""
        self.print_colored(message, MessageLevel.INFO)

    def success(self, message: str) -> None:
        """Print success message"""
        self.print_colored(f"✓ {message}", MessageLevel.SUCCESS)

    def warning(self, message: str) -> None:
        """Print warning message"""
        self.print_colored(f"[WARNING] {message}", MessageLevel.WARNING)

    def error(self, message: str) -> None:
        """Print error message"""
        self.print_colored(f"✗ {message}", MessageLevel.ERROR)

    def critical(self, message: str) -> None:
        """Print critical error message"""
        self.print_colored(f"[CRITICAL ERROR] {message}", MessageLevel.CRITICAL)

    def debug(self, message: str) -> None:
        """Print debug message"""
        self.print_colored(f"[DEBUG] {message}", MessageLevel.DEBUG)

    def section_header(self, title: str) -> None:
        """Print a formatted section header"""
        separator = "=" * len(title)
        self.print_colored(f"\n{separator}", MessageLevel.INFO)
        self.print_colored(title, MessageLevel.INFO)
        self.print_colored(f"{separator}", MessageLevel.INFO)

    def config_item(self, key: str, value: str, mask_value: bool = False) -> None:
        """Print a configuration item with consistent formatting"""
        display_value = "***" if mask_value and value else value if value else "(not set)"
        colored_value = self._get_colored_message(display_value, MessageLevel.SUCCESS)
        self.print_colored(f"  {key}: {colored_value}", MessageLevel.INFO)


_global_messenger: Optional[ConsoleMessenger] = None

def get_messenger() -> ConsoleMessenger:
    """Get the global messenger instance"""
    global _global_messenger
    if _global_messenger is None:
        _global_messenger = ConsoleMessenger()
    return _global_messenger

def configure_messenger(logger: Optional[logging.Logger] = None, enable_colors: bool = True) -> None:
    """Configure the global messenger with a logger"""
    global _global_messenger
    _global_messenger = ConsoleMessenger(logger=logger, enable_colors=enable_colors)

def print_colored(message: str, level: str = "info") -> None:
    """Legacy function for backward compatibility"""
    level_mapping = {
        "info": MessageLevel.INFO,
        "success": MessageLevel.SUCCESS,
        "warning": MessageLevel.WARNING,
        "error": MessageLevel.ERROR,
        "debug": MessageLevel.DEBUG,
        "critical": MessageLevel.CRITICAL,
    }
    msg_level = level_mapping.get(level.lower(), MessageLevel.INFO)
    get_messenger().print_colored(message, msg_level)
