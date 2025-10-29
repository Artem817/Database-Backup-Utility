    
from abc import ABC, abstractmethod
from pathlib import Path
import csv

class IConnectionProvider(ABC):
    @abstractmethod
    def get_connection(self): pass
    
    @abstractmethod
    def get_connection_params(self): pass

class ILogger(ABC):
    @abstractmethod
    def info(self, message: str): pass
    @abstractmethod
    def error(self, message: str): pass
    @abstractmethod
    def warning(self, message: str): pass

class IMessenger(ABC):
    @abstractmethod
    def success(self, message: str): pass
    @abstractmethod
    def error(self, message: str): pass
    @abstractmethod
    def info(self, message: str): pass
    @abstractmethod
    def warning(self, message: str): pass