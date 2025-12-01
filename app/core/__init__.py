"""
Core application infrastructure
Provides database management, configuration, and common utilities
"""

from app.core.database import DatabaseManager, db_manager
from app.core.config import AppConfig, config
from app.core.environment import Environment, get_environment, detect_environment, load_environment_config

__all__ = [
    'DatabaseManager',
    'db_manager',
    'AppConfig',
    'config',
    'Environment',
    'get_environment',
    'detect_environment',
    'load_environment_config'
]

