"""
Core application infrastructure
Provides configuration and common utilities
"""

from app.core.config import AppConfig, config
from app.core.environment import Environment, get_environment, detect_environment, load_environment_config

__all__ = [
    'AppConfig',
    'config',
    'Environment',
    'get_environment',
    'detect_environment',
    'load_environment_config'
]

