"""
Environment Management
Handles environment detection and configuration loading
Now uses properties files (application.properties style)
"""

import os
from enum import Enum
from pathlib import Path
from typing import Optional
from app.core.properties_loader import load_application_properties, get_active_profile


class Environment(str, Enum):
    """Application environments"""
    DEVELOPMENT = "dev"
    STAGING = "stage"
    PRODUCTION = "prod"
    
    @classmethod
    def from_string(cls, value: str) -> "Environment":
        """Convert string to Environment enum"""
        value_lower = value.lower().strip()
        for env in cls:
            if env.value == value_lower:
                return env
        raise ValueError(f"Invalid environment: {value}. Must be one of: {', '.join([e.value for e in cls])}")
    
    @property
    def is_production(self) -> bool:
        """Check if this is production environment"""
        return self == Environment.PRODUCTION
    
    @property
    def is_development(self) -> bool:
        """Check if this is development environment"""
        return self == Environment.DEVELOPMENT
    
    @property
    def is_staging(self) -> bool:
        """Check if this is staging environment"""
        return self == Environment.STAGING


def detect_environment() -> Environment:
    """
    Detect the current environment from profile or environment variable
    
    Priority:
    1. SPRING_PROFILES_ACTIVE
    2. APP_PROFILE
    3. APP_ENV
    4. Default to DEVELOPMENT
    """
    profile = get_active_profile()
    try:
        return Environment.from_string(profile)
    except ValueError:
        # Default to dev if invalid
        return Environment.DEVELOPMENT


def load_environment_config(env: Optional[Environment] = None) -> None:
    """
    Load environment-specific configuration from properties files
    
    Loads in order:
    1. application.properties (base config, always loaded)
    2. application-{profile}.properties (profile-specific overrides)
    
    Args:
        env: Environment to load. If None, detects from profile/environment variable
    """
    if env is None:
        env = detect_environment()
    
    # Load properties files
    load_application_properties(profile=env.value)


def get_environment() -> Environment:
    """Get current environment"""
    return detect_environment()


# Global environment instance
current_environment = detect_environment()
