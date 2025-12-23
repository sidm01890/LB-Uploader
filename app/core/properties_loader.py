"""
Properties File Loader
Handles loading of application.properties and profile-specific properties files
Similar to Spring Boot's profile system
"""

import os
import re
from pathlib import Path
from typing import Dict, Optional
from dotenv import load_dotenv


class PropertiesLoader:
    """Loads properties from application.properties files with profile support"""
    
    def __init__(self, base_dir: Optional[Path] = None):
        """
        Initialize properties loader
        
        Args:
            base_dir: Base directory for properties files (default: project root)
        """
        if base_dir is None:
            base_dir = Path(__file__).parent.parent.parent
        self.base_dir = base_dir
        self.properties: Dict[str, str] = {}
    
    def load_properties_file(self, file_path: Path) -> Dict[str, str]:
        """
        Load properties from a .properties file
        
        Args:
            file_path: Path to properties file
            
        Returns:
            Dictionary of key-value pairs
        """
        props = {}
        if not file_path.exists():
            return props
        
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                # Skip empty lines and comments
                if not line or line.startswith('#'):
                    continue
                
                # Handle key=value pairs
                if '=' in line:
                    key, value = line.split('=', 1)
                    key = key.strip()
                    value = value.strip()
                    
                    # Remove quotes if present
                    if value.startswith('"') and value.endswith('"'):
                        value = value[1:-1]
                    elif value.startswith("'") and value.endswith("'"):
                        value = value[1:-1]
                    
                    props[key] = value
        
        return props
    
    def load_all_properties(self, active_profile: Optional[str] = None) -> Dict[str, str]:
        """
        Load all properties files in order:
        1. application.properties (base)
        2. application-{profile}.properties (profile-specific, overrides base)
        
        Args:
            active_profile: Active profile (dev, stage, prod). If None, detects from env
            
        Returns:
            Combined properties dictionary
        """
        if active_profile is None:
            active_profile = os.getenv('SPRING_PROFILES_ACTIVE') or os.getenv('APP_PROFILE') or os.getenv('APP_ENV', 'dev')
        
        # Load base application.properties
        base_file = self.base_dir / 'application.properties'
        if base_file.exists():
            self.properties.update(self.load_properties_file(base_file))
            print(f"✅ Loaded base config: {base_file.name}")
        else:
            print(f"⚠️  Base config not found: {base_file.name}")
        
        # Load profile-specific properties (overrides base)
        profile_file = self.base_dir / f'application-{active_profile}.properties'
        if profile_file.exists():
            profile_props = self.load_properties_file(profile_file)
            self.properties.update(profile_props)  # Profile overrides base
            print(f"✅ Loaded profile config: {profile_file.name} (profile: {active_profile})")
        else:
            print(f"⚠️  Profile config not found: {profile_file.name}")
        
        # Set properties as environment variables for compatibility with existing code
        # Map common properties to environment variable names
        property_to_env_map = {
            'mysql.host': 'MYSQL_HOST',
            'mysql.port': 'MYSQL_PORT',
            'mysql.user': 'MYSQL_USER',
            'mysql.password': 'MYSQL_PASSWORD',
            'mysql.database': 'MYSQL_DB',
            'mongo.host': 'MONGO_HOST',
            'mongo.port': 'MONGO_PORT',
            'mongo.database': 'MONGO_DATABASE',
            'mongo.username': 'MONGO_USERNAME',
            'mongo.password': 'MONGO_PASSWORD',
            'mongo.auth.source': 'MONGO_AUTH_SOURCE',
            'openai.api.key': 'OPENAI_API_KEY',
            'openai.model': 'OPENAI_MODEL',
            'openai.request.timeout': 'OPENAI_REQUEST_TIMEOUT',
            'smtp.host': 'SMTP_HOST',
            'smtp.port': 'SMTP_PORT',
            'smtp.user': 'SMTP_USER',
            'smtp.password': 'SMTP_PASSWORD',
            'email.from.email': 'FROM_EMAIL',
            'email.from.name': 'FROM_NAME',
            'email.enabled': 'EMAIL_ENABLED',
            'app.log.level': 'LOG_LEVEL',
            'app.debug': 'DEBUG',
            'app.enable.docs': 'ENABLE_DOCS',
            'app.enable.reload': 'ENABLE_RELOAD',
            'server.host': 'UVICORN_HOST',
            'server.port': 'UVICORN_PORT',
            'server.reload': 'UVICORN_RELOAD',
            'cors.allowed.origins': 'CORS_ALLOWED_ORIGINS',
            'data.upload.dir': 'DATA_UPLOAD_DIR',
        }
        
        for key, value in self.properties.items():
            # Set the original key as env var (for backward compatibility)
            if key not in os.environ:  # Don't override existing env vars
                os.environ[key] = value
            
            # Also set mapped environment variable names (for Pydantic Settings)
            if key in property_to_env_map:
                env_key = property_to_env_map[key]
<<<<<<< Updated upstream
                # Only set env var if not already set (don't override docker-compose env vars)
                # This allows docker-compose env vars to take precedence over properties
                if env_key not in os.environ:
                    os.environ[env_key] = value
=======
                # Always update env vars from properties (stage properties override base)
                # This ensures stage-specific configs take precedence
                os.environ[env_key] = value
>>>>>>> Stashed changes
        
        return self.properties
    
    def get(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """Get a property value"""
        return self.properties.get(key, default)
    
    def get_int(self, key: str, default: int = 0) -> int:
        """Get a property value as integer"""
        try:
            return int(self.properties.get(key, default))
        except (ValueError, TypeError):
            return default
    
    def get_bool(self, key: str, default: bool = False) -> bool:
        """Get a property value as boolean"""
        value = self.properties.get(key, str(default)).lower()
        return value in ('true', '1', 'yes', 'on')


def load_application_properties(profile: Optional[str] = None) -> PropertiesLoader:
    """
    Load application properties with profile support
    
    Args:
        profile: Active profile (dev, stage, prod). If None, detects from environment
        
    Returns:
        PropertiesLoader instance with loaded properties
    """
    loader = PropertiesLoader()
    loader.load_all_properties(active_profile=profile)
    return loader


def get_active_profile() -> str:
    """
    Get the active profile from environment variables
    
    Priority:
    1. SPRING_PROFILES_ACTIVE (Spring Boot convention)
    2. APP_PROFILE
    3. APP_ENV
    4. Default: dev
    
    Returns:
        Active profile name (dev, stage, or prod)
    """
    profile = (
        os.getenv('SPRING_PROFILES_ACTIVE') or 
        os.getenv('APP_PROFILE') or 
        os.getenv('APP_ENV', 'dev')
    )
    return profile.lower().strip()

