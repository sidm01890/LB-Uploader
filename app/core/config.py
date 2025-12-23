"""
Enhanced Configuration Management with Pydantic
Provides type-safe configuration with validation and environment support
Uses properties files (application.properties style) with profile support
"""

import os
from typing import Optional
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from app.core.environment import Environment, detect_environment, load_environment_config, get_environment
from app.core.properties_loader import load_application_properties, get_active_profile

# Load properties files first (this sets environment variables)
# This must happen before any config classes are instantiated
load_environment_config()

# Get current environment (lazy evaluation to avoid circular imports)
def _get_current_env():
    """Get current environment, detecting if needed"""
    return detect_environment()

# This will be evaluated when AppConfig is instantiated
def _is_dev():
    return _get_current_env().is_development

def _is_prod():
    return _get_current_env().is_production


# DatabaseConfig removed - using MongoDB now
# MongoDB connection will be configured separately


class EmailConfig(BaseSettings):
    """Email configuration"""
    
    enabled: bool = Field(False, env=["EMAIL_ENABLED", "email.enabled"])
    smtp_host: str = Field("smtp.gmail.com", env=["SMTP_HOST", "smtp.host"])
    smtp_port: int = Field(587, env=["SMTP_PORT", "smtp.port"])
    smtp_user: str = Field("", env=["SMTP_USER", "smtp.user"])
    smtp_password: str = Field("", env=["SMTP_PASSWORD", "smtp.password"])
    from_email: Optional[str] = Field(None, env=["FROM_EMAIL", "email.from.email"])
    from_name: str = Field("Smart Uploader System", env=["FROM_NAME", "email.from.name"])
    
    @property
    def sender_email(self) -> str:
        """Get sender email, defaulting to smtp_user if not set"""
        return self.from_email or self.smtp_user
    
    model_config = SettingsConfigDict(
        # Properties files are loaded via properties_loader
        # Environment variables take precedence
        case_sensitive=False
    )


class OpenAIConfig(BaseSettings):
    """OpenAI API configuration"""
    
    # Make API key optional in development - will be required when actually using AI features
    api_key: str = Field(
        default="your_openai_api_key_here",
        validation_alias="OPENAI_API_KEY"
    )
    model: str = Field("gpt-4o-mini", validation_alias="OPENAI_MODEL")
    request_timeout: int = Field(30, validation_alias="OPENAI_REQUEST_TIMEOUT")
    
    model_config = SettingsConfigDict(
        # Properties files are loaded via properties_loader
        # Environment variables take precedence
        case_sensitive=False
    )


class MongoDBConfig(BaseSettings):
    """MongoDB configuration for storing uploaded sheets"""
    
<<<<<<< Updated upstream
<<<<<<< Updated upstream
    # Pydantic V2: Use validation_alias to map field names to env var names
    # This allows us to read MONGO_HOST, MONGO_PORT, etc.
    host: str = Field(default="localhost", validation_alias="MONGO_HOST")
    port: int = Field(default=27017, validation_alias="MONGO_PORT")
    database: str = Field(default="devyani_mongo", validation_alias="MONGO_DATABASE")
    username: Optional[str] = Field(default=None, validation_alias="MONGO_USERNAME")
    password: Optional[str] = Field(default=None, validation_alias="MONGO_PASSWORD")
    auth_source: str = Field(default="admin", validation_alias="MONGO_AUTH_SOURCE")
=======
=======
>>>>>>> Stashed changes
    # Prioritize MONGO_HOST environment variable over properties file
    host: str = Field(default="localhost", validation_alias="MONGO_HOST", env="MONGO_HOST")
    port: int = Field(default=27017, validation_alias="MONGO_PORT", env="MONGO_PORT")
    database: str = Field(default="devyani_mongo", validation_alias="MONGO_DATABASE", env="MONGO_DATABASE")
    username: Optional[str] = Field(None, env=["MONGO_USERNAME", "mongo.username"])
    password: Optional[str] = Field(None, env=["MONGO_PASSWORD", "mongo.password"])
    auth_source: str = Field("admin", env=["MONGO_AUTH_SOURCE", "mongo.auth.source"])
>>>>>>> Stashed changes
    
    # Connection pool settings
    max_pool_size: int = Field(default=50, validation_alias="MONGO_MAX_POOL_SIZE")
    min_pool_size: int = Field(default=10, validation_alias="MONGO_MIN_POOL_SIZE")
    max_idle_time_ms: int = Field(default=45000, validation_alias="MONGO_MAX_IDLE_TIME_MS")
    server_selection_timeout_ms: int = Field(default=5000, validation_alias="MONGO_SERVER_SELECTION_TIMEOUT_MS")
    
    model_config = SettingsConfigDict(
        case_sensitive=False,
        # Pydantic V2: Populate from environment variables
        populate_by_name=True,  # Allow both field name and alias
    )
    
    def get_connection_string(self) -> str:
        """Get MongoDB connection string"""
        from urllib.parse import quote_plus
        
        if self.username and self.password:
            username = quote_plus(self.username)
            password = quote_plus(self.password)
            return (
                f"mongodb://{username}:{password}"
                f"@{self.host}:{self.port}/{self.database}?authSource={self.auth_source}"
            )
        else:
            # Local MongoDB without authentication (default for development)
            return f"mongodb://{self.host}:{self.port}/{self.database}"


class AppConfig(BaseSettings):
    """Main application configuration with environment awareness"""
    
    # Environment
    environment: Environment = Field(default_factory=get_environment, env="APP_ENV")
    
    # Core services (lazy instantiation to ensure properties are loaded first)
    # database: DatabaseConfig removed - using MongoDB now
    email: EmailConfig = Field(default_factory=EmailConfig)
    openai: OpenAIConfig = Field(default_factory=OpenAIConfig)
    mongodb: MongoDBConfig = Field(default_factory=MongoDBConfig)
    
    # Application settings (environment-specific defaults)
    log_level: str = Field(
        default_factory=lambda: "DEBUG" if _is_dev() else "INFO",
        env=["LOG_LEVEL", "app.log.level"]
    )
    max_file_size_mb: int = Field(50, env=["MAX_FILE_SIZE_MB", "app.max.file.size.mb"])
    batch_size: int = Field(1000, env=["BATCH_SIZE", "app.batch.size"])
    data_upload_dir: str = Field("./data/uploaded_files", env=["DATA_UPLOAD_DIR", "data.upload.dir"])
    uvicorn_host: str = Field(
        default_factory=lambda: "127.0.0.1" if _is_dev() else "0.0.0.0",
        env=["UVICORN_HOST", "server.host"]
    )
    uvicorn_port: int = Field(8010, env=["UVICORN_PORT", "server.port"])
    uvicorn_reload: bool = Field(
        default_factory=_is_dev,
        env=["UVICORN_RELOAD", "server.reload"]
    )
    cors_allowed_origins: str = Field(
        default_factory=lambda: "*" if _is_dev() else "",
        env=["CORS_ALLOWED_ORIGINS", "cors.allowed.origins"]
    )
    
    # Database connection pooling (environment-specific)
    db_pool_size: int = Field(
        default_factory=lambda: 5 if _is_dev() else 10,
        env=["DB_POOL_SIZE", "db.pool.size"]
    )
    db_max_overflow: int = Field(
        default_factory=lambda: 10 if _is_dev() else 20,
        env=["DB_MAX_OVERFLOW", "db.max.overflow"]
    )
    db_pool_recycle: int = Field(1800, env=["DB_POOL_RECYCLE", "db.pool.recycle"])
    
    # Environment-specific features
    debug: bool = Field(
        default_factory=_is_dev,
        env=["DEBUG", "app.debug"]
    )
    enable_docs: bool = Field(
        default_factory=lambda: not _is_prod(),
        env=["ENABLE_DOCS", "app.enable.docs"]
    )
    enable_reload: bool = Field(
        default_factory=_is_dev,
        env=["ENABLE_RELOAD", "app.enable.reload"]
    )
    
    model_config = SettingsConfigDict(
        # Properties files are loaded via properties_loader, not directly here
        # Environment variables take precedence
        case_sensitive=False
    )
    
    def __init__(self, **kwargs):
        # Set environment in kwargs if not provided
        if 'environment' not in kwargs:
            kwargs['environment'] = _get_current_env()
        elif isinstance(kwargs.get('environment'), str):
            kwargs['environment'] = Environment.from_string(kwargs['environment'])
        
        super().__init__(**kwargs)


# Global configuration instance
# Properties are already loaded via load_environment_config() at module import
config = AppConfig()

# Backward compatibility - expose individual configs at module level
# MySQL configs removed - using MongoDB now

OPENAI_API_KEY = config.openai.api_key
MODEL_NAME = config.openai.model

EMAIL_ENABLED = config.email.enabled
SMTP_HOST = config.email.smtp_host
SMTP_PORT = config.email.smtp_port
SMTP_USER = config.email.smtp_user
SMTP_PASSWORD = config.email.smtp_password
FROM_EMAIL = config.email.sender_email
FROM_NAME = config.email.from_name

# MongoDB config
MONGO_HOST = config.mongodb.host
MONGO_PORT = config.mongodb.port
MONGO_DATABASE = config.mongodb.database
MONGO_USERNAME = config.mongodb.username
MONGO_PASSWORD = config.mongodb.password
