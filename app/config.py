"""
Legacy configuration module - maintained for backward compatibility
New code should use app.core.config instead
"""

# Import from new config system for backward compatibility
from app.core.config import (
    config,
    OPENAI_API_KEY,
    MODEL_NAME,
    EMAIL_ENABLED,
    SMTP_HOST,
    SMTP_PORT,
    SMTP_USER,
    SMTP_PASSWORD,
    FROM_EMAIL,
    FROM_NAME
)

# Re-export for backward compatibility
__all__ = [
    'OPENAI_API_KEY', 'MODEL_NAME',
    'EMAIL_ENABLED', 'SMTP_HOST', 'SMTP_PORT', 'SMTP_USER', 
    'SMTP_PASSWORD', 'FROM_EMAIL', 'FROM_NAME',
    'config'
]

