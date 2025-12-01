"""
Custom Exception Hierarchy
Provides consistent error handling across the application
"""

from typing import Optional, Dict, Any


class AppException(Exception):
    """Base exception for all application errors"""
    
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        self.message = message
        self.details = details or {}
        super().__init__(self.message)


class DatabaseError(AppException):
    """Database-related errors"""
    pass


class ConfigurationError(AppException):
    """Configuration-related errors"""
    pass


class ValidationError(AppException):
    """Data validation errors"""
    pass


class UploadError(AppException):
    """File upload errors"""
    pass


class MappingError(AppException):
    """Column mapping errors"""
    pass


class ReconciliationError(AppException):
    """Reconciliation process errors"""
    pass


class ServiceUnavailableError(AppException):
    """External service unavailable errors"""
    pass

