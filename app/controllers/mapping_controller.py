"""
Mapping Controller - Handles column mapping operations
"""

from fastapi import HTTPException
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)


class MappingController:
    """Controller for mapping operations"""
    
    def __init__(self):
        pass
    
    def get_root_info(self) -> Dict[str, Any]:
        """Get root API information"""
        return {
            "service": "Smart Column Mapper API",
            "version": "2.0.0",
            "status": "operational",
            "documentation": "/docs",
            "health_check": "/api/health",
            "endpoints": {
                "core": ["/api/map-columns", "/api/upload", "/api/upload-data", "/api/validate-mapping", "/api/health"],
                "financial": ["/api/financial/health", "/api/financial/config/*", "/api/financial/reconciliation/*", "/api/financial/analytics/*"],
                "automation": ["/api/v1/automation/*", "/api/v1/jobs/*"]
            }
        }
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get health check status"""
        return {
            "status": "healthy",
            "service": "Smart Column Mapper API",
            "version": "2.0.0"
        }

