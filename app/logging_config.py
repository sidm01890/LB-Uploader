"""
Logging configuration for the application
"""
import logging
import sys
from typing import Optional
from datetime import datetime


def setup_logging(log_level: str = "INFO"):
    """
    Setup logging configuration
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    # Convert string level to logging constant
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)
    
    # Configure logging format
    logging.basicConfig(
        level=numeric_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # Set specific logger levels
    logging.getLogger("uvicorn").setLevel(logging.INFO)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    
    # Reduce PyMongo verbosity (connection pooling and heartbeat logs are too verbose)
    logging.getLogger("pymongo").setLevel(logging.WARNING)
    logging.getLogger("pymongo.connection").setLevel(logging.WARNING)
    logging.getLogger("pymongo.topology").setLevel(logging.WARNING)


class RequestLogger:
    """Request/response logger for API endpoints"""
    
    def log_response(self, endpoint: str, status_code: int, response_time: float, method: str):
        """Log API response"""
        logging.info(
            f"{method} {endpoint} - Status: {status_code} - Time: {response_time:.3f}s"
        )
    
    def log_error(self, endpoint: str, error: Exception, status_code: Optional[int] = None, 
                  validation_errors: Optional[list] = None):
        """Log API errors"""
        error_msg = f"Error in {endpoint}: {str(error)}"
        if status_code:
            error_msg += f" (Status: {status_code})"
        if validation_errors:
            error_msg += f" - Validation errors: {validation_errors}"
        logging.error(error_msg, exc_info=True)


# Create singleton instance
request_logger = RequestLogger()
