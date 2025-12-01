"""
Legacy database module - maintained for backward compatibility
New code should use app.core.database instead
"""

from app.core.database import get_mysql_connection, db_manager

# Re-export for backward compatibility
__all__ = ['get_mysql_connection', 'db_manager']

