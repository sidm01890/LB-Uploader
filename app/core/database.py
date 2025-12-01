"""
Unified Database Management
Provides connection pooling, async support, and consistent error handling
"""

import logging
from contextlib import asynccontextmanager, contextmanager
from typing import Optional, Dict, Any, List
from sqlalchemy import create_engine, text, pool
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import sessionmaker, Session
import mysql.connector
from mysql.connector import Error as MySQLError

from app.core.config import config

logger = logging.getLogger(__name__)


class DatabaseManager:
    """
    Unified database connection manager with connection pooling
    
    Supports both sync and async operations:
    - Sync: For legacy code and simple queries
    - Async: For FastAPI routes and modern async code
    """
    
    def __init__(self):
        """Initialize database manager with connection pooling"""
        self.sync_engine = None
        self.async_engine = None
        self.async_session_factory = None
        self.sync_session_factory = None
        self._initialize_engines()
    
    def _initialize_engines(self):
        """Initialize SQLAlchemy engines for sync and async operations"""
        try:
            # Sync engine for legacy code
            sync_connection_str = (
                f"mysql+mysqlconnector://{config.database.user}:{config.database.password}"
                f"@{config.database.host}:{config.database.port}/{config.database.database}"
            )
            
            self.sync_engine = create_engine(
                sync_connection_str,
                poolclass=pool.QueuePool,
                pool_size=config.db_pool_size,
                max_overflow=config.db_max_overflow,
                pool_recycle=config.db_pool_recycle,
                pool_pre_ping=True,  # Verify connections before using
                echo=False
            )
            
            self.sync_session_factory = sessionmaker(
                bind=self.sync_engine,
                autocommit=False,
                autoflush=False
            )
            
            # Async engine for modern async code
            # Note: Using aiomysql for async support
            try:
                async_connection_str = (
                    f"mysql+aiomysql://{config.database.user}:{config.database.password}"
                    f"@{config.database.host}:{config.database.port}/{config.database.database}"
                )
                
                self.async_engine = create_async_engine(
                    async_connection_str,
                    pool_size=config.db_pool_size,
                    max_overflow=config.db_max_overflow,
                    pool_recycle=config.db_pool_recycle,
                    pool_pre_ping=True,
                    echo=False
                )
                
                self.async_session_factory = async_sessionmaker(
                    bind=self.async_engine,
                    class_=AsyncSession,
                    expire_on_commit=False
                )
                
                logger.info("✅ Async database engine initialized")
            except ImportError:
                logger.warning("⚠️ aiomysql not installed, async support disabled")
                logger.info("Install with: pip install aiomysql")
                self.async_engine = None
                self.async_session_factory = None
            
            logger.info("✅ Database manager initialized successfully")
            logger.info(f"   Pool size: {config.db_pool_size}, Max overflow: {config.db_max_overflow}")
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize database manager: {e}")
            raise
    
    @contextmanager
    def get_sync_session(self):
        """
        Get synchronous database session with automatic cleanup
        
        Usage:
            with db_manager.get_sync_session() as session:
                result = session.execute(query)
        """
        session = self.sync_session_factory()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Database session error: {e}")
            raise
        finally:
            session.close()
    
    @asynccontextmanager
    async def get_async_session(self):
        """
        Get asynchronous database session with automatic cleanup
        
        Usage:
            async with db_manager.get_async_session() as session:
                result = await session.execute(query)
        """
        if not self.async_session_factory:
            raise RuntimeError("Async engine not available. Install aiomysql.")
        
        async with self.async_session_factory() as session:
            try:
                yield session
                await session.commit()
            except Exception as e:
                await session.rollback()
                logger.error(f"Async database session error: {e}")
                raise
    
    def get_connection_dict(self) -> Dict[str, Any]:
        """
        Get connection dictionary for mysql.connector (legacy compatibility)
        
        Returns:
            Dictionary with connection parameters
        """
        return {
            'host': config.database.host,
            'port': config.database.port,
            'user': config.database.user,
            'password': config.database.password,
            'database': config.database.database
        }
    
    @contextmanager
    def get_mysql_connector(self):
        """
        Get mysql.connector connection (for legacy code migration)
        
        Usage:
            with db_manager.get_mysql_connector() as conn:
                cursor = conn.cursor(dictionary=True)
                cursor.execute(query)
        """
        conn = None
        try:
            conn = mysql.connector.connect(**self.get_connection_dict())
            yield conn
        except MySQLError as e:
            logger.error(f"MySQL connector error: {e}")
            raise
        finally:
            if conn and conn.is_connected():
                conn.close()
    
    def execute_query(self, query: str, params: Optional[Dict] = None) -> List[Dict[str, Any]]:
        """
        Execute a synchronous query and return results
        
        Args:
            query: SQL query string
            params: Query parameters
            
        Returns:
            List of result dictionaries
        """
        with self.get_sync_session() as session:
            result = session.execute(text(query), params or {})
            rows = result.fetchall()
            # Convert to list of dicts
            return [dict(row._mapping) for row in rows]
    
    async def execute_query_async(self, query: str, params: Optional[Dict] = None) -> List[Dict[str, Any]]:
        """
        Execute an asynchronous query and return results
        
        Args:
            query: SQL query string
            params: Query parameters
            
        Returns:
            List of result dictionaries
        """
        async with self.get_async_session() as session:
            result = await session.execute(text(query), params or {})
            rows = result.fetchall()
            return [dict(row._mapping) for row in rows]
    
    def test_connection(self) -> bool:
        """Test database connection"""
        try:
            with self.get_sync_session() as session:
                session.execute(text("SELECT 1"))
            logger.info("✅ Database connection test successful")
            return True
        except Exception as e:
            logger.error(f"❌ Database connection test failed: {e}")
            return False
    
    def close(self):
        """Close all database connections"""
        if self.sync_engine:
            self.sync_engine.dispose()
        if self.async_engine:
            # Async engine cleanup would be done in async context
            pass
        logger.info("Database connections closed")


# Global database manager instance
db_manager = DatabaseManager()

# Backward compatibility function
def get_mysql_connection():
    """
    Backward compatibility function for existing code
    Returns SQLAlchemy engine (sync)
    """
    return db_manager.sync_engine

