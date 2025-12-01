"""
MongoDB service for storing uploaded sheets and upload metadata
"""

from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
from typing import Dict, List, Any, Optional
from datetime import datetime
import logging
import uuid
import json

from app.core.config import config

logger = logging.getLogger(__name__)

# Global MongoDB client and database instances
_mongo_client: Optional[MongoClient] = None
_mongo_database: Optional[Database] = None


def get_mongodb_client() -> MongoClient:
    """Get or create MongoDB client"""
    global _mongo_client
    
    if _mongo_client is not None:
        return _mongo_client
    
    try:
        connection_string = config.mongodb.get_connection_string()
        logger.info(f"Connecting to MongoDB: {config.mongodb.host}:{config.mongodb.port}")
        logger.info(f"Database: {config.mongodb.database}")
        
        _mongo_client = MongoClient(
            connection_string,
            maxPoolSize=config.mongodb.max_pool_size,
            minPoolSize=config.mongodb.min_pool_size,
            maxIdleTimeMS=config.mongodb.max_idle_time_ms,
            serverSelectionTimeoutMS=config.mongodb.server_selection_timeout_ms
        )
        
        # Test connection
        _mongo_client.admin.command('ping')
        logger.info("✅ MongoDB connection successful")
        
        return _mongo_client
        
    except ConnectionFailure as e:
        logger.error(f"❌ MongoDB connection failed: {e}")
        raise
    except ServerSelectionTimeoutError as e:
        logger.error(f"❌ MongoDB server selection timeout: {e}")
        raise
    except Exception as e:
        logger.error(f"❌ Unexpected MongoDB error: {e}")
        raise


def get_mongodb_database() -> Database:
    """Get MongoDB database instance"""
    global _mongo_database
    
    if _mongo_database is not None:
        return _mongo_database
    
    try:
        client = get_mongodb_client()
        _mongo_database = client[config.mongodb.database]
        logger.info(f"✅ MongoDB database '{config.mongodb.database}' accessed")
        return _mongo_database
        
    except Exception as e:
        logger.error(f"❌ Error accessing MongoDB database: {e}")
        raise


def get_collection_name(datasource: Optional[str] = None) -> str:
    """
    Get MongoDB collection name based on datasource.
    If datasource is provided, use datasource-specific collection.
    Otherwise, use default 'uploaded_sheets' collection.
    
    Args:
        datasource: Data source identifier (e.g., ZOMATO, TRM, POS_ORDERS)
    
    Returns:
        Collection name (lowercase, normalized)
    """
    if datasource:
        # Normalize datasource name to collection name
        # Convert to lowercase and replace special characters
        collection_name = datasource.lower().strip()
        # Replace spaces and special chars with underscores
        collection_name = collection_name.replace(' ', '_').replace('-', '_')
        # Remove any remaining special characters
        collection_name = ''.join(c if c.isalnum() or c == '_' else '_' for c in collection_name)
        # Remove multiple underscores
        while '__' in collection_name:
            collection_name = collection_name.replace('__', '_')
        # Remove leading/trailing underscores
        collection_name = collection_name.strip('_')
        
        # Ensure it's a valid collection name (not empty)
        if not collection_name:
            collection_name = "uploaded_sheets"
        
        return collection_name
    else:
        return "uploaded_sheets"


def get_uploaded_sheets_collection(datasource: Optional[str] = None) -> Collection:
    """
    Get the MongoDB collection for uploaded sheets.
    Uses datasource-specific collection if datasource is provided.
    
    Args:
        datasource: Data source identifier (e.g., ZOMATO, TRM)
    
    Returns:
        MongoDB Collection object
    """
    db = get_mongodb_database()
    collection_name = get_collection_name(datasource)
    collection = db[collection_name]
    
    # Create indexes if they don't exist
    try:
        collection.create_index("upload_id", unique=True)
        collection.create_index("datasource")
        collection.create_index("uploaded_at")
        collection.create_index("status")
        collection.create_index([("datasource", 1), ("uploaded_at", -1)])
    except Exception as e:
        # Indexes might already exist
        logger.debug(f"Index creation (may already exist): {e}")
    
    return collection


def save_uploaded_sheet(
    filename: str,
    datasource: str,
    table_name: str,
    file_size: int,
    file_type: str,
    headers: List[str],
    raw_data: Dict[str, List[Any]],  # Changed: column-based format {column_name: [values]}
    uploaded_by: Optional[str] = None,
    column_mapping: Optional[Dict[str, str]] = None
) -> str:
    """
    Save only raw data to MongoDB in column-based format.
    raw_data format: {column_name: [value1, value2, ...], ...}
    Uses datasource-specific collection (e.g., 'zomato' for ZOMATO datasource).
    
    Returns:
        upload_id: Unique identifier for this upload (for backward compatibility, but not saved)
    """
    try:
        # Get datasource-specific collection
        collection = get_uploaded_sheets_collection(datasource)
        collection_name = get_collection_name(datasource)
        
        logger.info(f"Saving raw data to MongoDB collection: {collection_name} (datasource: {datasource})")
        
        upload_id = str(uuid.uuid4())
        
        # Only save raw_data - MongoDB will automatically add _id
        document = {
            "raw_data": raw_data
        }
        
        # Count rows from first column (all columns should have same length)
        row_count = len(list(raw_data.values())[0]) if raw_data else 0
        
        result = collection.insert_one(document)
        logger.info(f"✅ Saved raw data to MongoDB collection '{collection_name}': {row_count} rows, {len(raw_data)} columns")
        
        return upload_id
        
    except Exception as e:
        logger.error(f"❌ Error saving raw data to MongoDB: {e}")
        raise


def update_upload_status(
    upload_id: str,
    status: str,
    validation_results: Optional[Dict[str, Any]] = None,
    mysql_upload_results: Optional[Dict[str, Any]] = None,
    error: Optional[str] = None,
    datasource: Optional[str] = None
) -> bool:
    """
    Update upload status and results in MongoDB.
    Uses datasource-specific collection if datasource is provided.
    
    Args:
        upload_id: Unique upload identifier
        status: New status (processing|completed|failed)
        validation_results: Validation results dictionary
        mysql_upload_results: MySQL upload results dictionary
        error: Error message if failed
        datasource: Data source identifier (to find correct collection)
    """
    try:
        # If datasource not provided, try to find it from the upload record
        if not datasource:
            # Try default collection first
            default_collection = get_uploaded_sheets_collection()
            record = default_collection.find_one({"upload_id": upload_id})
            if record:
                datasource = record.get("datasource")
        
        collection = get_uploaded_sheets_collection(datasource)
        
        update_data = {
            "status": status,
            "updated_at": datetime.utcnow()
        }
        
        if validation_results is not None:
            update_data["validation_results"] = validation_results
        
        if mysql_upload_results is not None:
            update_data["mysql_upload_results"] = mysql_upload_results
        
        if error is not None:
            update_data["error"] = error
        
        if status == "completed" or status == "failed":
            update_data["processing_completed_at"] = datetime.utcnow()
        
        result = collection.update_one(
            {"upload_id": upload_id},
            {"$set": update_data}
        )
        
        if result.modified_count > 0:
            logger.info(f"✅ Updated upload status: upload_id={upload_id}, status={status}")
            return True
        else:
            logger.warning(f"⚠️ No document found to update: upload_id={upload_id}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error updating upload status: {e}")
        return False


def get_upload_record(upload_id: str, datasource: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    Get upload record by upload_id.
    Searches datasource-specific collection if provided, otherwise searches all collections.
    
    Args:
        upload_id: Unique upload identifier
        datasource: Data source identifier (to search specific collection)
    """
    try:
        if datasource:
            # Search specific collection
            collection = get_uploaded_sheets_collection(datasource)
            record = collection.find_one({"upload_id": upload_id})
        else:
            # Search all collections (try datasource-specific first, then default)
            # Get all collection names
            db = get_mongodb_database()
            collection_names = db.list_collection_names()
            
            record = None
            # Try datasource-specific collections first
            for coll_name in collection_names:
                if coll_name != "uploaded_sheets":  # Skip default, try it last
                    collection = db[coll_name]
                    record = collection.find_one({"upload_id": upload_id})
                    if record:
                        break
            
            # If not found, try default collection
            if not record:
                collection = get_uploaded_sheets_collection()
                record = collection.find_one({"upload_id": upload_id})
        
        if record:
            # Convert ObjectId to string for JSON serialization
            record['_id'] = str(record['_id'])
        
        return record
        
    except Exception as e:
        logger.error(f"❌ Error getting upload record: {e}")
        return None


def list_uploads(
    datasource: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 100,
    skip: int = 0
) -> List[Dict[str, Any]]:
    """
    List upload records with optional filters.
    Uses datasource-specific collection if datasource is provided.
    
    Args:
        datasource: Filter by datasource (also determines which collection to query)
        status: Filter by status
        limit: Maximum number of records to return
        skip: Number of records to skip
    """
    try:
        # Use datasource-specific collection if provided
        collection = get_uploaded_sheets_collection(datasource)
        
        query = {}
        if datasource:
            query["datasource"] = datasource
        if status:
            query["status"] = status
        
        records = collection.find(query).sort("uploaded_at", -1).skip(skip).limit(limit)
        
        result = []
        for record in records:
            record['_id'] = str(record['_id'])
            result.append(record)
        
        return result
        
    except Exception as e:
        logger.error(f"❌ Error listing uploads: {e}")
        return []


def close_mongodb_connection():
    """Close MongoDB connection"""
    global _mongo_client, _mongo_database
    
    if _mongo_client:
        try:
            _mongo_client.close()
            logger.info("✅ MongoDB connection closed")
        except Exception as e:
            logger.error(f"❌ Error closing MongoDB connection: {e}")
        finally:
            _mongo_client = None
            _mongo_database = None

