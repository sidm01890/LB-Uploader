"""
MongoDB Service - Handles MongoDB connections and operations
Stores uploaded file metadata and data in MongoDB
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
import uuid
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
from app.core.config import config

logger = logging.getLogger(__name__)


class MongoDBService:
    """Service for MongoDB operations"""
    
    def __init__(self):
        """Initialize MongoDB connection"""
        self.client = None
        self.db = None
        self._connect()
    
    def _connect(self):
        """Connect to MongoDB"""
        try:
            connection_string = config.mongodb.get_connection_string()
            logger.info(f"🔌 Connecting to MongoDB: {config.mongodb.host}:{config.mongodb.port}/{config.mongodb.database}")
            
            self.client = MongoClient(
                connection_string,
                maxPoolSize=config.mongodb.max_pool_size,
                minPoolSize=config.mongodb.min_pool_size,
                maxIdleTimeMS=config.mongodb.max_idle_time_ms,
                serverSelectionTimeoutMS=config.mongodb.server_selection_timeout_ms
            )
            
            # Test connection
            self.client.admin.command('ping')
            self.db = self.client[config.mongodb.database]
            
            logger.info("✅ MongoDB connection established successfully")
            
            # Create indexes
            self._create_indexes()
            
        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            logger.error(f"❌ Failed to connect to MongoDB: {e}")
            logger.warning("⚠️ MongoDB operations will be disabled. Files will only be stored on disk.")
            self.client = None
            self.db = None
        except Exception as e:
            logger.error(f"❌ MongoDB connection error: {e}")
            self.client = None
            self.db = None
    
    def _create_indexes(self):
        """Create indexes for better query performance"""
        if self.db is None:
            return
        
        try:
            # Index on upload_id (unique)
            self.db.uploaded_files.create_index("upload_id", unique=True)
            
            # Index on datasource
            self.db.uploaded_files.create_index("datasource")
            
            # Index on uploaded_at
            self.db.uploaded_files.create_index("uploaded_at")
            
            # Index on status
            self.db.uploaded_files.create_index("status")
            
            # Compound index for common queries
            self.db.uploaded_files.create_index([("datasource", 1), ("uploaded_at", -1)])
            
            logger.info("✅ MongoDB indexes created")
        except Exception as e:
            logger.warning(f"⚠️ Failed to create indexes: {e}")
    
    def is_connected(self) -> bool:
        """Check if MongoDB is connected"""
        if self.client is None or self.db is None:
            return False
        try:
            self.client.admin.command('ping')
            return True
        except Exception:
            return False
    
    def save_uploaded_file(
        self,
        filename: str,
        datasource: str,
        file_path: str,
        file_size: int,
        uploaded_by: str = "api_user"
    ) -> Optional[str]:
        """
        Save uploaded file metadata to MongoDB
        
        Args:
            filename: Name of the uploaded file
            datasource: Data source identifier
            file_path: Path where file is stored on disk
            file_size: Size of file in bytes
            uploaded_by: Username who uploaded the file
        
        Returns:
            upload_id if successful, None otherwise
        """
        if not self.is_connected():
            logger.warning("⚠️ MongoDB not connected, skipping metadata save")
            return None
        
        try:
            upload_id = str(uuid.uuid4())
            
            document = {
                "upload_id": upload_id,
                "filename": filename,
                "datasource": datasource.upper(),
                "file_path": file_path,
                "file_size": file_size,
                "file_type": filename.split('.')[-1].lower() if '.' in filename else 'unknown',
                "uploaded_at": datetime.utcnow(),
                "uploaded_by": uploaded_by,
                "status": "stored",
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow()
            }
            
            result = self.db.uploaded_files.insert_one(document)
            
            if result.inserted_id:
                logger.info(f"✅ File metadata saved to MongoDB: upload_id={upload_id}")
                return upload_id
            else:
                logger.warning("⚠️ Failed to save file metadata to MongoDB")
                return None
                
        except Exception as e:
            logger.error(f"❌ Error saving file metadata to MongoDB: {e}")
            return None
    
    def update_upload_status(
        self,
        upload_id: str,
        status: str,
        error: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Update upload status in MongoDB
        
        Args:
            upload_id: Upload ID
            status: New status (e.g., 'processing', 'completed', 'failed')
            error: Error message if failed
            metadata: Additional metadata to update
        
        Returns:
            True if successful, False otherwise
        """
        if not self.is_connected():
            return False
        
        try:
            update_data = {
                "status": status,
                "updated_at": datetime.utcnow()
            }
            
            if error:
                update_data["error"] = error
            
            if metadata:
                update_data.update(metadata)
            
            result = self.db.uploaded_files.update_one(
                {"upload_id": upload_id},
                {"$set": update_data}
            )
            
            if result.modified_count > 0:
                logger.info(f"✅ Updated upload status in MongoDB: upload_id={upload_id}, status={status}")
                return True
            else:
                logger.warning(f"⚠️ Upload ID not found in MongoDB: {upload_id}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error updating upload status in MongoDB: {e}")
            return False
    
    def get_upload_record(self, upload_id: str) -> Optional[Dict[str, Any]]:
        """Get upload record by upload_id"""
        if not self.is_connected():
            return None
        
        try:
            record = self.db.uploaded_files.find_one({"upload_id": upload_id})
            if record:
                # Convert ObjectId to string for JSON serialization
                record["_id"] = str(record["_id"])
                # Convert datetime to ISO format
                if "uploaded_at" in record:
                    record["uploaded_at"] = record["uploaded_at"].isoformat()
                if "created_at" in record:
                    record["created_at"] = record["created_at"].isoformat()
                if "updated_at" in record:
                    record["updated_at"] = record["updated_at"].isoformat()
            return record
        except Exception as e:
            logger.error(f"❌ Error getting upload record from MongoDB: {e}")
            return None
    
    def list_uploads(
        self,
        datasource: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        List uploads with optional filters
        
        Args:
            datasource: Filter by datasource
            status: Filter by status
            limit: Maximum number of records to return
        
        Returns:
            List of upload records
        """
        if not self.is_connected():
            return []
        
        try:
            query = {}
            if datasource:
                query["datasource"] = datasource.upper()
            if status:
                query["status"] = status
            
            records = self.db.uploaded_files.find(query).sort("uploaded_at", -1).limit(limit)
            
            result = []
            for record in records:
                # Convert ObjectId to string
                record["_id"] = str(record["_id"])
                # Convert datetime to ISO format
                if "uploaded_at" in record:
                    record["uploaded_at"] = record["uploaded_at"].isoformat()
                if "created_at" in record:
                    record["created_at"] = record["created_at"].isoformat()
                if "updated_at" in record:
                    record["updated_at"] = record["updated_at"].isoformat()
                result.append(record)
            
            return result
        except Exception as e:
            logger.error(f"❌ Error listing uploads from MongoDB: {e}")
            return []
    
    def list_all_collections(self) -> List[str]:
        """
        List all collection names in the database
        
        Returns:
            List of collection names (empty list if not connected)
        """
        if not self.is_connected():
            logger.warning("⚠️ MongoDB not connected, cannot list collections")
            return []
        
        try:
            collections = self.db.list_collection_names()
            # Filter out system collections (optional - you can remove this if you want to include them)
            user_collections = [col for col in collections if not col.startswith("system.")]
            logger.info(f"📋 Found {len(user_collections)} collections in database")
            return sorted(user_collections)  # Return sorted list for consistency
        except Exception as e:
            logger.error(f"❌ Error listing collections: {e}")
            return []
    
    def create_collection(self, collection_name: str) -> Dict[str, Any]:
        """
        Create a new collection in MongoDB if it doesn't exist
        
        Args:
            collection_name: Name of the collection to create (will be converted to lowercase)
        
        Returns:
            Dictionary with status and message
        
        Raises:
            ValueError: If collection already exists
        """
        if not self.is_connected():
            raise ConnectionError("MongoDB is not connected")
        
        # Convert to lowercase as requested
        collection_name_lower = collection_name.lower()
        
        # Check if collection already exists
        existing_collections = self.db.list_collection_names()
        if collection_name_lower in existing_collections:
            raise ValueError(f"Collection '{collection_name_lower}' already exists")
        
        # Create the collection (MongoDB creates collections lazily, so we insert an empty doc and delete it)
        collection = self.db[collection_name_lower]
        
        # Create collection by inserting and immediately deleting a document
        # This ensures the collection is created with proper structure
        temp_doc = {"_temp": True, "created_at": datetime.utcnow()}
        result = collection.insert_one(temp_doc)
        collection.delete_one({"_id": result.inserted_id})
        
        logger.info(f"✅ Created new collection: {collection_name_lower}")
        
        return {
            "status": "success",
            "message": f"Collection '{collection_name_lower}' created successfully",
            "collection_name": collection_name_lower
        }
    
    def close(self):
        """Close MongoDB connection"""
        if self.client is not None:
            self.client.close()
            logger.info("🔌 MongoDB connection closed")


# Global MongoDB service instance
mongodb_service = MongoDBService()

