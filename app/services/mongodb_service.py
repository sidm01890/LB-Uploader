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
        List all collection names from raw_data_collection
        
        Returns:
            List of collection names (empty list if not connected or collection doesn't exist)
        """
        if not self.is_connected():
            logger.warning("⚠️ MongoDB not connected, cannot list collections")
            return []
        
        try:
            raw_data_collection = self.db["raw_data_collection"]
            # Check if collection exists (it might not exist if no collections were created via API yet)
            if "raw_data_collection" not in self.db.list_collection_names():
                logger.info("📋 raw_data_collection does not exist yet - returning empty list")
                return []
            
            # Find all documents and extract collection names
            documents = raw_data_collection.find({}, {"collection_name": 1})
            collections = [doc.get("collection_name") for doc in documents if doc.get("collection_name")]
            logger.info(f"📋 Found {len(collections)} collections in raw_data_collection")
            return sorted(collections)  # Return sorted list for consistency
        except Exception as e:
            logger.error(f"❌ Error listing collections from raw_data_collection: {e}")
            return []

    def create_collection(
        self,
        collection_name: str,
        unique_ids: List[str] = None
    ) -> Dict[str, Any]:
        """
        Create a new collection and its processed version in MongoDB if they don't exist
        
        Args:
            collection_name: Name of the collection to create (will be converted to lowercase)
            unique_ids: List of field names that form unique identifiers (can be empty)
        
        Returns:
            Dictionary with status and message
        
        Raises:
            ValueError: If collection already exists
        """
        if not self.is_connected():
            raise ConnectionError("MongoDB is not connected")
        
        if unique_ids is None:
            unique_ids = []
        
        # Convert to lowercase as requested
        collection_name_lower = collection_name.lower()
        processed_collection_name = f"{collection_name_lower}_processed"
        
        # Check if collections already exist
        existing_collections = self.db.list_collection_names()
        if collection_name_lower in existing_collections:
            raise ValueError(f"Collection '{collection_name_lower}' already exists")
        if processed_collection_name in existing_collections:
            raise ValueError(f"Processed collection '{processed_collection_name}' already exists")
        
        # Create the main collection (MongoDB creates collections lazily, so we insert an empty doc and delete it)
        collection = self.db[collection_name_lower]
        temp_doc = {"_temp": True, "created_at": datetime.utcnow()}
        result = collection.insert_one(temp_doc)
        collection.delete_one({"_id": result.inserted_id})
        logger.info(f"✅ Created new collection: {collection_name_lower}")
        
        # Create the processed collection
        processed_collection = self.db[processed_collection_name]
        temp_doc_processed = {"_temp": True, "created_at": datetime.utcnow()}
        result_processed = processed_collection.insert_one(temp_doc_processed)
        processed_collection.delete_one({"_id": result_processed.inserted_id})
        logger.info(f"✅ Created processed collection: {processed_collection_name}")
        
        # Save entry to raw_data_collection
        try:
            raw_data_collection = self.db["raw_data_collection"]
            # Check if entry already exists (shouldn't happen, but just in case)
            existing_entry = raw_data_collection.find_one({"collection_name": collection_name_lower})
            if not existing_entry:
                entry_doc = {
                    "collection_name": collection_name_lower,
                    "processed_collection_name": processed_collection_name,
                    "unique_ids": unique_ids,
                    "created_at": datetime.utcnow(),
                    "created_via_api": True
                }
                raw_data_collection.insert_one(entry_doc)
                logger.info(f"📝 Added '{collection_name_lower}' to raw_data_collection with unique_ids: {unique_ids}")
            else:
                logger.warning(f"⚠️ Entry for '{collection_name_lower}' already exists in raw_data_collection")
        except Exception as e:
            logger.error(f"❌ Failed to save entry to raw_data_collection: {e}")
            # Don't fail the whole operation if raw_data_collection save fails
        
        return {
            "status": "success",
            "message": f"Collection '{collection_name_lower}' and processed collection '{processed_collection_name}' created successfully",
            "collection_name": collection_name_lower,
            "processed_collection_name": processed_collection_name,
            "unique_ids": unique_ids
        }
    
    def update_collection_unique_ids(
        self,
        collection_name: str,
        unique_ids: List[str]
    ) -> Dict[str, Any]:
        """
        Update unique_ids for an existing collection in raw_data_collection
        
        Args:
            collection_name: Name of the collection (will be converted to lowercase)
            unique_ids: List of field names that form unique identifiers
        
        Returns:
            Dictionary with status and message
        
        Raises:
            ValueError: If collection doesn't exist in raw_data_collection
            ConnectionError: If MongoDB is not connected
        """
        if not self.is_connected():
            raise ConnectionError("MongoDB is not connected")
        
        collection_name_lower = collection_name.lower()
        
        try:
            raw_data_collection = self.db["raw_data_collection"]
            
            # Check if collection exists in raw_data_collection
            existing_entry = raw_data_collection.find_one({"collection_name": collection_name_lower})
            if not existing_entry:
                raise ValueError(f"Collection '{collection_name_lower}' not found in raw_data_collection. Please create the collection first.")
            
            # Update unique_ids
            raw_data_collection.update_one(
                {"collection_name": collection_name_lower},
                {
                    "$set": {
                        "unique_ids": unique_ids,
                        "updated_at": datetime.utcnow()
                    }
                }
            )
            
            logger.info(f"✅ Updated unique_ids for collection '{collection_name_lower}': {unique_ids}")
            
            return {
                "status": "success",
                "message": f"Unique IDs updated successfully for collection '{collection_name_lower}'",
                "collection_name": collection_name_lower,
                "unique_ids": unique_ids
            }
            
        except ValueError:
            raise
        except ConnectionError:
            raise
        except Exception as e:
            logger.error(f"❌ Error updating unique_ids for collection '{collection_name_lower}': {e}")
            raise ValueError(f"Failed to update unique_ids: {str(e)}")
    
    def get_collection_unique_ids(self, collection_name: str) -> Optional[Dict[str, Any]]:
        """
        Get unique_ids for a collection from raw_data_collection
        
        Args:
            collection_name: Name of the collection (will be converted to lowercase)
        
        Returns:
            Dictionary with collection info and unique_ids, or None if not found
        
        Raises:
            ConnectionError: If MongoDB is not connected
        """
        if not self.is_connected():
            raise ConnectionError("MongoDB is not connected")
        
        collection_name_lower = collection_name.lower()
        
        try:
            raw_data_collection = self.db["raw_data_collection"]
            
            # Find the collection entry
            entry = raw_data_collection.find_one({"collection_name": collection_name_lower})
            
            if not entry:
                return None
            
            # Extract unique_ids (default to empty list if not present)
            unique_ids = entry.get("unique_ids", [])
            
            # Prepare response
            result = {
                "collection_name": collection_name_lower,
                "unique_ids": unique_ids,
                "unique_ids_count": len(unique_ids)
            }
            
            # Add optional fields if they exist
            if "processed_collection_name" in entry:
                result["processed_collection_name"] = entry["processed_collection_name"]
            if "created_at" in entry:
                result["created_at"] = entry["created_at"].isoformat()
            if "updated_at" in entry:
                result["updated_at"] = entry["updated_at"].isoformat()
            
            logger.info(f"✅ Retrieved unique_ids for collection '{collection_name_lower}': {unique_ids}")
            
            return result
            
        except ConnectionError:
            raise
        except Exception as e:
            logger.error(f"❌ Error getting unique_ids for collection '{collection_name_lower}': {e}")
            return None
    
    def get_collection_keys(self, collection_name: str) -> List[str]:
        """
        Get unique keys from all documents in a collection
        
        Args:
            collection_name: Name of the collection (will be converted to lowercase)
        
        Returns:
            List of unique keys (excluding _id, created_at, updated_at)
        
        Raises:
            ValueError: If collection doesn't exist
            ConnectionError: If MongoDB is not connected
        """
        if not self.is_connected():
            raise ConnectionError("MongoDB is not connected")
        
        collection_name_lower = collection_name.lower()
        
        # Check if collection exists
        existing_collections = self.db.list_collection_names()
        if collection_name_lower not in existing_collections:
            raise ValueError(f"Collection '{collection_name_lower}' does not exist")
        
        try:
            collection = self.db[collection_name_lower]
            
            # Get a sample of documents to extract keys
            # We'll check multiple documents to get all possible keys
            all_keys = set()
            
            # Get all documents (or a reasonable sample)
            # For large collections, we might want to limit, but for now get all
            documents = collection.find({}).limit(1000)  # Limit to first 1000 docs for performance
            
            for doc in documents:
                # Extract all keys from the document
                keys = doc.keys()
                all_keys.update(keys)
            
            # Exclude system fields
            excluded_keys = {"_id", "created_at", "updated_at"}
            user_keys = sorted([key for key in all_keys if key not in excluded_keys])
            
            logger.info(f"🔑 Found {len(user_keys)} unique keys in collection '{collection_name_lower}'")
            
            return user_keys
            
        except ValueError:
            # Re-raise ValueError
            raise
        except Exception as e:
            logger.error(f"❌ Error getting keys from collection '{collection_name_lower}': {e}")
            raise ValueError(f"Failed to get keys from collection: {str(e)}")
    
    def save_collection_field_mapping(
        self,
        collection_name: str,
        selected_fields: List[str]
    ) -> Dict[str, Any]:
        """
        Save or update field mapping for a collection
        
        Args:
            collection_name: Name of the collection (will be converted to lowercase)
            selected_fields: List of field names to use for this collection
        
        Returns:
            Dictionary with status and message
        
        Raises:
            ValueError: If collection doesn't exist
            ConnectionError: If MongoDB is not connected
        """
        if not self.is_connected():
            raise ConnectionError("MongoDB is not connected")
        
        collection_name_lower = collection_name.lower()
        
        # Check if collection exists
        existing_collections = self.db.list_collection_names()
        if collection_name_lower not in existing_collections:
            raise ValueError(f"Collection '{collection_name_lower}' does not exist")
        
        try:
            # Get all available keys from the collection for validation
            all_keys = self.get_collection_keys(collection_name_lower)
            
            # Validate that all selected fields exist in the collection
            invalid_fields = [field for field in selected_fields if field not in all_keys]
            if invalid_fields:
                raise ValueError(
                    f"Invalid fields for collection '{collection_name_lower}': {', '.join(invalid_fields)}. "
                    f"Available fields: {', '.join(all_keys)}"
                )
            
            # Save to collection_field_mappings collection
            mappings_collection = self.db["collection_field_mappings"]
            
            mapping_doc = {
                "collection_name": collection_name_lower,
                "selected_fields": selected_fields,
                "total_available_fields": len(all_keys),
                "updated_at": datetime.utcnow()
            }
            
            # Check if mapping already exists
            existing_mapping = mappings_collection.find_one({"collection_name": collection_name_lower})
            
            if existing_mapping:
                # Update existing mapping
                mappings_collection.update_one(
                    {"collection_name": collection_name_lower},
                    {"$set": mapping_doc}
                )
                logger.info(f"🔄 Updated field mapping for collection '{collection_name_lower}'")
                action = "updated"
            else:
                # Create new mapping
                mapping_doc["created_at"] = datetime.utcnow()
                mappings_collection.insert_one(mapping_doc)
                logger.info(f"✅ Created field mapping for collection '{collection_name_lower}'")
                action = "created"
            
            return {
                "status": "success",
                "message": f"Field mapping {action} successfully for collection '{collection_name_lower}'",
                "collection_name": collection_name_lower,
                "selected_fields_count": len(selected_fields),
                "total_available_fields": len(all_keys)
            }
            
        except ValueError:
            # Re-raise ValueError
            raise
        except Exception as e:
            logger.error(f"❌ Error saving field mapping for collection '{collection_name_lower}': {e}")
            raise ValueError(f"Failed to save field mapping: {str(e)}")
    
    def get_collection_field_mapping(self, collection_name: str) -> Optional[Dict[str, Any]]:
        """
        Get field mapping for a collection
        
        Args:
            collection_name: Name of the collection (will be converted to lowercase)
        
        Returns:
            Dictionary with mapping data or None if not found
        
        Raises:
            ConnectionError: If MongoDB is not connected
        """
        if not self.is_connected():
            raise ConnectionError("MongoDB is not connected")
        
        collection_name_lower = collection_name.lower()
        
        try:
            mappings_collection = self.db["collection_field_mappings"]
            mapping = mappings_collection.find_one({"collection_name": collection_name_lower})
            
            if mapping:
                # Convert ObjectId to string and datetime to ISO format
                mapping["_id"] = str(mapping["_id"])
                if "created_at" in mapping:
                    mapping["created_at"] = mapping["created_at"].isoformat()
                if "updated_at" in mapping:
                    mapping["updated_at"] = mapping["updated_at"].isoformat()
            
            return mapping
            
        except Exception as e:
            logger.error(f"❌ Error getting field mapping for collection '{collection_name_lower}': {e}")
            return None
    
    def list_all_field_mappings(self) -> List[Dict[str, Any]]:
        """
        List all field mappings
        
        Returns:
            List of field mappings (empty list if not connected)
        """
        if not self.is_connected():
            logger.warning("⚠️ MongoDB not connected, cannot list field mappings")
            return []
        
        try:
            mappings_collection = self.db["collection_field_mappings"]
            mappings = list(mappings_collection.find({}))
            
            # Convert ObjectId to string and datetime to ISO format
            for mapping in mappings:
                mapping["_id"] = str(mapping["_id"])
                if "created_at" in mapping:
                    mapping["created_at"] = mapping["created_at"].isoformat()
                if "updated_at" in mapping:
                    mapping["updated_at"] = mapping["updated_at"].isoformat()
            
            logger.info(f"📋 Found {len(mappings)} field mapping(s)")
            return mappings
            
        except Exception as e:
            logger.error(f"❌ Error listing field mappings: {e}")
            return []
    
    def save_excel_data_row_wise(
        self,
        collection_name: str,
        row_data: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Save Excel data in row-wise format to MongoDB collection.
        Each row becomes a separate document in the collection.
        Collection name will be converted to lowercase.
        
        Args:
            collection_name: Name of the collection (will be converted to lowercase)
            row_data: List of dictionaries where each dictionary represents a row
                     Example: [{"Order ID": 123, "Amount": 100}, {"Order ID": 456, "Amount": 200}]
            
        Returns:
            Dictionary with status and insertion details
        """
        if not self.is_connected():
            logger.warning("⚠️ MongoDB not connected, skipping data save")
            return {
                "success": False,
                "message": "MongoDB not connected",
                "rows_inserted": 0
            }
        
        try:
            # Convert collection name to lowercase
            collection_name_lower = collection_name.lower()
            collection = self.db[collection_name_lower]
            
            # Drop upload_id index if it exists (since our row documents don't use upload_id)
            try:
                # Check if index exists and drop it
                indexes = list(collection.list_indexes())
                for index in indexes:
                    index_keys = index.get('key', {})
                    if 'upload_id' in index_keys:
                        index_name = index.get('name', 'upload_id_1')
                        collection.drop_index(index_name)
                        logger.info(f"✅ Dropped upload_id index '{index_name}' from collection '{collection_name_lower}'")
            except Exception as index_error:
                # Index might not exist or already dropped, which is fine
                logger.debug(f"Index check for '{collection_name_lower}': {index_error}")
            
            # Create separate document for each row
            current_time = datetime.utcnow()
            documents = []
            
            for row in row_data:
                # Each row becomes a separate document
                # Add timestamps to each row document
                document = dict(row)  # Create a copy of the row
                document["created_at"] = current_time
                document["updated_at"] = current_time
                documents.append(document)
            
            # Insert all documents
            if documents:
                result = collection.insert_many(documents)
                rows_inserted = len(result.inserted_ids)
                
                logger.info(f"✅ Saved {rows_inserted} row documents to MongoDB collection '{collection_name_lower}'")
                return {
                    "success": True,
                    "message": f"Successfully saved {rows_inserted} row documents to collection '{collection_name_lower}'",
                    "rows_inserted": rows_inserted,
                    "columns_count": len(row_data[0]) if row_data else 0,
                    "collection_name": collection_name_lower
                }
            else:
                logger.warning(f"⚠️ No documents to insert into collection '{collection_name_lower}'")
                return {
                    "success": False,
                    "message": "No documents to insert",
                    "rows_inserted": 0
                }
                
        except Exception as e:
            logger.error(f"❌ Error saving Excel data to MongoDB collection '{collection_name}': {e}")
            return {
                "success": False,
                "message": f"Error saving data: {str(e)}",
                "rows_inserted": 0
            }
    
    def collection_exists(self, collection_name: str) -> bool:
        """
        Check if a MongoDB collection exists
        
        Args:
            collection_name: Name of the collection to check (will be converted to lowercase)
        
        Returns:
            True if collection exists, False otherwise
        """
        if not self.is_connected():
            return False
        
        try:
            collection_name_lower = collection_name.lower()
            existing_collections = self.db.list_collection_names()
            return collection_name_lower in existing_collections
        except Exception as e:
            logger.error(f"❌ Error checking if collection '{collection_name}' exists: {e}")
            return False
    
    def save_report_formulas(
        self,
        report_name: str,
        formulas: List[Dict[str, str]]
    ) -> Dict[str, Any]:
        """
        Save report formulas to the 'formulas' collection.
        If the collection doesn't exist, it will be created automatically.
        
        Args:
            report_name: Name of the report (converted to lowercase)
            formulas: List of formula dictionaries with 'formula_name' and 'formula_value'
        
        Returns:
            Dictionary with status and message
        
        Raises:
            ConnectionError: If MongoDB is not connected
        """
        if not self.is_connected():
            raise ConnectionError("MongoDB is not connected")
        
        if not report_name or not report_name.strip():
            raise ValueError("Report name is required and cannot be empty")
        
        # Allow empty formulas array - no validation needed
        
        try:
            report_name_lower = report_name.lower().strip()
            collection = self.db["formulas"]
            
            # Check if collection exists
            collection_exists = self.collection_exists("formulas")
            
            # Prepare document with report metadata and formulas
            report_doc = {
                "report_name": report_name_lower,
                "formulas": formulas,
                "formulas_count": len(formulas),
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow()
            }
            
            # Check if document with this report_name already exists
            existing_doc = collection.find_one({"report_name": report_name_lower})
            
            if existing_doc:
                # Update existing document
                collection.update_one(
                    {"report_name": report_name_lower},
                    {"$set": {
                        "formulas": formulas,
                        "formulas_count": len(formulas),
                        "updated_at": datetime.utcnow()
                    }}
                )
                action = "updated"
                logger.info(f"🔄 Updated report formulas for '{report_name_lower}' in 'formulas' collection")
            else:
                # Insert new document
                collection.insert_one(report_doc)
                action = "created"
                logger.info(f"✅ Created report formulas for '{report_name_lower}' in 'formulas' collection")
            
            return {
                "status": "success",
                "message": f"Report formulas {action} successfully for '{report_name_lower}' in 'formulas' collection",
                "report_name": report_name_lower,
                "formulas_count": len(formulas),
                "collection_existed": collection_exists
            }
            
        except ValueError:
            raise
        except ConnectionError:
            raise
        except Exception as e:
            logger.error(f"❌ Error saving report formulas for '{report_name}': {e}")
            raise ValueError(f"Failed to save report formulas: {str(e)}")
    
    def delete_collection(self, collection_name: str) -> Dict[str, Any]:
        """
        Delete a MongoDB collection
        
        Args:
            collection_name: Name of the collection to delete (will be converted to lowercase)
        
        Returns:
            Dictionary with status and message
        
        Raises:
            ConnectionError: If MongoDB is not connected
            ValueError: If collection doesn't exist
        """
        if not self.is_connected():
            raise ConnectionError("MongoDB is not connected")
        
        if not collection_name or not collection_name.strip():
            raise ValueError("Collection name is required and cannot be empty")
        
        try:
            collection_name_lower = collection_name.lower().strip()
            
            # Check if collection exists
            if not self.collection_exists(collection_name_lower):
                raise ValueError(f"Collection '{collection_name_lower}' does not exist")
            
            # Drop the collection
            self.db[collection_name_lower].drop()
            logger.info(f"🗑️ Deleted collection '{collection_name_lower}'")
            
            return {
                "status": "success",
                "message": f"Collection '{collection_name_lower}' deleted successfully",
                "collection_name": collection_name_lower
            }
            
        except ValueError:
            raise
        except ConnectionError:
            raise
        except Exception as e:
            logger.error(f"❌ Error deleting collection '{collection_name}': {e}")
            raise ValueError(f"Failed to delete collection: {str(e)}")
    
    def get_report_formulas(self, report_name: str) -> Optional[Dict[str, Any]]:
        """
        Get report formulas from the 'formulas' collection
        
        Args:
            report_name: Name of the report (will be converted to lowercase)
        
        Returns:
            Dictionary with report data or None if not found
        
        Raises:
            ConnectionError: If MongoDB is not connected
            ValueError: If collection doesn't exist
        """
        if not self.is_connected():
            raise ConnectionError("MongoDB is not connected")
        
        if not report_name or not report_name.strip():
            raise ValueError("Report name is required and cannot be empty")
        
        try:
            report_name_lower = report_name.lower().strip()
            
            # Check if formulas collection exists
            if not self.collection_exists("formulas"):
                raise ValueError(f"Formulas collection does not exist")
            
            # Get the document from formulas collection
            collection = self.db["formulas"]
            document = collection.find_one({"report_name": report_name_lower})
            
            if not document:
                logger.warning(f"⚠️ Report document not found for '{report_name_lower}' in 'formulas' collection")
                return None
            
            # Convert ObjectId to string and datetime to ISO format
            if "_id" in document:
                document["_id"] = str(document["_id"])
            if "created_at" in document:
                document["created_at"] = document["created_at"].isoformat()
            if "updated_at" in document:
                document["updated_at"] = document["updated_at"].isoformat()
            
            logger.info(f"✅ Retrieved report formulas for '{report_name_lower}' from 'formulas' collection")
            return document
            
        except ValueError:
            raise
        except ConnectionError:
            raise
        except Exception as e:
            logger.error(f"❌ Error getting report formulas for '{report_name}': {e}")
            raise ValueError(f"Failed to get report formulas: {str(e)}")
    
    def update_report_formulas(
        self,
        report_name: str,
        formulas: List[Dict[str, str]]
    ) -> Dict[str, Any]:
        """
        Update report formulas in the 'formulas' collection.
        Document must exist.
        
        Args:
            report_name: Name of the report (will be converted to lowercase)
            formulas: List of formula dictionaries with 'formula_name' and 'formula_value'
        
        Returns:
            Dictionary with status and message
        
        Raises:
            ConnectionError: If MongoDB is not connected
            ValueError: If collection or document doesn't exist or validation fails
        """
        if not self.is_connected():
            raise ConnectionError("MongoDB is not connected")
        
        if not report_name or not report_name.strip():
            raise ValueError("Report name is required and cannot be empty")
        
        # Allow empty formulas array - no validation needed
        
        try:
            report_name_lower = report_name.lower().strip()
            
            # Check if formulas collection exists
            if not self.collection_exists("formulas"):
                raise ValueError(f"Formulas collection does not exist")
            
            collection = self.db["formulas"]
            
            # Check if document exists
            existing_doc = collection.find_one({"report_name": report_name_lower})
            if not existing_doc:
                raise ValueError(f"Report document not found for '{report_name_lower}' in 'formulas' collection")
            
            # Update the document with new formulas
            collection.update_one(
                {"report_name": report_name_lower},
                {"$set": {
                    "formulas": formulas,
                    "formulas_count": len(formulas),
                    "updated_at": datetime.utcnow()
                }}
            )
            
            logger.info(f"🔄 Updated report formulas for '{report_name_lower}' in 'formulas' collection")
            
            return {
                "status": "success",
                "message": f"Report formulas updated successfully for '{report_name_lower}' in 'formulas' collection",
                "report_name": report_name_lower,
                "formulas_count": len(formulas)
            }
            
        except ValueError:
            raise
        except ConnectionError:
            raise
        except Exception as e:
            logger.error(f"❌ Error updating report formulas for '{report_name}': {e}")
            raise ValueError(f"Failed to update report formulas: {str(e)}")
    
    def delete_report_formulas(self, report_name: str) -> Dict[str, Any]:
        """
        Delete a report document from the 'formulas' collection
        
        Args:
            report_name: Name of the report to delete (will be converted to lowercase)
        
        Returns:
            Dictionary with status and message
        
        Raises:
            ConnectionError: If MongoDB is not connected
            ValueError: If collection or document doesn't exist
        """
        if not self.is_connected():
            raise ConnectionError("MongoDB is not connected")
        
        if not report_name or not report_name.strip():
            raise ValueError("Report name is required and cannot be empty")
        
        try:
            report_name_lower = report_name.lower().strip()
            
            # Check if formulas collection exists
            if not self.collection_exists("formulas"):
                raise ValueError(f"Formulas collection does not exist")
            
            collection = self.db["formulas"]
            
            # Check if document exists
            existing_doc = collection.find_one({"report_name": report_name_lower})
            if not existing_doc:
                raise ValueError(f"Report document not found for '{report_name_lower}' in 'formulas' collection")
            
            # Delete the document
            collection.delete_one({"report_name": report_name_lower})
            logger.info(f"🗑️ Deleted report formulas for '{report_name_lower}' from 'formulas' collection")
            
            return {
                "status": "success",
                "message": f"Report formulas for '{report_name_lower}' deleted successfully from 'formulas' collection",
                "collection_name": "formulas",
                "report_name": report_name_lower
            }
            
        except ValueError:
            raise
        except ConnectionError:
            raise
        except Exception as e:
            logger.error(f"❌ Error deleting report formulas for '{report_name}': {e}")
            raise ValueError(f"Failed to delete report formulas: {str(e)}")
    
    def get_all_formulas(self) -> List[Dict[str, Any]]:
        """
        Get all report formulas from the 'formulas' collection
        
        Returns:
            List of dictionaries with report formula data
        
        Raises:
            ConnectionError: If MongoDB is not connected
            ValueError: If collection doesn't exist
        """
        if not self.is_connected():
            raise ConnectionError("MongoDB is not connected")
        
        try:
            # Check if formulas collection exists
            if not self.collection_exists("formulas"):
                logger.warning("⚠️ Formulas collection does not exist")
                return []
            
            collection = self.db["formulas"]
            
            # Get all documents from the collection
            documents = list(collection.find({}).sort("report_name", 1))  # Sort by report_name
            
            # Convert ObjectId to string and datetime to ISO format for each document
            for doc in documents:
                if "_id" in doc:
                    doc["_id"] = str(doc["_id"])
                if "created_at" in doc:
                    doc["created_at"] = doc["created_at"].isoformat()
                if "updated_at" in doc:
                    doc["updated_at"] = doc["updated_at"].isoformat()
            
            logger.info(f"✅ Retrieved {len(documents)} report formula(s) from 'formulas' collection")
            return documents
            
        except ConnectionError:
            raise
        except Exception as e:
            logger.error(f"❌ Error getting all formulas: {e}")
            raise ValueError(f"Failed to get all formulas: {str(e)}")
    
    def close(self):
        """Close MongoDB connection"""
        if self.client is not None:
            self.client.close()
            logger.info("🔌 MongoDB connection closed")


# Global MongoDB service instance
mongodb_service = MongoDBService()

