"""
Database Setup Controller - Handles MongoDB collection setup operations
"""

from fastapi import HTTPException
from typing import Dict, Any, List
import logging

from app.services.mongodb_service import mongodb_service

logger = logging.getLogger(__name__)


class DBSetupController:
    """Controller for database setup operations"""
    
    def __init__(self):
        pass
    
    async def create_collection(
        self,
        collection_name: str,
        unique_ids: List[str] = None
    ) -> Dict[str, Any]:
        """
        Create a new MongoDB collection and its processed version
        
        Args:
            collection_name: Name of the collection to create (will be converted to lowercase)
            unique_ids: List of field names that form unique identifiers (can be empty)
        
        Returns:
            Dictionary with status and details
        
        Raises:
            HTTPException: If collection already exists or MongoDB is not connected
        """
        if not collection_name or not collection_name.strip():
            raise HTTPException(
                status_code=400,
                detail="Collection name is required and cannot be empty"
            )
        
        if unique_ids is None:
            unique_ids = []
        
        try:
            result = mongodb_service.create_collection(
                collection_name.strip(),
                unique_ids
            )
            
            return {
                "status": 200,
                "message": result["message"],
                "data": {
                    "collection_name": result["collection_name"],
                    "processed_collection_name": result["processed_collection_name"],
                    "unique_ids": result["unique_ids"],
                    "mongodb_connected": mongodb_service.is_connected()
                }
            }
        
        except ValueError as e:
            # This shouldn't happen now since we skip instead of raising ValueError
            # But keep it for backward compatibility
            raise HTTPException(
                status_code=409,
                detail=str(e)
            )
        
        except ConnectionError as e:
            # MongoDB not connected
            raise HTTPException(
                status_code=503,
                detail=f"MongoDB connection error: {str(e)}"
            )
        
        except Exception as e:
            logger.error(f"❌ Error creating collection '{collection_name}': {e}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to create collection: {str(e)}"
            )
    
    async def list_all_collections(self) -> Dict[str, Any]:
        """
        Get all MongoDB collection names
        
        Returns:
            Dictionary with status and list of collection names
        
        Raises:
            HTTPException: If MongoDB is not connected
        """
        try:
            collections = mongodb_service.list_all_collections()
            
            return {
                "status": 200,
                "message": f"Found {len(collections)} collection(s)",
                "data": {
                    "collections": collections,
                    "count": len(collections),
                    "mongodb_connected": mongodb_service.is_connected()
                }
            }
        
        except Exception as e:
            logger.error(f"❌ Error listing collections: {e}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to list collections: {str(e)}"
            )
    
    async def get_collection_keys(self, collection_name: str) -> Dict[str, Any]:
        """
        Get unique keys from a MongoDB collection
        
        Args:
            collection_name: Name of the collection to get keys from
        
        Returns:
            Dictionary with status and list of unique keys
        
        Raises:
            HTTPException: If collection doesn't exist or MongoDB is not connected
        """
        if not collection_name or not collection_name.strip():
            raise HTTPException(
                status_code=400,
                detail="Collection name is required and cannot be empty"
            )
        
        try:
            keys = mongodb_service.get_collection_keys(collection_name.strip())
            
            return {
                "status": 200,
                "message": f"Found {len(keys)} unique key(s) in collection '{collection_name.lower()}'",
                "data": {
                    "collection_name": collection_name.lower(),
                    "keys": keys,
                    "count": len(keys),
                    "mongodb_connected": mongodb_service.is_connected()
                }
            }
        
        except ValueError as e:
            # Collection doesn't exist or other value error
            error_msg = str(e)
            if "does not exist" in error_msg:
                raise HTTPException(
                    status_code=404,
                    detail=error_msg
                )
            else:
                raise HTTPException(
                    status_code=400,
                    detail=error_msg
                )
        
        except ConnectionError as e:
            # MongoDB not connected
            raise HTTPException(
                status_code=503,
                detail=f"MongoDB connection error: {str(e)}"
            )
        
        except Exception as e:
            logger.error(f"❌ Error getting keys from collection '{collection_name}': {e}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to get collection keys: {str(e)}"
            )
    
    async def save_collection_field_mapping(
        self,
        collection_name: str,
        selected_fields: List[str]
    ) -> Dict[str, Any]:
        """
        Save or update field mapping for a collection
        
        Args:
            collection_name: Name of the collection
            selected_fields: List of field names to use for this collection
        
        Returns:
            Dictionary with status and details
        
        Raises:
            HTTPException: If collection doesn't exist or fields are invalid
        """
        if not collection_name or not collection_name.strip():
            raise HTTPException(
                status_code=400,
                detail="Collection name is required and cannot be empty"
            )
        
        if not selected_fields or len(selected_fields) == 0:
            raise HTTPException(
                status_code=400,
                detail="At least one field must be selected"
            )
        
        try:
            result = mongodb_service.save_collection_field_mapping(
                collection_name.strip(),
                [field.strip() for field in selected_fields if field.strip()]
            )
            
            return {
                "status": 200,
                "message": result["message"],
                "data": {
                    "collection_name": result["collection_name"],
                    "selected_fields": selected_fields,
                    "selected_fields_count": result["selected_fields_count"],
                    "total_available_fields": result["total_available_fields"],
                    "mongodb_connected": mongodb_service.is_connected()
                }
            }
        
        except ValueError as e:
            error_msg = str(e)
            if "does not exist" in error_msg:
                raise HTTPException(
                    status_code=404,
                    detail=error_msg
                )
            else:
                raise HTTPException(
                    status_code=400,
                    detail=error_msg
                )
        
        except ConnectionError as e:
            raise HTTPException(
                status_code=503,
                detail=f"MongoDB connection error: {str(e)}"
            )
        
        except Exception as e:
            logger.error(f"❌ Error saving field mapping for collection '{collection_name}': {e}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to save field mapping: {str(e)}"
            )
    
    async def get_collection_field_mapping(self, collection_name: str) -> Dict[str, Any]:
        """
        Get field mapping for a collection
        
        Args:
            collection_name: Name of the collection
        
        Returns:
            Dictionary with mapping data
        
        Raises:
            HTTPException: If mapping not found or MongoDB is not connected
        """
        if not collection_name or not collection_name.strip():
            raise HTTPException(
                status_code=400,
                detail="Collection name is required and cannot be empty"
            )
        
        try:
            mapping = mongodb_service.get_collection_field_mapping(collection_name.strip())
            
            if not mapping:
                # Return 200 with empty/default data instead of 404
                return {
                    "status": 200,
                    "message": f"No field mapping found for collection '{collection_name.lower().strip()}'",
                    "data": {
                        "collection_name": collection_name.lower().strip(),
                        "selected_fields": [],
                        "selected_fields_count": 0,
                        "total_available_fields": 0
                    }
                }
            
            return {
                "status": 200,
                "message": f"Field mapping found for collection '{mapping['collection_name']}'",
                "data": mapping
            }
        
        except HTTPException:
            raise
        except ConnectionError as e:
            raise HTTPException(
                status_code=503,
                detail=f"MongoDB connection error: {str(e)}"
            )
        except Exception as e:
            logger.error(f"❌ Error getting field mapping for collection '{collection_name}': {e}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to get field mapping: {str(e)}"
            )
    
    async def list_all_field_mappings(self) -> Dict[str, Any]:
        """
        List all field mappings
        
        Returns:
            Dictionary with status and list of mappings
        """
        try:
            mappings = mongodb_service.list_all_field_mappings()
            
            return {
                "status": 200,
                "message": f"Found {len(mappings)} field mapping(s)",
                "data": {
                    "mappings": mappings,
                    "count": len(mappings),
                    "mongodb_connected": mongodb_service.is_connected()
                }
            }
        
        except Exception as e:
            logger.error(f"❌ Error listing field mappings: {e}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to list field mappings: {str(e)}"
            )
    
    async def update_collection_unique_ids(
        self,
        collection_name: str,
        unique_ids: List[str]
    ) -> Dict[str, Any]:
        """
        Update unique_ids for an existing collection
        
        Args:
            collection_name: Name of the collection
            unique_ids: List of field names that form unique identifiers
        
        Returns:
            Dictionary with status and details
        
        Raises:
            HTTPException: If collection doesn't exist or MongoDB is not connected
        """
        if not collection_name or not collection_name.strip():
            raise HTTPException(
                status_code=400,
                detail="Collection name is required and cannot be empty"
            )
        
        if unique_ids is None:
            unique_ids = []
        
        try:
            result = mongodb_service.update_collection_unique_ids(
                collection_name.strip(),
                unique_ids
            )
            
            return {
                "status": 200,
                "message": result["message"],
                "data": {
                    "collection_name": result["collection_name"],
                    "unique_ids": result["unique_ids"],
                    "mongodb_connected": mongodb_service.is_connected()
                }
            }
        
        except ValueError as e:
            error_msg = str(e)
            if "not found" in error_msg.lower():
                raise HTTPException(
                    status_code=404,
                    detail=error_msg
                )
            else:
                raise HTTPException(
                    status_code=400,
                    detail=error_msg
                )
        
        except ConnectionError as e:
            raise HTTPException(
                status_code=503,
                detail=f"MongoDB connection error: {str(e)}"
            )
        
        except Exception as e:
            logger.error(f"❌ Error updating unique_ids for collection '{collection_name}': {e}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to update unique_ids: {str(e)}"
            )
    
    async def get_collection_unique_ids(self, collection_name: str) -> Dict[str, Any]:
        """
        Get unique_ids for a collection
        
        Args:
            collection_name: Name of the collection
        
        Returns:
            Dictionary with status and unique_ids data
        
        Raises:
            HTTPException: If collection doesn't exist or MongoDB is not connected
        """
        if not collection_name or not collection_name.strip():
            raise HTTPException(
                status_code=400,
                detail="Collection name is required and cannot be empty"
            )
        
        try:
            result = mongodb_service.get_collection_unique_ids(collection_name.strip())
            
            if not result:
                raise HTTPException(
                    status_code=404,
                    detail=f"Collection '{collection_name.lower().strip()}' not found in raw_data_collection"
                )
            
            return {
                "status": 200,
                "message": f"Unique IDs retrieved successfully for collection '{result['collection_name']}'",
                "data": {
                    "collection_name": result["collection_name"],
                    "unique_ids": result["unique_ids"],
                    "unique_ids_count": result["unique_ids_count"],
                    "mongodb_connected": mongodb_service.is_connected()
                }
            }
        
        except HTTPException:
            raise
        except ConnectionError as e:
            raise HTTPException(
                status_code=503,
                detail=f"MongoDB connection error: {str(e)}"
            )
        except Exception as e:
            logger.error(f"❌ Error getting unique_ids for collection '{collection_name}': {e}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to get unique_ids: {str(e)}"
            )
    
    async def list_all_uploaded_files(self) -> Dict[str, Any]:
        """
        Get all data from uploaded_files collection
        
        Returns:
            Dictionary with status and list of all uploaded files
        
        Raises:
            HTTPException: If MongoDB is not connected
        """
        try:
            # Get all uploaded files (no limit, get all records)
            uploaded_files = mongodb_service.list_uploads(limit=None)  # limit=None means no limit
            
            return {
                "status": 200,
                "message": f"Found {len(uploaded_files)} uploaded file(s)",
                "data": {
                    "uploaded_files": uploaded_files,
                    "count": len(uploaded_files),
                    "mongodb_connected": mongodb_service.is_connected()
                }
            }
        
        except ConnectionError as e:
            raise HTTPException(
                status_code=503,
                detail=f"MongoDB connection error: {str(e)}"
            )
        except Exception as e:
            logger.error(f"❌ Error listing uploaded files: {e}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to list uploaded files: {str(e)}"
            )