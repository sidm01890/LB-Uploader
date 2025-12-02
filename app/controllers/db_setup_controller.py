"""
Database Setup Controller - Handles MongoDB collection setup operations
"""

from fastapi import HTTPException
from typing import Dict, Any
import logging

from app.services.mongodb_service import mongodb_service

logger = logging.getLogger(__name__)


class DBSetupController:
    """Controller for database setup operations"""
    
    def __init__(self):
        pass
    
    async def create_collection(self, collection_name: str) -> Dict[str, Any]:
        """
        Create a new MongoDB collection
        
        Args:
            collection_name: Name of the collection to create (will be converted to lowercase)
        
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
        
        try:
            result = mongodb_service.create_collection(collection_name.strip())
            
            return {
                "status": 200,
                "message": result["message"],
                "data": {
                    "collection_name": result["collection_name"],
                    "mongodb_connected": mongodb_service.is_connected()
                }
            }
        
        except ValueError as e:
            # Collection already exists
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

