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
                raise HTTPException(
                    status_code=404,
                    detail=f"Field mapping not found for collection '{collection_name.lower()}'"
                )
            
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
    
    async def save_report_formulas(
        self,
        report_name: str,
        formulas: List[Dict[str, str]]
    ) -> Dict[str, Any]:
        """
        Save report formulas to a MongoDB collection.
        If the collection doesn't exist, it will be created.
        
        Args:
            report_name: Name of the report (will be used as collection name)
            formulas: List of formula dictionaries with 'formula_name' and 'formula_value'
        
        Returns:
            Dictionary with status and details
        
        Raises:
            HTTPException: If validation fails or MongoDB is not connected
        """
        if not report_name or not report_name.strip():
            raise HTTPException(
                status_code=400,
                detail="Report name is required and cannot be empty"
            )
        
        if not formulas or len(formulas) == 0:
            raise HTTPException(
                status_code=400,
                detail="At least one formula is required"
            )
        
        # Validate formula structure
        for formula in formulas:
            if not isinstance(formula, dict):
                raise HTTPException(
                    status_code=400,
                    detail="Each formula must be a dictionary with 'formula_name' and 'formula_value'"
                )
            if "formula_name" not in formula or "formula_value" not in formula:
                raise HTTPException(
                    status_code=400,
                    detail="Each formula must have 'formula_name' and 'formula_value' fields"
                )
            if not formula.get("formula_name") or not formula.get("formula_value"):
                raise HTTPException(
                    status_code=400,
                    detail="Formula name and value cannot be empty"
                )
        
        try:
            result = mongodb_service.save_report_formulas(
                report_name.strip(),
                formulas
            )
            
            return {
                "status": 200,
                "message": result["message"],
                "data": {
                    "report_name": result["report_name"],
                    "formulas_count": result["formulas_count"],
                    "formulas": formulas,
                    "collection_existed": result["collection_existed"],
                    "mongodb_connected": mongodb_service.is_connected()
                }
            }
        
        except ValueError as e:
            raise HTTPException(
                status_code=400,
                detail=str(e)
            )
        
        except ConnectionError as e:
            raise HTTPException(
                status_code=503,
                detail=f"MongoDB connection error: {str(e)}"
            )
        
        except Exception as e:
            logger.error(f"❌ Error saving report formulas for '{report_name}': {e}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to save report formulas: {str(e)}"
            )
    
    async def delete_report_collection(self, report_name: str) -> Dict[str, Any]:
        """
        Delete a report collection from MongoDB
        
        Args:
            report_name: Name of the report collection to delete
        
        Returns:
            Dictionary with status and details
        
        Raises:
            HTTPException: If collection doesn't exist or MongoDB is not connected
        """
        if not report_name or not report_name.strip():
            raise HTTPException(
                status_code=400,
                detail="Report name is required and cannot be empty"
            )
        
        try:
            result = mongodb_service.delete_collection(report_name.strip())
            
            return {
                "status": 200,
                "message": result["message"],
                "data": {
                    "report_name": result["collection_name"],
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
            logger.error(f"❌ Error deleting report collection '{report_name}': {e}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to delete report collection: {str(e)}"
            )
    
    async def get_report_formulas(self, report_name: str) -> Dict[str, Any]:
        """
        Get report formulas from a MongoDB collection
        
        Args:
            report_name: Name of the report collection
        
        Returns:
            Dictionary with status and report data
        
        Raises:
            HTTPException: If collection doesn't exist or MongoDB is not connected
        """
        if not report_name or not report_name.strip():
            raise HTTPException(
                status_code=400,
                detail="Report name is required and cannot be empty"
            )
        
        try:
            document = mongodb_service.get_report_formulas(report_name.strip())
            
            if not document:
                raise HTTPException(
                    status_code=404,
                    detail=f"Report document not found in collection '{report_name.lower().strip()}'"
                )
            
            return {
                "status": 200,
                "message": "Report formulas retrieved successfully",
                "data": {
                    "report_name": document.get("report_name"),
                    "formulas": document.get("formulas", []),
                    "formulas_count": document.get("formulas_count", len(document.get("formulas", []))),
                    "created_at": document.get("created_at"),
                    "updated_at": document.get("updated_at"),
                    "mongodb_connected": mongodb_service.is_connected()
                }
            }
        
        except ValueError as e:
            error_msg = str(e)
            if "does not exist" in error_msg or "not found" in error_msg:
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
            logger.error(f"❌ Error getting report formulas for '{report_name}': {e}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to get report formulas: {str(e)}"
            )
    
    async def update_report_formulas(
        self,
        report_name: str,
        formulas: List[Dict[str, str]]
    ) -> Dict[str, Any]:
        """
        Update report formulas in an existing MongoDB collection.
        Collection must exist.
        
        Args:
            report_name: Name of the report collection
            formulas: List of formula dictionaries with 'formula_name' and 'formula_value'
        
        Returns:
            Dictionary with status and details
        
        Raises:
            HTTPException: If validation fails, collection doesn't exist, or MongoDB is not connected
        """
        if not report_name or not report_name.strip():
            raise HTTPException(
                status_code=400,
                detail="Report name is required and cannot be empty"
            )
        
        if not formulas or len(formulas) == 0:
            raise HTTPException(
                status_code=400,
                detail="At least one formula is required"
            )
        
        # Validate formula structure
        for formula in formulas:
            if not isinstance(formula, dict):
                raise HTTPException(
                    status_code=400,
                    detail="Each formula must be a dictionary with 'formula_name' and 'formula_value'"
                )
            if "formula_name" not in formula or "formula_value" not in formula:
                raise HTTPException(
                    status_code=400,
                    detail="Each formula must have 'formula_name' and 'formula_value' fields"
                )
            if not formula.get("formula_name") or not formula.get("formula_value"):
                raise HTTPException(
                    status_code=400,
                    detail="Formula name and value cannot be empty"
                )
        
        try:
            result = mongodb_service.update_report_formulas(
                report_name.strip(),
                formulas
            )
            
            return {
                "status": 200,
                "message": result["message"],
                "data": {
                    "report_name": result["report_name"],
                    "formulas_count": result["formulas_count"],
                    "formulas": formulas,
                    "mongodb_connected": mongodb_service.is_connected()
                }
            }
        
        except ValueError as e:
            error_msg = str(e)
            if "does not exist" in error_msg or "not found" in error_msg:
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
            logger.error(f"❌ Error updating report formulas for '{report_name}': {e}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to update report formulas: {str(e)}"
            )

    