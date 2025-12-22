"""
Formulas Controller - Handles report formula operations
"""

from fastapi import HTTPException
from typing import Dict, Any, List
import logging

from app.services.mongodb_service import mongodb_service

logger = logging.getLogger(__name__)


class FormulasController:
    """Controller for report formula operations"""
    
    def __init__(self):
        pass
    
    async def save_report_formulas(
        self,
        report_name: str,
        formulas: List[Dict[str, Any]],
        mapping_keys: Dict[str, List[str]] = None,
        conditions: Dict[str, List[Dict[str, Any]]] = None
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
        
        if mapping_keys is None:
            mapping_keys = {}
        
        if conditions is None:
            conditions = {}
        
        # Allow empty formulas array - no validation needed
        
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
                formulas,
                mapping_keys,
                conditions
            )
            
            return {
                "status": 200,
                "message": result["message"],
                "data": {
                    "report_name": result["report_name"],
                    "formulas_count": result["formulas_count"],
                    "formulas": formulas,
                    "mapping_keys": result.get("mapping_keys", {}),
                    "conditions": result.get("conditions", {}),
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
        Delete a report document from the 'formulas' collection
        
        Args:
            report_name: Name of the report to delete
        
        Returns:
            Dictionary with status and details
        
        Raises:
            HTTPException: If document doesn't exist or MongoDB is not connected
        """
        if not report_name or not report_name.strip():
            raise HTTPException(
                status_code=400,
                detail="Report name is required and cannot be empty"
            )
        
        try:
            result = mongodb_service.delete_report_formulas(report_name.strip())
            
            return {
                "status": 200,
                "message": result["message"],
                "data": {
                    "report_name": result["report_name"],
                    "collection_name": result["collection_name"],
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
                    "mapping_keys": document.get("mapping_keys", {}),
                    "conditions": document.get("conditions", {}),
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
        formulas: List[Dict[str, Any]],
        mapping_keys: Dict[str, List[str]] = None,
        conditions: Dict[str, List[Dict[str, Any]]] = None
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
        
        if mapping_keys is None:
            mapping_keys = {}
        
        if conditions is None:
            conditions = {}
        
        # Allow empty formulas array - no validation needed
        
        # Validate formula structure
        for formula in formulas:
            if not isinstance(formula, dict):
                raise HTTPException(
                    status_code=400,
                    detail="Each formula must be a dictionary"
                )
            # Validate required fields for new structure
            if "logicName" not in formula:
                raise HTTPException(
                    status_code=400,
                    detail="Each formula must have 'logicName' field"
                )
            if "formulaText" not in formula:
                raise HTTPException(
                    status_code=400,
                    detail="Each formula must have 'formulaText' field"
                )
            if "fields" not in formula or not isinstance(formula.get("fields"), list):
                raise HTTPException(
                    status_code=400,
                    detail="Each formula must have 'fields' array"
                )
        
        try:
            result = mongodb_service.update_report_formulas(
                report_name.strip(),
                formulas,
                mapping_keys,
                conditions
            )
            
            return {
                "status": 200,
                "message": result["message"],
                "data": {
                    "report_name": result["report_name"],
                    "formulas_count": result["formulas_count"],
                    "formulas": formulas,
                    "mapping_keys": result.get("mapping_keys", {}),
                    "conditions": result.get("conditions", {}),
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
    
    async def get_all_formulas(self) -> Dict[str, Any]:
        """
        Get all report formulas from the 'formulas' collection
        
        Returns:
            Dictionary with status and list of all formulas
        
        Raises:
            HTTPException: If MongoDB is not connected
        """
        try:
            documents = mongodb_service.get_all_formulas()
            
            return {
                "status": 200,
                "message": f"Retrieved {len(documents)} report formula(s) successfully",
                "data": {
                    "formulas": documents,
                    "count": len(documents),
                    "mongodb_connected": mongodb_service.is_connected()
                }
            }
        
        except ConnectionError as e:
            raise HTTPException(
                status_code=503,
                detail=f"MongoDB connection error: {str(e)}"
            )
        
        except Exception as e:
            logger.error(f"❌ Error getting all formulas: {e}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to get all formulas: {str(e)}"
            )

    