"""
Upload Controller - Handles upload-related business logic
Similar to Node.js controllers: handles request/response, calls services
"""

from fastapi import UploadFile, HTTPException
from typing import Dict, Any, Optional, List, Tuple
import logging
import json
from app.services.file_utils import extract_headers, extract_sample_data, get_file_info
from app.services.db_service import get_table_columns
from app.services.mapping_service import suggest_mapping
from app.services.upload_service import uploader
from app.services.validation_service import validator
from app.services.config_manager import ConfigurationManager

logger = logging.getLogger(__name__)


class UploadController:
    """Controller for upload operations - handles business logic"""
    
    def __init__(self):
        self.config_manager = ConfigurationManager()
    
    async def map_columns(
        self,
        file: UploadFile,
        table_name: str,
        include_sample_data: bool = False
    ) -> Dict[str, Any]:
        """
        Map file columns to database columns using AI.
        Returns mapping suggestions.
        """
        try:
            # Get file information
            file_info = get_file_info(file)
            logger.info(f"Processing file: {file_info}")
            
            # Extract headers
            headers = await extract_headers(file)
            # Reset file pointer so the file can be read again
            await self._reset_file_pointer(file)
            
            # Get database schema
            db_columns = get_table_columns(table_name)
            
            # Extract sample data if requested
            sample_data = None
            if include_sample_data:
                sample_result = await extract_sample_data(file)
                sample_data = sample_result.get("sample_data", [])
                await self._reset_file_pointer(file)
            
            # Get AI mapping with enhanced intelligence
            mapping_result = await suggest_mapping(headers, db_columns, sample_data)

            return {
                "table_name": table_name,
                "file_info": file_info,
                "file_headers": headers,
                "db_columns": db_columns,
                "ai_mapping_result": mapping_result,
                "sample_data_included": include_sample_data
            }
            
        except HTTPException as e:
            raise e
        except Exception as e:
            logger.error(f"Error in map_columns: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
    
    async def upload_data(
        self,
        file: UploadFile,
        table_name: str,
        mapping: str,  # JSON string
        validate_before_upload: bool = True
    ) -> Dict[str, Any]:
        """
        Upload and process data with column mapping.
        """
        try:
            # Parse mapping JSON
            try:
                column_mapping = json.loads(mapping)
            except json.JSONDecodeError:
                raise HTTPException(status_code=400, detail="Invalid mapping JSON format")
            
            # Get file information
            file_info = get_file_info(file)
            logger.info(f"Uploading file: {file_info}")
            
            # Ensure file pointer is at start before processing
            await self._reset_file_pointer(file)
            
            # Process upload with validation (with MongoDB integration)
            upload_result = await uploader.process_upload(
                file, table_name, column_mapping, 
                {"validate_before_upload": validate_before_upload},
                uploaded_by="api_user"
            )
            
            return {
                "success": upload_result.get("success", False),
                "file_info": file_info,
                "table_name": table_name,
                "upload_results": upload_result.get("upload_results", {}),
                "validation_results": upload_result.get("validation_results", {}),
                "summary": upload_result.get("summary", {}),
                "error": upload_result.get("error")
            }
            
        except HTTPException as e:
            raise e
        except Exception as e:
            logger.error(f"Error in upload_data: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
    
    async def validate_mapping(
        self,
        file: UploadFile,
        table_name: str,
        mapping: str
    ) -> Dict[str, Any]:
        """
        Validate column mapping and data quality before upload.
        """
        try:
            # Parse mapping JSON
            try:
                column_mapping = json.loads(mapping)
            except json.JSONDecodeError:
                raise HTTPException(status_code=400, detail="Invalid mapping JSON format")
            
            # Extract file data
            file_data = await uploader._extract_file_data(file)
            
            # Get table schema
            db_columns = await uploader._get_table_schema(table_name)
            
            # Validate data
            validation_results = await validator.validate_mapped_data(
                file_data, column_mapping, db_columns
            )
            
            return {
                "table_name": table_name,
                "validation_results": validation_results,
                "file_info": get_file_info(file),
                "mapping": column_mapping
            }
            
        except HTTPException as e:
            raise e
        except Exception as e:
            logger.error(f"Error in validate_mapping: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
    
    async def upload_by_datasource(
        self,
        file: UploadFile,
        datasource: str,
        chunk_size: Optional[int] = None,
        client: Optional[str] = None,
        validate_before_upload: bool = True
    ) -> Dict[str, Any]:
        """
        Upload file based on datasource configuration.
        Main business logic for datasource-based uploads.
        """
        try:
            # Log additional parameters for tracking
            if chunk_size:
                logger.info(f"Chunk size parameter received: {chunk_size} (not used in processing)")
            if client:
                logger.info(f"Client parameter received: {client}")
            
            # Normalize datasource name (case-insensitive)
            datasource_upper = datasource.upper()
            
            # Find datasource config
            config = self._get_datasource_config(datasource, datasource_upper)
            
            # Get table name from config
            table_name = self._get_table_name_from_config(config, datasource_upper)
            
            if not table_name:
                raise HTTPException(
                    status_code=400,
                    detail=f"Could not determine target table for datasource '{datasource}'. Please ensure the datasource configuration has a 'load_config.target_table' setting."
                )
            
            # Process file
            result = await self._process_file_for_upload(
                file, datasource, table_name, config, validate_before_upload, client
            )
            
            return result
            
        except HTTPException as e:
            raise e
        except Exception as e:
            logger.error(f"Error in upload_by_datasource: {str(e)}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
    
    def _get_datasource_config(self, datasource: str, datasource_upper: str):
        """Get datasource configuration"""
        # First, try exact match on source_id
        config = self.config_manager.get_data_source_config(datasource.lower())
        
        # If not found, try to find by source_name (case-insensitive)
        if not config:
            all_configs = self.config_manager.list_data_sources()
            for cfg in all_configs:
                if cfg.source_id.upper() == datasource_upper or cfg.source_name.upper() == datasource_upper:
                    config = cfg
                    break
        
        return config
    
    def _get_table_name_from_config(self, config, datasource_upper: str) -> Optional[str]:
        """Extract table name from config or use fallback mappings"""
        # Common mappings for datasource to table name (fallback)
        common_mappings = {
            'ZOMATO': 'zomato',
            'TRM': 'trm',
            'MPR': 'mpr_hdfc_upi',
            'MPR_UPI': 'mpr_hdfc_upi',
            'MPR_CARD': 'mpr_hdfc_card',
            'BANK': 'fin_bank_statements',
            'ORDERS': 'orders'
        }
        
        table_name = None
        if config:
            if config.load_config and config.load_config.get('target_table'):
                table_name = config.load_config.get('target_table')
            elif config.entity_type:
                # Fallback: try to derive from entity_type or source_id
                entity_to_table = {
                    'payment': 'fin_payments',
                    'order': 'orders',
                    'transaction': 'trm'
                }
                table_name = entity_to_table.get(config.entity_type.lower())
            
            if not table_name:
                # Last resort: use source_id as table name (if it matches a known table)
                table_name = config.source_id.replace('platform_', '').replace('_', '')
        else:
            # If no config found, try common mappings
            if datasource_upper in common_mappings:
                table_name = common_mappings[datasource_upper]
            else:
                raise HTTPException(
                    status_code=404, 
                    detail=f"Datasource '{datasource_upper}' not found. Available datasources can be listed via /api/financial/config/datasources"
                )
        
        return table_name
    
    async def _process_file_for_upload(
        self,
        file: UploadFile,
        datasource: str,
        table_name: str,
        config,
        validate_before_upload: bool,
        client: Optional[str]
    ) -> Dict[str, Any]:
        """Process a single file for upload"""
        try:
            file_info = get_file_info(file)
            logger.info(f"Processing file {file.filename} for datasource {datasource} -> table {table_name}")
            
            # Reset file pointer
            await self._reset_file_pointer(file)
            
            # Extract headers
            headers = await extract_headers(file)
            await self._reset_file_pointer(file)
            
            # Get column mappings
            column_mapping, forced_mappings_count = await self._get_column_mapping(
                file, headers, table_name, config
            )
            
            # Reset file pointer for upload
            await self._reset_file_pointer(file)
            
            # Process upload (with MongoDB integration)
            upload_result = await uploader.process_upload(
                file, table_name, column_mapping,
                {"validate_before_upload": validate_before_upload},
                datasource=datasource,
                uploaded_by=client or "api_user"
            )
            
            return {
                "success": upload_result.get("success", False),
                "datasource": datasource,
                "table_name": table_name,
                "files_processed": 1,
                "files_successful": 1 if upload_result.get("success") else 0,
                "files_failed": 0 if upload_result.get("success") else 1,
                "results": [{
                    "filename": file.filename,
                    "file_info": file_info,
                    "success": upload_result.get("success", False),
                    "table_name": table_name,
                    "datasource": datasource,
                    "upload_id": upload_result.get("upload_id"),
                    "upload_results": upload_result.get("upload_results", {}),
                    "validation_results": upload_result.get("validation_results", {}),
                    "summary": upload_result.get("summary", {}),
                    "mapping_used": {
                        "forced_mappings_count": forced_mappings_count,
                        "total_mappings": len(column_mapping)
                    },
                    "error": upload_result.get("error")
                }],
                "summary": {
                    "total_files": 1,
                    "successful_uploads": 1 if upload_result.get("success") else 0,
                    "failed_uploads": 0 if upload_result.get("success") else 1
                }
            }
            
        except Exception as file_error:
            logger.error(f"Error processing file {file.filename}: {str(file_error)}")
            return {
                "success": False,
                "datasource": datasource,
                "table_name": table_name,
                "files_processed": 1,
                "files_successful": 0,
                "files_failed": 1,
                "results": [{
                    "filename": file.filename,
                    "success": False,
                    "error": str(file_error)
                }],
                "summary": {
                    "total_files": 1,
                    "successful_uploads": 0,
                    "failed_uploads": 1
                }
            }
    
    async def _get_column_mapping(
        self,
        file: UploadFile,
        headers: List[str],
        table_name: str,
        config
    ) -> Tuple[Dict[str, str], int]:
        """Get column mapping (forced + AI/fallback)"""
        column_mapping = {}
        forced_mappings_count = 0
        
        # First, try to get forced mappings from config
        if config and config.ai_mapping_config:
            forced_mappings = config.ai_mapping_config.get('forced_mappings', {})
            if forced_mappings:
                # Use forced mappings, but only for headers that exist in the file
                for file_header, db_column in forced_mappings.items():
                    if file_header in headers:
                        column_mapping[file_header] = db_column
                forced_mappings_count = len(forced_mappings)
                logger.info(f"Using {len(column_mapping)} forced mappings from config")
        
        # If no forced mappings or incomplete, use AI mapping or fallback
        if not column_mapping or len(column_mapping) < len(headers) * 0.5:
            db_columns = get_table_columns(table_name)
            
            # Extract sample data for better AI mapping
            sample_result = await extract_sample_data(file)
            sample_data = sample_result.get("sample_data", [])
            await self._reset_file_pointer(file)
            
            # Get AI mapping
            ai_mapping_result = await suggest_mapping(headers, db_columns, sample_data)
            
            # Extract mappings from the result structure
            ai_mappings = {}
            if ai_mapping_result.get('mapping') and not ai_mapping_result.get('mapping', {}).get('error'):
                # AI succeeded - extract from mapping.mappings
                ai_mappings = ai_mapping_result.get('mapping', {}).get('mappings', {})
                logger.info(f"Using AI-generated mappings: {len(ai_mappings)} mappings")
            elif ai_mapping_result.get('fallback_mapping'):
                # AI failed - use fallback mapping
                ai_mappings = ai_mapping_result.get('fallback_mapping', {})
                logger.info(f"Using fallback mappings (AI failed): {len(ai_mappings)} mappings")
            
            # Merge forced mappings with AI mappings (forced take precedence)
            for file_header, db_column in ai_mappings.items():
                if db_column and file_header not in column_mapping:
                    column_mapping[file_header] = db_column
            
            logger.info(f"Generated {len(column_mapping)} total mappings (forced + AI/fallback) out of {len(headers)} headers")
        
        return column_mapping, forced_mappings_count
    
    async def _reset_file_pointer(self, file: UploadFile):
        """Helper to reset file pointer"""
        try:
            await file.seek(0)
        except Exception:
            try:
                file.file.seek(0)
            except Exception:
                pass

