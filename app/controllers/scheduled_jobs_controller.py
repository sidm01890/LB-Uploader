"""
Scheduled Jobs Controller - Handles scheduled data processing jobs
Processes data from collections based on field mappings and saves to processed collections
"""

from fastapi import HTTPException
from typing import Dict, Any, List, Optional
import logging
from datetime import datetime
import re

from app.services.mongodb_service import mongodb_service

logger = logging.getLogger(__name__)


class ScheduledJobsController:
    """Controller for scheduled data processing jobs"""
    
    def __init__(self):
        pass
    
    def _sanitize_date(self, value: Any) -> Optional[datetime]:
        """
        Convert various date formats to MongoDB datetime format
        
        Args:
            value: Date value in various formats (string, datetime, etc.)
        
        Returns:
            datetime object or None if conversion fails
        """
        if value is None:
            return None
        
        # If already a datetime object, return as is
        if isinstance(value, datetime):
            return value
        
        # If it's not a string, try to convert to string first
        if not isinstance(value, str):
            value = str(value)
        
        # Remove leading/trailing whitespace
        value = value.strip()
        
        if not value or value.lower() in ['none', 'null', 'nan', '']:
            return None
        
        # Common date formats to try
        date_formats = [
            '%Y-%m-%d',                    # 2024-01-15
            '%Y-%m-%d %H:%M:%S',           # 2024-01-15 10:30:45
            '%Y-%m-%d %H:%M:%S.%f',        # 2024-01-15 10:30:45.123456
            '%d/%m/%Y',                    # 15/01/2024
            '%d-%m-%Y',                    # 15-01-2024
            '%m/%d/%Y',                    # 01/15/2024
            '%m-%d-%Y',                    # 01-15-2024
            '%d/%m/%Y %H:%M:%S',           # 15/01/2024 10:30:45
            '%d-%m-%Y %H:%M:%S',           # 15-01-2024 10:30:45
            '%Y/%m/%d',                    # 2024/01/15
            '%Y/%m/%d %H:%M:%S',           # 2024/01/15 10:30:45
            '%d %b %Y',                    # 15 Jan 2024
            '%d %B %Y',                    # 15 January 2024
            '%b %d, %Y',                   # Jan 15, 2024
            '%B %d, %Y',                   # January 15, 2024
            '%Y%m%d',                      # 20240115
            '%d.%m.%Y',                    # 15.01.2024
            '%Y.%m.%d',                    # 2024.01.15
        ]
        
        # Try each format
        for fmt in date_formats:
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
        
        # Try ISO format parsing
        try:
            return datetime.fromisoformat(value.replace('Z', '+00:00'))
        except (ValueError, AttributeError):
            pass
        
        # Try parsing with regex for common patterns
        # Match patterns like: YYYY-MM-DD, DD/MM/YYYY, etc.
        date_patterns = [
            (r'(\d{4})-(\d{2})-(\d{2})(?:\s+(\d{2}):(\d{2}):(\d{2}))?', '%Y-%m-%d'),
            (r'(\d{2})/(\d{2})/(\d{4})(?:\s+(\d{2}):(\d{2}):(\d{2}))?', '%d/%m/%Y'),
            (r'(\d{2})-(\d{2})-(\d{4})(?:\s+(\d{2}):(\d{2}):(\d{2}))?', '%d-%m-%Y'),
        ]
        
        for pattern, base_fmt in date_patterns:
            match = re.match(pattern, value)
            if match:
                try:
                    if len(match.groups()) > 3:
                        return datetime.strptime(value, f'{base_fmt} %H:%M:%S')
                    else:
                        return datetime.strptime(value, base_fmt)
                except ValueError:
                    continue
        
        # If all parsing fails, log warning and return None
        logger.warning(f"⚠️ Could not parse date value: {value}")
        return None
    
    def _sanitize_value(self, value: Any, field_name: str) -> Any:
        """
        Sanitize a single value based on its type and field name
        
        Args:
            value: The value to sanitize
            field_name: Name of the field (used for date detection)
        
        Returns:
            Sanitized value
        """
        if value is None:
            return None
        
        # Check if field name suggests it's a date field
        field_lower = field_name.lower()
        is_date_field = any(keyword in field_lower for keyword in [
            'date', 'time', 'timestamp', 'created', 'updated', 'modified',
            'dob', 'birth', 'expiry', 'expires', 'valid', 'start', 'end'
        ])
        
        # If it's a date field or looks like a date, try to convert
        if is_date_field:
            sanitized_date = self._sanitize_date(value)
            if sanitized_date is not None:
                return sanitized_date
        
        # For string values, clean up whitespace
        if isinstance(value, str):
            value = value.strip()
            if value == '':
                return None
        
        # Return value as is if no sanitization needed
        return value
    
    def _calculate_unique_id(self, document: Dict[str, Any], unique_ids: List[str]) -> Optional[str]:
        """
        Calculate unique_id for a document based on unique_ids array
        
        Args:
            document: Document to calculate unique_id for
            unique_ids: List of field names that form unique identifier
        
        Returns:
            unique_id string (combined values with underscore) or None if unique_ids is empty
        """
        if not unique_ids or len(unique_ids) == 0:
            return None
        
        # Get values for each unique_id field
        values = []
        for field in unique_ids:
            value = document.get(field)
            if value is None:
                # If any field is None, we can't create a valid unique_id
                # Return None to indicate we can't create unique identifier
                return None
            # Convert to string and handle special characters
            value_str = str(value).strip()
            if not value_str:
                return None
            values.append(value_str)
        
        # Combine values with underscore
        return "_".join(values)
    
    def _get_changed_fields(self, existing_doc: Dict[str, Any], new_doc: Dict[str, Any]) -> Dict[str, Any]:
        """
        Compare two documents and return only changed fields
        
        Args:
            existing_doc: Existing document from processed collection
            new_doc: New document to compare
        
        Returns:
            Dictionary with only changed fields
        """
        changed_fields = {}
        
        # Compare each field in new_doc
        for key, new_value in new_doc.items():
            # Skip metadata fields that should always be updated
            if key in ['processed_at', 'updated_at']:
                changed_fields[key] = new_value
                continue
            
            existing_value = existing_doc.get(key)
            
            # Check if values are different
            if existing_value != new_value:
                changed_fields[key] = new_value
        
        return changed_fields
    
    def _sanitize_document(self, document: Dict[str, Any], selected_fields: List[str]) -> Dict[str, Any]:
        """
        Sanitize a document by processing selected fields
        
        Args:
            document: Original document from MongoDB
            selected_fields: List of fields to include in processed document
        
        Returns:
            Sanitized document with only selected fields
        """
        sanitized = {}
        
        for field in selected_fields:
            if field in document:
                sanitized[field] = self._sanitize_value(document[field], field)
            else:
                # Field not found in document, set to None
                sanitized[field] = None
        
        # Add processing metadata
        sanitized['processed_at'] = datetime.utcnow()
        
        return sanitized
    
    async def process_collection_data(
        self,
        collection_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Process data from collections based on field mappings
        
        This function:
        1. Reads from collection_field_mappings
        2. For each mapping, gets collection_name and selected_fields
        3. Reads data from the main collection
        4. Applies data sanitization (especially dates)
        5. Saves processed data to {collection_name}_processed
        
        Args:
            collection_name: Optional specific collection to process. If None, processes all collections
        
        Returns:
            Dictionary with processing status and results
        """
        if not mongodb_service.is_connected():
            raise HTTPException(
                status_code=503,
                detail="MongoDB is not connected"
            )
        
        try:
            # Get all field mappings
            if collection_name:
                # Get specific mapping
                mapping = mongodb_service.get_collection_field_mapping(collection_name)
                mappings = [mapping] if mapping else []
            else:
                # Get all mappings
                mappings = mongodb_service.list_all_field_mappings()
            
            if not mappings:
                return {
                    "status": 200,
                    "message": "No field mappings found to process",
                    "data": {
                        "collections_processed": 0,
                        "total_documents_processed": 0,
                        "results": []
                    }
                }
            
            results = []
            total_documents_processed = 0
            
            # Process each mapping
            for mapping in mappings:
                if not mapping:
                    continue
                
                collection_name_mapping = mapping.get('collection_name')
                selected_fields = mapping.get('selected_fields', [])
                
                if not collection_name_mapping or not selected_fields:
                    logger.warning(f"⚠️ Skipping mapping with missing collection_name or selected_fields: {mapping}")
                    continue
                
                try:
                    # Process this collection
                    result = await self._process_single_collection(
                        collection_name_mapping,
                        selected_fields
                    )
                    results.append(result)
                    total_documents_processed += result.get('documents_processed', 0)
                    
                except Exception as e:
                    logger.error(f"❌ Error processing collection '{collection_name_mapping}': {e}")
                    results.append({
                        "collection_name": collection_name_mapping,
                        "status": "error",
                        "error": str(e),
                        "documents_processed": 0
                    })
            
            return {
                "status": 200,
                "message": f"Processed {len(results)} collection(s)",
                "data": {
                    "collections_processed": len(results),
                    "total_documents_processed": total_documents_processed,
                    "results": results
                }
            }
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"❌ Error in process_collection_data: {e}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to process collection data: {str(e)}"
            )
    
    async def _process_single_collection(
        self,
        collection_name: str,
        selected_fields: List[str]
    ) -> Dict[str, Any]:
        """
        Process a single collection based on field mapping
        
        Args:
            collection_name: Name of the collection to process
            selected_fields: List of fields to select and process
        
        Returns:
            Dictionary with processing results
        """
        if not mongodb_service.is_connected() or mongodb_service.db is None:
            raise ConnectionError("MongoDB is not connected")
        
        try:
            # Get the main collection
            main_collection = mongodb_service.db[collection_name]
            
            # Check if collection exists and has data
            document_count = main_collection.count_documents({})
            if document_count == 0:
                logger.info(f"📋 Collection '{collection_name}' is empty, skipping processing")
                return {
                    "collection_name": collection_name,
                    "status": "skipped",
                    "message": "Collection is empty",
                    "documents_processed": 0
                }
            
            # Get the processed collection name
            processed_collection_name = f"{collection_name}_processed"
            processed_collection = mongodb_service.db[processed_collection_name]
            
            # Get unique_ids from raw_data_collection
            unique_ids_info = mongodb_service.get_collection_unique_ids(collection_name)
            unique_ids = unique_ids_info.get("unique_ids", []) if unique_ids_info else []
            
            logger.info(f"📋 Processing collection '{collection_name}' with unique_ids: {unique_ids}")
            
            # Read all documents from main collection
            documents = list(main_collection.find({}))
            
            # Process documents with upsert logic
            inserted_count = 0
            updated_count = 0
            skipped_count = 0
            processed_count = 0
            
            for doc in documents:
                try:
                    # Store original _id for reference (we'll remove it later)
                    original_id = doc.get('_id')
                    
                    # Calculate unique_id from original document (before sanitization)
                    # This ensures we use the actual field values, even if they're not in selected_fields
                    unique_id = self._calculate_unique_id(doc, unique_ids)
                    
                    # Remove MongoDB _id from document before processing
                    doc.pop('_id', None)
                    
                    # Sanitize document
                    sanitized_doc = self._sanitize_document(doc, selected_fields)
                    
                    # Add unique_id to sanitized document
                    sanitized_doc['unique_id'] = unique_id
                    
                    # Handle upsert logic
                    if unique_id is None:
                        # If unique_id is null, always insert (no duplicate check)
                        processed_collection.insert_one(sanitized_doc)
                        inserted_count += 1
                        processed_count += 1
                        logger.debug(f"✅ Inserted document with null unique_id for '{collection_name}'")
                    else:
                        # Check if document with this unique_id exists
                        existing_doc = processed_collection.find_one({"unique_id": unique_id})
                        
                        if existing_doc:
                            # Document exists - update only changed fields
                            changed_fields = self._get_changed_fields(existing_doc, sanitized_doc)
                            
                            if changed_fields:
                                # Update only changed fields
                                update_result = processed_collection.update_one(
                                    {"unique_id": unique_id},
                                    {"$set": changed_fields}
                                )
                                if update_result.modified_count > 0:
                                    updated_count += 1
                                    processed_count += 1
                                    logger.debug(f"🔄 Updated document with unique_id '{unique_id}' for '{collection_name}'")
                                else:
                                    skipped_count += 1
                            else:
                                # No changes, skip update
                                skipped_count += 1
                                logger.debug(f"⏭️ No changes for document with unique_id '{unique_id}' for '{collection_name}'")
                        else:
                            # Document doesn't exist - insert new
                            processed_collection.insert_one(sanitized_doc)
                            inserted_count += 1
                            processed_count += 1
                            logger.debug(f"✅ Inserted new document with unique_id '{unique_id}' for '{collection_name}'")
                    
                except Exception as e:
                    logger.warning(f"⚠️ Error processing document in '{collection_name}': {e}")
                    continue
            
            logger.info(
                f"✅ Processed {processed_count} documents from '{collection_name}' to '{processed_collection_name}': "
                f"{inserted_count} inserted, {updated_count} updated, {skipped_count} skipped"
            )
            
            return {
                "collection_name": collection_name,
                "processed_collection_name": processed_collection_name,
                "status": "success",
                "documents_processed": processed_count,
                "documents_inserted": inserted_count,
                "documents_updated": updated_count,
                "documents_skipped": skipped_count,
                "total_documents_in_source": document_count,
                "selected_fields": selected_fields,
                "selected_fields_count": len(selected_fields),
                "unique_ids": unique_ids
            }
            
        except Exception as e:
            logger.error(f"❌ Error processing collection '{collection_name}': {e}")
            raise

