"""
Scheduled Jobs Controller - Handles scheduled data processing jobs
Processes data from collections based on field mappings and saves to processed collections
"""

from fastapi import HTTPException
from typing import Dict, Any, List, Optional
import logging
from datetime import datetime
import re
import asyncio
import os
import gc

from app.services.mongodb_service import mongodb_service

logger = logging.getLogger(__name__)


class ScheduledJobsController:
    """Controller for scheduled data processing jobs"""
    
    def __init__(self):
        # Configurable batch processing settings
        # These can be adjusted via environment variables if needed
        # Reduced batch size for formula calculations to handle large datasets (800K+ records)
        self.batch_size = int(os.getenv("SCHEDULED_JOB_BATCH_SIZE", "5000"))  # Default for regular processing
        self.formula_batch_size = int(os.getenv("FORMULA_JOB_BATCH_SIZE", "1000"))  # Smaller batch for formula calculations
        self.batch_delay_seconds = float(os.getenv("SCHEDULED_JOB_BATCH_DELAY", "0.01"))  # Delay between batches
    
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
        
        # If all parsing fails, return None
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
    
    def _move_to_backup_and_delete(
        self,
        main_collection,
        backup_collection,
        original_docs: List[Dict[str, Any]],
        batch_unique_ids: List[Optional[str]],
        unique_ids: List[str],
        batch_results: Dict[str, int],
        collection_name: str,
        batch_num: int
    ) -> int:
        """
        Move processed documents to backup collection (insert only, no upsert)
        and delete from source
        
        All documents in the batch have been processed (inserted, updated, or skipped),
        so we move all of them to backup using insert only and remove from source.
        Note: Backup allows duplicates - same records can be inserted multiple times.
        
        Args:
            main_collection: Source collection to delete from
            backup_collection: Backup collection to move to
            original_docs: List of original documents with _id (in same order as batch)
            batch_unique_ids: List of unique_ids for each document (in same order)
            unique_ids: List of field names that form unique identifier
            batch_results: Results from batch processing (inserted, updated, skipped counts)
            collection_name: Name of collection being processed
            batch_num: Batch number for logging
        
        Returns:
            Number of documents moved to backup
        """
        try:
            # All documents in the batch have been processed (inserted, updated, or skipped)
            # So we move all of them to backup using insert only
            total_in_batch = len(original_docs)
            
            if total_in_batch == 0:
                return 0
            
            # Prepare documents for backup (ensure _id is present)
            backup_docs = []
            ids_to_delete = []
            
            for idx, doc in enumerate(original_docs):
                if '_id' not in doc:
                    continue
                
                # Create a copy for backup
                backup_doc = doc.copy()
                unique_id = batch_unique_ids[idx] if idx < len(batch_unique_ids) else None
                
                # Add unique_id to backup document if it exists
                if unique_id is not None:
                    backup_doc['unique_id'] = unique_id
                
                backup_docs.append(backup_doc)
                ids_to_delete.append(doc['_id'])
            
            if not backup_docs:
                return 0
            
            # Bulk insert to backup collection (insert only, allows duplicates)
            try:
                backup_result = backup_collection.insert_many(backup_docs, ordered=False)
                moved_count = len(backup_result.inserted_ids)
                
                # Bulk delete from source collection
                if moved_count > 0 and ids_to_delete:
                    delete_result = main_collection.delete_many({"_id": {"$in": ids_to_delete}})
                    deleted_count = delete_result.deleted_count
                    
                    logger.info(
                        f"✅ Batch {batch_num} for '{collection_name}': "
                        f"Inserted {moved_count} documents to backup, deleted {deleted_count} from source"
                    )
                    
                    return moved_count
                else:
                    logger.warning(f"⚠️ Batch {batch_num}: No documents moved to backup")
                    return 0
                    
            except Exception as backup_error:
                # Check if it's a duplicate key error (E11000) - this is expected and OK for backup
                error_str = str(backup_error)
                if 'E11000' in error_str or 'duplicate key' in error_str.lower():
                    logger.warning(
                        f"⚠️ Batch {batch_num}: Some documents already exist in backup (duplicates allowed). "
                        f"Attempting individual inserts..."
                    )
                    # Try inserting documents one by one to handle duplicates gracefully
                    moved_count = 0
                    for doc in backup_docs:
                        try:
                            backup_collection.insert_one(doc)
                            moved_count += 1
                        except Exception as doc_error:
                            # Skip duplicate documents silently
                            if 'E11000' not in str(doc_error) and 'duplicate key' not in str(doc_error).lower():
                                logger.warning(f"⚠️ Error inserting document to backup: {doc_error}")
                    
                    # Delete successfully moved documents from source
                    if moved_count > 0:
                        successful_ids = [backup_docs[i]['_id'] for i in range(moved_count)]
                        delete_result = main_collection.delete_many({"_id": {"$in": successful_ids}})
                        deleted_count = delete_result.deleted_count
                        logger.info(
                            f"✅ Batch {batch_num} for '{collection_name}': "
                            f"Inserted {moved_count} documents to backup (skipped duplicates), "
                            f"deleted {deleted_count} from source"
                        )
                    return moved_count
                else:
                    logger.error(f"❌ Error moving documents to backup in batch {batch_num}: {backup_error}")
                    # If backup fails, don't delete from source
                    return 0
                
        except Exception as e:
            logger.error(f"❌ Error in _move_to_backup_and_delete for batch {batch_num}: {e}")
            return 0
    
    def _process_batch_upsert(
        self,
        processed_collection,
        batch_docs: List[tuple],
        collection_name: str,
        batch_num: int
    ) -> Dict[str, Any]:
        """
        Process a batch of documents with upsert logic using bulk operations
        
        Args:
            processed_collection: MongoDB collection to write to
            batch_docs: List of tuples (unique_id, sanitized_doc)
            collection_name: Name of collection being processed
            batch_num: Batch number for logging
        
        Returns:
            Dictionary with counts of inserted, updated, skipped, processed
        """
        from pymongo import UpdateOne
        
        inserted_count = 0
        updated_count = 0
        skipped_count = 0
        processed_count = 0
        
        # Separate documents by unique_id status
        null_unique_id_docs = []
        docs_with_unique_id = []
        
        for unique_id, sanitized_doc in batch_docs:
            if unique_id is None:
                null_unique_id_docs.append(sanitized_doc)
            else:
                docs_with_unique_id.append((unique_id, sanitized_doc))
        
        # Bulk insert documents with null unique_id
        if null_unique_id_docs:
            try:
                result = processed_collection.insert_many(null_unique_id_docs, ordered=False)
                inserted_count += len(result.inserted_ids)
                processed_count += len(result.inserted_ids)
            except Exception as e:
                logger.warning(f"⚠️ Error bulk inserting null unique_id docs in batch {batch_num}: {e}")
        
        # For documents with unique_id, use bulk write with upsert
        if docs_with_unique_id:
            # First, get existing documents to check for changes
            unique_ids = [uid for uid, _ in docs_with_unique_id]
            existing_docs = {
                doc['unique_id']: doc 
                for doc in processed_collection.find({"unique_id": {"$in": unique_ids}})
            }
            
            # Prepare bulk operations
            bulk_operations = []
            docs_to_insert = []
            
            for unique_id, sanitized_doc in docs_with_unique_id:
                if unique_id in existing_docs:
                    # Document exists - check for changes
                    existing_doc = existing_docs[unique_id]
                    changed_fields = self._get_changed_fields(existing_doc, sanitized_doc)
                    
                    if changed_fields:
                        # Update only changed fields
                        bulk_operations.append(
                            UpdateOne(
                                {"unique_id": unique_id},
                                {"$set": changed_fields}
                            )
                        )
                    else:
                        skipped_count += 1
                else:
                    # Document doesn't exist - prepare for insert
                    docs_to_insert.append(sanitized_doc)
            
            # Execute bulk updates
            if bulk_operations:
                try:
                    result = processed_collection.bulk_write(bulk_operations, ordered=False)
                    updated_count += result.modified_count
                    processed_count += result.modified_count
                except Exception as e:
                    logger.warning(f"⚠️ Error in bulk update batch {batch_num}: {e}")
            
            # Bulk insert new documents
            if docs_to_insert:
                try:
                    result = processed_collection.insert_many(docs_to_insert, ordered=False)
                    inserted_count += len(result.inserted_ids)
                    processed_count += len(result.inserted_ids)
                except Exception as e:
                    logger.warning(f"⚠️ Error bulk inserting new docs in batch {batch_num}: {e}")
        
        logger.info(
            f"✅ Batch {batch_num} for '{collection_name}': "
            f"{inserted_count} inserted, {updated_count} updated, {skipped_count} skipped"
        )
        
        return {
            "inserted": inserted_count,
            "updated": updated_count,
            "skipped": skipped_count,
            "processed": processed_count
        }
    
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
                # Mark files as processed even if collection is empty
                files_updated = mongodb_service.update_upload_status_by_datasource(
                    datasource=collection_name,
                    status="processed",
                    metadata={
                        "processed_at": datetime.utcnow(),
                        "documents_processed": 0,
                        "note": "Collection was empty, no documents to process"
                    }
                )
                if files_updated > 0:
                    logger.info(f"✅ Updated {files_updated} file(s) status to 'processed' (empty collection) for datasource '{collection_name}'")
                
                return {
                    "collection_name": collection_name,
                    "status": "skipped",
                    "message": "Collection is empty",
                    "documents_processed": 0,
                    "files_status_updated": files_updated
                }
            
            # Get the processed collection name
            processed_collection_name = f"{collection_name}_processed"
            processed_collection = mongodb_service.db[processed_collection_name]
            
            # Get the backup collection name
            backup_collection_name = f"{collection_name}_backup"
            backup_collection = mongodb_service.db[backup_collection_name]
            
            # Get unique_ids from raw_data_collection
            unique_ids_info = mongodb_service.get_collection_unique_ids(collection_name)
            unique_ids = unique_ids_info.get("unique_ids", []) if unique_ids_info else []
            
            logger.info(f"📋 Processing collection '{collection_name}' with unique_ids: {unique_ids}")
            
            # Update file status to "processing" for all files with this datasource
            files_marked_processing = mongodb_service.update_upload_status_by_datasource(
                datasource=collection_name,
                status="processing",
                metadata={
                    "processing_started_at": datetime.utcnow()
                }
            )
            if files_marked_processing > 0:
                logger.info(f"🔄 Marked {files_marked_processing} file(s) as 'processing' for datasource '{collection_name}'")
            
            # Process documents in batches for better performance
            # Reduced batch size to prevent blocking other API requests
            batch_size = self.batch_size
            inserted_count = 0
            updated_count = 0
            skipped_count = 0
            processed_count = 0
            moved_to_backup_count = 0
            batch_num = 0
            
            # Process in batches using cursor
            cursor = main_collection.find({}).batch_size(batch_size)
            batch_docs = []
            batch_original_docs = []  # Store original documents for backup
            batch_unique_ids = []  # Store unique_ids for backup upsert
            
            for doc in cursor:
                try:
                    # Store original document with _id for backup
                    original_doc = doc.copy()
                    original_id = doc.get('_id')
                    
                    # Calculate unique_id from original document (before sanitization)
                    unique_id = self._calculate_unique_id(doc, unique_ids)
                    
                    # Remove MongoDB _id from document before processing
                    doc.pop('_id', None)
                    
                    # Sanitize document
                    sanitized_doc = self._sanitize_document(doc, selected_fields)
                    
                    # Add unique_id to sanitized document
                    sanitized_doc['unique_id'] = unique_id
                    
                    batch_docs.append((unique_id, sanitized_doc))
                    batch_original_docs.append(original_doc)  # Keep original for backup
                    batch_unique_ids.append(unique_id)  # Keep unique_id for backup upsert
                    
                    # Process batch when it reaches batch_size
                    if len(batch_docs) >= batch_size:
                        batch_num += 1
                        batch_results = self._process_batch_upsert(
                            processed_collection,
                            batch_docs,
                            collection_name,
                            batch_num
                        )
                        inserted_count += batch_results['inserted']
                        updated_count += batch_results['updated']
                        skipped_count += batch_results['skipped']
                        processed_count += batch_results['processed']
                        
                        # Move successfully processed documents to backup and delete from source
                        moved_count = self._move_to_backup_and_delete(
                            main_collection,
                            backup_collection,
                            batch_original_docs,
                            batch_unique_ids,
                            unique_ids,
                            batch_results,
                            collection_name,
                            batch_num
                        )
                        moved_to_backup_count += moved_count
                        
                        batch_docs = []  # Clear batch
                        batch_original_docs = []  # Clear original docs
                        batch_unique_ids = []  # Clear unique_ids
                        
                        # Yield control to event loop to allow other requests to be processed
                        await asyncio.sleep(self.batch_delay_seconds)
                        
                except Exception as e:
                    logger.warning(f"⚠️ Error processing document in '{collection_name}': {e}")
                    continue
            
            # Process remaining documents in final batch
            if batch_docs:
                batch_num += 1
                batch_results = self._process_batch_upsert(
                    processed_collection,
                    batch_docs,
                    collection_name,
                    batch_num
                )
                inserted_count += batch_results['inserted']
                updated_count += batch_results['updated']
                skipped_count += batch_results['skipped']
                processed_count += batch_results['processed']
                
                # Move successfully processed documents to backup and delete from source
                moved_count = self._move_to_backup_and_delete(
                    main_collection,
                    backup_collection,
                    batch_original_docs,
                    batch_unique_ids,
                    unique_ids,
                    batch_results,
                    collection_name,
                    batch_num
                )
                moved_to_backup_count += moved_count
            
            logger.info(
                f"✅ Processed {processed_count} documents from '{collection_name}' to '{processed_collection_name}': "
                f"{inserted_count} inserted, {updated_count} updated, {skipped_count} skipped. "
                f"Moved {moved_to_backup_count} documents to '{backup_collection_name}'"
            )
            
            # Update file status to "processed" for all files with this datasource
            files_updated = 0
            if processed_count > 0:
                files_updated = mongodb_service.update_upload_status_by_datasource(
                    datasource=collection_name,
                    status="processed",
                    metadata={
                        "processed_at": datetime.utcnow(),
                        "documents_processed": processed_count
                    }
                )
                if files_updated > 0:
                    logger.info(f"✅ Updated {files_updated} file(s) status to 'processed' for datasource '{collection_name}'")
            else:
                # Even if no documents were processed, mark files as processed (collection was empty)
                files_updated = mongodb_service.update_upload_status_by_datasource(
                    datasource=collection_name,
                    status="processed",
                    metadata={
                        "processed_at": datetime.utcnow(),
                        "documents_processed": 0,
                        "note": "Collection was empty, no documents to process"
                    }
                )
                if files_updated > 0:
                    logger.info(f"✅ Updated {files_updated} file(s) status to 'processed' (empty collection) for datasource '{collection_name}'")
            
            return {
                "collection_name": collection_name,
                "processed_collection_name": processed_collection_name,
                "backup_collection_name": backup_collection_name,
                "status": "success",
                "documents_processed": processed_count,
                "documents_inserted": inserted_count,
                "documents_updated": updated_count,
                "documents_skipped": skipped_count,
                "documents_moved_to_backup": moved_to_backup_count,
                "files_status_updated": files_updated,
                "total_documents_in_source": document_count,
                "selected_fields": selected_fields,
                "selected_fields_count": len(selected_fields),
                "unique_ids": unique_ids
            }
            
        except Exception as e:
            logger.error(f"❌ Error processing collection '{collection_name}': {e}")
            # Update file status to "failed" if processing encounters an error
            mongodb_service.update_upload_status_by_datasource(
                datasource=collection_name,
                status="failed",
                metadata={
                    "error": str(e),
                    "failed_at": datetime.utcnow()
                }
            )
            raise
    
    def _parse_formula_text(self, formula_text: str) -> Dict[str, Any]:
        """
        Parse formula text to extract collection name and field references
        
        Args:
            formula_text: Formula text like "zomato.net_amount + zomato.merchant_pack_charge" or "CALCULATED_NET_AMOUNT + TAX_PAID_BY_CUSTOMER"
        
        Returns:
            Dictionary with:
                - source_collection: Primary collection name for this formula (first one found, e.g., "zomato" -> "zomato_processed")
                - field_references: List of field names referenced
                - calculated_field_references: List of calculated field names referenced (uppercase standalone fields)
                - all_collections: All collections referenced (base names, without _processed)
        """
        # Pattern to match collection.field but NOT numeric patterns like 0.05, 1.5, etc.
        # Collection name must start with a letter or underscore (not a digit)
        # This prevents matching "0.05" as "collection=0, field=05"
        collection_field_pattern = r'([a-zA-Z_]\w*)\.(\w+)'
        matches = re.findall(collection_field_pattern, formula_text)
        
        source_collections = []
        field_references = set()
        calculated_field_references = set()
        
        # Extract collection.field references
        for collection_name, field_name in matches:
            # Additional check: skip if collection name is all digits (shouldn't happen with new pattern, but safety check)
            if collection_name.isdigit():
                continue
                
            if field_name.isupper() or field_name.startswith('CALCULATED_'):
                calculated_field_references.add(field_name.upper())
            else:
                if collection_name not in source_collections:
                    source_collections.append(collection_name)
                field_references.add(field_name)
        
        # Extract standalone calculated field references (uppercase identifiers with underscores)
        # Pattern matches: CALCULATED_NET_AMOUNT, TAX_PAID_BY_CUSTOMER, PG_APPLIED_ON, etc.
        # But exclude numbers, operators, and collection.field patterns
        standalone_calc_pattern = r'\b([A-Z][A-Z0-9_]{2,})\b'
        standalone_matches = re.findall(standalone_calc_pattern, formula_text)
        
        # Get all collection.field patterns to exclude them
        collection_field_patterns = set()
        for coll, field in matches:
            collection_field_patterns.add(f"{coll}.{field}")
        
        for calc_ref in standalone_matches:
            # Skip if it's part of a collection.field pattern (e.g., "zomato.CALCULATED_NET_AMOUNT")
            # Check if this reference appears as "collection.calc_ref" pattern
            is_collection_field = False
            for coll, field in matches:
                if calc_ref.upper() == field.upper():
                    is_collection_field = True
                    break
            
            # Also check if it appears as part of a collection.field pattern in the text
            if not is_collection_field:
                # Check if there's a dot before or after this reference (indicating collection.field)
                pattern_before = r'\w+\.' + re.escape(calc_ref) + r'\b'
                pattern_after = r'\b' + re.escape(calc_ref) + r'\.\w+'
                if not (re.search(pattern_before, formula_text) or re.search(pattern_after, formula_text)):
                    calculated_field_references.add(calc_ref.upper())
        
        source_collection = None
        if source_collections:
            source_collection = f"{source_collections[0]}_processed"
        
        return {
            "source_collection": source_collection,
            "field_references": list(field_references),
            "calculated_field_references": list(calculated_field_references),
            "all_collections": source_collections
        }
    
    def _sort_formulas_by_dependencies(self, formulas: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Sort formulas by their dependencies using topological sort.
        Formulas that depend on other calculated fields will be processed after their dependencies.
        
        Args:
            formulas: List of formula dictionaries
        
        Returns:
            Sorted list of formulas in dependency order
        """
        # Build dependency graph and formula index
        formula_index = {}  # Maps logicNameKey -> formula
        formula_dependencies = {}  # Maps logicNameKey -> set of dependencies
        formula_outputs = {}  # Maps logicNameKey -> formula index in original list
        
        for idx, formula in enumerate(formulas):
            logic_name_key = formula.get('logicNameKey', '')
            if not logic_name_key:
                continue
            
            logic_name_key_upper = logic_name_key.upper()
            formula_index[logic_name_key_upper] = formula
            formula_outputs[logic_name_key_upper] = idx
            
            # Parse formula to get dependencies
            formula_text = formula.get('formulaText', '')
            meta = self._parse_formula_text(formula_text)
            calculated_deps = meta.get('calculated_field_references', [])
            formula_dependencies[logic_name_key_upper] = set(calculated_deps)
        
        # Topological sort using Kahn's algorithm
        # Count how many formulas each formula depends on (incoming edges)
        dependency_count = {key: 0 for key in formula_index.keys()}
        for formula_key, deps in formula_dependencies.items():
            for dep in deps:
                if dep in formula_index:  # Only count dependencies that are actually formulas
                    dependency_count[formula_key] = dependency_count.get(formula_key, 0) + 1
        
        # Start with formulas that have no dependencies (dependency_count = 0)
        queue = [key for key, count in dependency_count.items() if count == 0]
        sorted_formulas = []
        processed = set()
        
        # If no formulas have zero dependencies, use original order (circular dependency case)
        if not queue:
            logger.warning("⚠️ Circular dependency detected or all formulas depend on others. Using original order.")
            return formulas
        
        while queue:
            # Sort queue by original index to maintain relative order for formulas at same level
            queue.sort(key=lambda k: formula_outputs.get(k, 9999))
            
            current = queue.pop(0)
            if current in processed:
                continue
            
            processed.add(current)
            formula = formula_index[current]
            sorted_formulas.append(formula)
            
            # Find formulas that depend on current formula and reduce their dependency count
            for formula_key, deps in formula_dependencies.items():
                if current in deps:
                    dependency_count[formula_key] = dependency_count.get(formula_key, 1) - 1
                    if dependency_count[formula_key] == 0 and formula_key not in processed:
                        queue.append(formula_key)
        
        # Add any remaining formulas that weren't processed (shouldn't happen, but safety)
        for formula in formulas:
            logic_name_key = formula.get('logicNameKey', '')
            if logic_name_key and logic_name_key.upper() not in processed:
                sorted_formulas.append(formula)
                logger.warning(f"⚠️ Formula '{logic_name_key}' was not included in dependency sort, appending at end")
        
        # Log the sorted order
        if len(sorted_formulas) != len(formulas):
            logger.warning(f"⚠️ Formula count mismatch after sorting: {len(sorted_formulas)} vs {len(formulas)}")
        else:
            logger.info("✅ Formulas sorted by dependencies:")
            for idx, formula in enumerate(sorted_formulas, 1):
                logic_name_key = formula.get('logicNameKey', '')
                formula_text = formula.get('formulaText', '')
                deps = formula_dependencies.get(logic_name_key.upper(), set())
                if deps:
                    logger.info(f"  {idx}. {logic_name_key} (depends on: {', '.join(sorted(deps))}) = {formula_text}")
                else:
                    logger.info(f"  {idx}. {logic_name_key} (no dependencies) = {formula_text}")
        
        return sorted_formulas
    
    def _build_mapping_key(self, document: Dict[str, Any], key_fields: List[str], doc_id: Optional[Any] = None) -> Optional[str]:
        """
        Build a composite mapping key from the given document using provided fields.
        Returns None if any field is missing or empty.
        If key_fields is empty, returns a fallback key (unique_id or doc_id) if available.
        
        Args:
            document: Document dictionary (may not have _id if it was already popped)
            key_fields: List of field names to use for building the key
            doc_id: Optional document _id (passed separately if document._id was already removed)
        """
        if not key_fields:
            # Fallback: use unique_id if available, otherwise use doc_id parameter
            # This allows processing documents even when mapping_keys is not specified
            unique_id = document.get('unique_id')
            if unique_id:
                return str(unique_id)
            
            # Try document._id first, then fallback to doc_id parameter
            doc_id_value = document.get('_id') or doc_id
            if doc_id_value:
                return str(doc_id_value)
            
            # Last resort: return None (should rarely happen)
            return None
        
        values = []
        for field in key_fields:
            value = document.get(field)
            if value is None:
                return None
            value_str = str(value).strip()
            if value_str == "":
                return None
            values.append(value_str)
        
        return "_".join(values)
    
    def _build_condition_filter(self, conditions: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Convert condition objects into a MongoDB filter.
        Supported operators: equal, not_equal, greater_than, less_than, greater_equal, less_equal, in, not_in.
        """
        if not conditions:
            return {}
        
        mongo_filter: Dict[str, Any] = {}
        operator_map = {
            "equal": "$eq",
            "not_equal": "$ne",
            "greater_than": "$gt",
            "less_than": "$lt",
            "greater_equal": "$gte",
            "less_equal": "$lte",
            "in": "$in",
            "not_in": "$nin",
        }
        
        for condition in conditions:
            column = condition.get("column")
            operator = condition.get("operator")
            value = condition.get("value")
            
            if not column or not operator:
                continue
            
            mongo_op = operator_map.get(operator)
            if not mongo_op:
                continue
            
            if isinstance(value, str) and value.strip().lower() in ["null", "none", ""]:
                value = None
            
            if column not in mongo_filter:
                mongo_filter[column] = {}
            
            mongo_filter[column][mongo_op] = value
        
        return mongo_filter
    
    def _evaluate_condition(
        self,
        base_value: float,
        condition: Dict[str, Any]
    ) -> bool:
        """
        Evaluate a single condition against a base value.
        
        Args:
            base_value: The numeric value to test against
            condition: Condition dictionary with conditionType, value1, value2, formulaValue
        
        Returns:
            True if condition matches, False otherwise
        """
        condition_type = condition.get("conditionType", "").lower()
        value1_str = condition.get("value1", "")
        value2_str = condition.get("value2", "")
        
        try:
            # Convert value1 to float
            if not value1_str:
                return False
            value1 = float(value1_str)
            
            # For between condition, also need value2
            if condition_type == "between":
                if not value2_str:
                    return False
                value2 = float(value2_str)
                # Inclusive range check
                return value1 <= base_value <= value2
            elif condition_type == "equal":
                return base_value == value1
            elif condition_type == "greater_than":
                return base_value > value1
            elif condition_type == "less_than":
                return base_value < value1
            elif condition_type == "greater_equal":
                return base_value >= value1
            elif condition_type == "less_equal":
                return base_value <= value1
            else:
                logger.warning(f"⚠️ Unknown condition type: {condition_type}")
                return False
        except (ValueError, TypeError) as e:
            logger.warning(f"⚠️ Error evaluating condition: {e}")
            return False
    
    def _evaluate_reasons(
        self,
        reasons: List[Dict[str, Any]],
        calculated_fields: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Evaluate reasons based on delta column values and thresholds.
        
        Args:
            reasons: List of reason dictionaries with reason, delta_column, threshold, must_check
            calculated_fields: Dictionary of calculated fields including delta columns
        
        Returns:
            Dictionary with 'reason' (comma-separated string) and 'reconciliation_status'
        """
        matched_reasons = []
        
        if not reasons:
            return {
                "reason": "",
                "reconciliation_status": "RECONCILED"
            }
        
        for reason_config in reasons:
            try:
                reason_name = reason_config.get('reason', '')
                delta_column_name = reason_config.get('delta_column', '')
                threshold = reason_config.get('threshold', 0)
                must_check = reason_config.get('must_check', False)
                
                if not reason_name or not delta_column_name:
                    continue
                
                # Skip if must_check is false and we already have reasons
                if not must_check and matched_reasons:
                    continue
                
                # Get delta column value (case-insensitive lookup)
                delta_value = None
                delta_column_lower = delta_column_name.lower()
                
                # Try to find delta column value in calculated_fields
                for key, val in calculated_fields.items():
                    if key.lower() == delta_column_lower:
                        try:
                            delta_value = float(val) if val is not None else 0
                        except (ValueError, TypeError):
                            delta_value = 0
                        break
                
                # If delta column not found, default to 0
                if delta_value is None:
                    delta_value = 0
                
                # Check if absolute value exceeds threshold
                if abs(delta_value) > abs(threshold):
                    matched_reasons.append(reason_name)
                    
            except Exception as e:
                logger.warning(f"⚠️ Error evaluating reason '{reason_config.get('reason', 'unknown')}': {e}")
                continue
        
        # Build result
        if matched_reasons:
            return {
                "reason": ", ".join(matched_reasons),
                "reconciliation_status": "UNRECONCILED"
            }
        else:
            return {
                "reason": "",
                "reconciliation_status": "RECONCILED"
            }
    
    def _evaluate_delta_column(
        self,
        delta_column: Dict[str, Any],
        calculated_fields: Dict[str, Any]
    ) -> Any:
        """
        Evaluate a delta column expression using calculated field values.
        
        Args:
            delta_column: Dictionary with delta_column_name, first_formula, second_formula, value
            calculated_fields: Dictionary of already calculated fields
        
        Returns:
            Calculated delta value or 0 if evaluation fails
        """
        try:
            delta_column_name = delta_column.get('delta_column_name', '')
            first_formula = delta_column.get('first_formula', '')
            second_formula = delta_column.get('second_formula', '')
            value_expression = delta_column.get('value', '')
            
            if not delta_column_name or not value_expression:
                logger.warning(f"⚠️ Delta column missing required fields: {delta_column}")
                return 0
            
            # Start with the value expression
            evaluated_expression = value_expression
            
            # Replace all calculated field references in the expression (case-insensitive)
            # First, build a map of all calculated fields (uppercase keys for matching)
            calc_field_map = {}
            for key, val in calculated_fields.items():
                # Skip metadata fields
                if key in ['processed_at', 'updated_at'] or key.endswith('_mapping_key'):
                    continue
                # Store both uppercase and lowercase versions for matching
                calc_field_map[key.upper()] = val
                calc_field_map[key.lower()] = val
            
            # Replace all calculated field references in the expression
            # Match uppercase identifiers (like NET_AMOUNT, CALCULATED_NET_AMOUNT, etc.)
            uppercase_pattern = r'\b([A-Z][A-Z0-9_]{2,})\b'
            matches = re.findall(uppercase_pattern, evaluated_expression)
            
            for match in matches:
                if match in calc_field_map:
                    try:
                        value = calc_field_map[match]
                        value_float = float(value) if value is not None else 0
                        # Replace all occurrences of this field reference
                        pattern = r'\b' + re.escape(match) + r'\b'
                        evaluated_expression = re.sub(pattern, str(value_float), evaluated_expression)
                    except (ValueError, TypeError):
                        # If conversion fails, use 0
                        pattern = r'\b' + re.escape(match) + r'\b'
                        evaluated_expression = re.sub(pattern, '0', evaluated_expression)
                else:
                    # Field not found in calculated_fields, use 0 as default
                    pattern = r'\b' + re.escape(match) + r'\b'
                    evaluated_expression = re.sub(pattern, '0', evaluated_expression)
            
            # Also try lowercase matching for any remaining references
            lowercase_pattern = r'\b([a-z][a-z0-9_]{2,})\b'
            lowercase_matches = re.findall(lowercase_pattern, evaluated_expression)
            
            for match in lowercase_matches:
                if match in calc_field_map:
                    try:
                        value = calc_field_map[match]
                        value_float = float(value) if value is not None else 0
                        pattern = r'\b' + re.escape(match) + r'\b'
                        evaluated_expression = re.sub(pattern, str(value_float), evaluated_expression)
                    except (ValueError, TypeError):
                        pattern = r'\b' + re.escape(match) + r'\b'
                        evaluated_expression = re.sub(pattern, '0', evaluated_expression)
                else:
                    pattern = r'\b' + re.escape(match) + r'\b'
                    evaluated_expression = re.sub(pattern, '0', evaluated_expression)
            
            # Validate expression contains only safe characters
            safe_pattern = r'^[0-9+\-*/().\s]+$'
            if not re.match(safe_pattern, evaluated_expression):
                logger.warning(
                    f"⚠️ Delta column '{delta_column_name}' expression contains invalid characters after evaluation: {evaluated_expression}. "
                    f"Original expression: {value_expression}"
                )
                return 0
            
            # Evaluate the expression
            result = eval(evaluated_expression)
            
            # Convert to float
            if isinstance(result, (int, float)):
                return float(result)
            else:
                return 0
                
        except Exception as e:
            logger.error(f"❌ Error evaluating delta column '{delta_column.get('delta_column_name', 'unknown')}': {e}")
            return 0
    
    def _evaluate_formula(
        self,
        formula_text: str,
        document: Dict[str, Any],
        calculated_fields: Dict[str, Any],
        conditions: Optional[List[Dict[str, Any]]] = None
    ) -> Any:
        """
        Evaluate a formula using eval() with document values.
        If conditions are provided, evaluates conditions and returns formulaValue from matching condition.
        
        Args:
            formula_text: Formula text like "zomato.net_amount + zomato.merchant_pack_charge"
                         or "CALCULATED_TOTAL_AMOUNT - zomato.taxes_zomato_fee"
                         or "zomato.taxes_zomato_fee * 0.5"
            document: Source document with field values
            calculated_fields: Dictionary of previously calculated fields
            conditions: Optional list of condition dictionaries to apply after formula evaluation
        
        Returns:
            Calculated result value (or formulaValue from matching condition, or 0 if no condition matches)
        """
        try:
            evaluated_formula = formula_text
            
            # Step 1: Replace collection.field patterns with document values
            # Pattern: collection.field (e.g., "zomato.net_amount")
            def replace_collection_field(match):
                collection_name = match.group(1)
                field_name = match.group(2)
                
                # Get value from document (source collection field)
                value = document.get(field_name)
                if value is None:
                    return "0"
                # Convert to number if possible
                if isinstance(value, (int, float)):
                    return str(value)
                try:
                    return str(float(value))
                except (ValueError, TypeError):
                    return "0"
            
            evaluated_formula = re.sub(
                r'(\w+)\.(\w+)',
                replace_collection_field,
                evaluated_formula
            )
            
            # Step 2: Replace standalone calculated field references (uppercase, no collection prefix)
            # Pattern: CALCULATED_FIELD_NAME or CALCULATED_TOTAL_AMOUNT (standalone, not collection.field)
            # We need to replace ALL calculated field references in the formula
            # Formulas reference fields in uppercase (e.g., COMMISSION_VALUE), but we store them in lowercase
            for calc_key, calc_value in calculated_fields.items():
                # Skip metadata fields
                if calc_key in ['processed_at', 'updated_at'] or calc_key.endswith('_mapping_key'):
                    continue
                
                # Convert stored key (lowercase) to uppercase for matching
                upper_key = calc_key.upper()
                
                # Replace uppercase version in formula (e.g., "COMMISSION_VALUE" -> value)
                # Use word boundaries to ensure we match the whole field name
                pattern = r'\b' + re.escape(upper_key) + r'\b'
                
                # Check if this field is referenced in the formula
                if re.search(pattern, evaluated_formula):
                    value_str = str(calc_value) if calc_value is not None else "0"
                    # Replace all occurrences
                    evaluated_formula = re.sub(pattern, value_str, evaluated_formula)
                    
            
            # Also try replacing with original case (in case formula uses lowercase)
            for calc_key, calc_value in calculated_fields.items():
                # Skip metadata fields
                if calc_key in ['processed_at', 'updated_at'] or calc_key.endswith('_mapping_key'):
                    continue
                
                # Try original case (lowercase) as well
                pattern = r'\b' + re.escape(calc_key) + r'\b'
                if re.search(pattern, evaluated_formula):
                    value_str = str(calc_value) if calc_value is not None else "0"
                    evaluated_formula = re.sub(pattern, value_str, evaluated_formula)
                    
            
            # Step 3: Check for any remaining calculated field references that weren't replaced
            # Extract all potential calculated field references (uppercase identifiers with underscores)
            # Pattern matches: COMMISSION_VALUE, PG_CHARGE, etc. (but not numbers or operators)
            remaining_calc_refs = re.findall(r'\b[A-Z][A-Z0-9_]{2,}\b', evaluated_formula)
            if remaining_calc_refs:
                # Get available calculated fields (uppercase)
                available_fields_upper = [k.upper() for k in calculated_fields.keys() 
                                        if k not in ['processed_at', 'updated_at'] and not k.endswith('_mapping_key')]
                
                # Find which references are missing
                missing_refs = [ref for ref in remaining_calc_refs if ref not in available_fields_upper]
                
                if missing_refs:
                    # Try to find which formulas should calculate these missing fields
                    formula_outputs = getattr(self, '_formula_outputs_cache', {})
                    missing_info = []
                    for missing_ref in missing_refs:
                        if missing_ref in formula_outputs:
                            formula_idx = formula_outputs[missing_ref]
                            missing_info.append(f"{missing_ref} (should be calculated by formula at position {formula_idx + 1}, but it hasn't run yet or failed)")
                        else:
                            missing_info.append(f"{missing_ref} (no formula found that calculates this field - formula with logicNameKey='{missing_ref}' is missing)")
                    
                    logger.error(
                        f"❌ Formula '{formula_text}' references calculated fields that haven't been calculated yet: {missing_refs}. "
                        f"Available calculated fields: {available_fields_upper}. "
                        f"Details: {', '.join(missing_info)}. "
                        f"Formula after partial replacement: {evaluated_formula}"
                    )
                    # Don't raise error yet, let it fail at validation step with better message
            
            # Step 4: Validate formula contains only safe characters
            # After replacement, should only have numbers, operators, parentheses, spaces, and decimal points
            safe_pattern = r'^[0-9+\-*/().\s]+$'
            if not re.match(safe_pattern, evaluated_formula):
                available_fields = [k.upper() for k in calculated_fields.keys() 
                                 if k not in ['processed_at', 'updated_at'] and not k.endswith('_mapping_key')]
                raise ValueError(
                    f"Formula contains invalid characters after evaluation: {evaluated_formula}. "
                    f"Original formula: {formula_text}. "
                    f"Available calculated fields: {available_fields}"
                )
            
            # Step 5: Evaluate the formula to get base value
            result = eval(evaluated_formula)
            
            # Convert to appropriate type
            base_value = 0
            if isinstance(result, (int, float)):
                base_value = float(result) if isinstance(result, float) else int(result)
            
            # Step 6: If conditions are provided, evaluate them
            if conditions and len(conditions) > 0:
                # Check each condition in order
                for condition in conditions:
                    if self._evaluate_condition(base_value, condition):
                        # Condition matched, return the formulaValue
                        formula_value_str = condition.get("formulaValue", "0")
                        try:
                            formula_value = float(formula_value_str)
                            return formula_value
                        except (ValueError, TypeError):
                            logger.warning(f"⚠️ Invalid formulaValue in condition: {formula_value_str}")
                            return 0
                
                # No condition matched, return 0 as fallback
                return 0
            
            # No conditions, return the base value
            return base_value
            
        except ValueError as e:
            logger.error(f"❌ Validation error evaluating formula '{formula_text}': {e}")
            return None
        except Exception as e:
            logger.error(f"❌ Error evaluating formula '{formula_text}': {e}")
            return None
    
    async def process_formula_calculations(
        self,
        report_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Process formula calculations from formulas collection
        
        This function:
        1. Reads from formulas collection
        2. For each formula document, gets report_name and formulas array
        3. Checks/creates target collection with report_name
        4. Parses each formula to extract source collection and fields
        5. Reads documents from source collection (_processed)
        6. Evaluates all formulas for each document
        7. Saves calculated fields to target collection
        
        Args:
            report_name: Optional specific report to process. If None, processes all reports
        
        Returns:
            Dictionary with processing status and results
        """
        if not mongodb_service.is_connected():
            raise HTTPException(
                status_code=503,
                detail="MongoDB is not connected"
            )
        
        try:
            # Get all formula documents
            if report_name:
                # Get specific formula document
                formula_doc = mongodb_service.get_report_formulas(report_name)
                formula_docs = [formula_doc] if formula_doc else []
            else:
                # Get all formula documents
                formula_docs = mongodb_service.get_all_formulas()
            
            if not formula_docs:
                return {
                    "status": 200,
                    "message": "No formula documents found to process",
                    "data": {
                        "reports_processed": 0,
                        "total_documents_processed": 0,
                        "results": []
                    }
                }
            
            results = []
            total_documents_processed = 0
            
            # Process each formula document
            for formula_doc in formula_docs:
                if not formula_doc:
                    continue
                
                report_name_from_doc = formula_doc.get('report_name')
                formulas = formula_doc.get('formulas', [])
                mapping_keys = formula_doc.get('mapping_keys', {}) or {}
                conditions = formula_doc.get('conditions', {}) or {}
                delta_columns = formula_doc.get('delta_columns', []) or []
                reasons = formula_doc.get('reasons', []) or []
                
                if not report_name_from_doc or not formulas:
                    logger.warning(f"⚠️ Skipping formula document with missing report_name or formulas: {formula_doc.get('_id')}")
                    continue
                
                try:
                    # Process this report's formulas
                    result = await self._process_single_report_formulas(
                        report_name_from_doc,
                        formulas,
                        mapping_keys,
                        conditions,
                        delta_columns,
                        reasons
                    )
                    results.append(result)
                    total_documents_processed += result.get('documents_processed', 0)
                    
                except Exception as e:
                    logger.error(f"❌ Error processing report '{report_name_from_doc}': {e}")
                    results.append({
                        "report_name": report_name_from_doc,
                        "status": "error",
                        "error": str(e),
                        "documents_processed": 0
                    })
            
            return {
                "status": 200,
                "message": f"Processed {len(results)} report(s)",
                "data": {
                    "reports_processed": len(results),
                    "total_documents_processed": total_documents_processed,
                    "results": results
                }
            }
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"❌ Error in process_formula_calculations: {e}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to process formula calculations: {str(e)}"
            )
    
    async def _process_single_report_formulas(
        self,
        report_name: str,
        formulas: List[Dict[str, Any]],
        mapping_keys: Dict[str, List[str]],
        conditions: Dict[str, List[Dict[str, Any]]],
        delta_columns: List[Dict[str, Any]] = None,
        reasons: List[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Process formulas for a single report with multi-collection mapping/upsert logic.
        
        Args:
            report_name: Target collection name
            formulas: List of formula dictionaries
            mapping_keys: Dict of collection -> list of fields for mapping keys
            conditions: Dict of collection -> list of filter conditions
            delta_columns: List of delta column dictionaries to calculate after formulas
            reasons: List of reason dictionaries to check after delta columns
        """
        if not mongodb_service.is_connected() or mongodb_service.db is None:
            raise ConnectionError("MongoDB is not connected")
        
        if delta_columns is None:
            delta_columns = []
        
        if reasons is None:
            reasons = []
        
        try:
            target_collection = mongodb_service.db[report_name]
            
            if not formulas:
                logger.warning(f"⚠️ No formulas found for report '{report_name}'")
                return {
                    "report_name": report_name,
                    "status": "skipped",
                    "message": "No formulas found",
                    "documents_processed": 0
                }
            
            # Parse formulas to determine collection ordering
            formulas_meta = []
            collection_order: List[str] = []
            formulas_without_collection = []  # Formulas that don't reference any collection.field pattern
            

            for formula in formulas:
                formula_text = formula.get("formulaText", "")
                logic_name_key = formula.get("logicNameKey", "N/A")
                meta = self._parse_formula_text(formula_text)
                source_collection = meta.get("source_collection")
                meta["formula"] = formula
                
                logger.debug(f"📋 Parsed formula '{logic_name_key}': source_collection={source_collection}, "
                           f"calc_refs={meta.get('calculated_field_references', [])}")
                
                if source_collection and source_collection not in collection_order:
                    collection_order.append(source_collection)
                formulas_meta.append(meta)
            
            # Group formulas by their primary source collection
            # Include ALL formulas - those with source_collection and those without
            formulas_by_collection: Dict[str, List[Dict[str, Any]]] = {}
            for meta in formulas_meta:
                source_collection = meta.get("source_collection")
                formula = meta["formula"]
                logic_name_key = formula.get("logicNameKey", "N/A")
                
                if source_collection:
                    formulas_by_collection.setdefault(source_collection, []).append(formula)
                    logger.debug(f"📋 Added formula '{logic_name_key}' to collection '{source_collection}'")
                else:
                    # Formulas without source_collection should be processed with the primary collection
                    formulas_without_collection.append(formula)
                    logger.debug(f"📋 Added formula '{logic_name_key}' to formulas_without_collection (no source_collection)")
            
            # If no collections found but we have formulas, we need at least one collection to process
            # Try to determine primary collection from mapping_keys
            if not collection_order:
                if formulas_without_collection:
                    # All formulas are without collection reference - try to find a collection from mapping_keys
                    if mapping_keys:
                        # Use the first collection in mapping_keys as primary
                        first_mapping_key = list(mapping_keys.keys())[0]
                        primary_collection_name = f"{first_mapping_key}_processed"
                        collection_order = [primary_collection_name]
                        logger.info(f"ℹ️ No source collections found in formulas, using first mapping_key collection '{primary_collection_name}' as primary")
                    else:
                        logger.warning(f"⚠️ Could not determine source collections for report '{report_name}' and no mapping_keys available")
                        return {
                            "report_name": report_name,
                            "status": "skipped",
                            "message": "Could not determine source collections and no mapping_keys available",
                            "documents_processed": 0
                        }
                else:
                    logger.warning(f"⚠️ Could not determine source collections for report '{report_name}'")
                    return {
                        "report_name": report_name,
                        "status": "skipped",
                        "message": "Could not determine source collections",
                        "documents_processed": 0
                    }
            
            primary_collection_name = collection_order[0]
            primary_base_name = primary_collection_name.replace("_processed", "")
            primary_mapping_key_field = f"{primary_base_name}_mapping_key"
            
            logger.info(f"📋 Formulas without collection: {[f.get('logicNameKey', 'N/A') for f in formulas_without_collection]}")
            
            # Add formulas without collection to the primary collection's formula list
            if formulas_without_collection:
                if primary_collection_name not in formulas_by_collection:
                    formulas_by_collection[primary_collection_name] = []
                formulas_by_collection[primary_collection_name].extend(formulas_without_collection)
                logger.info(f"ℹ️ Added {len(formulas_without_collection)} formula(s) without collection reference to primary collection '{primary_collection_name}'")
                logger.info(f"📋 Formulas added: {[f.get('logicNameKey', 'N/A') for f in formulas_without_collection]}")
            
            # Log formulas_by_collection after adding formulas without collection
            logger.info(f"📋 Formulas by collection AFTER adding formulas without collection:")
            for coll_name, coll_formulas in formulas_by_collection.items():
                logger.info(f"  Collection '{coll_name}': {len(coll_formulas)} formulas")
                for f in coll_formulas:
                    logger.info(f"    - {f.get('logicNameKey', 'N/A')} = {f.get('formulaText', 'N/A')}")
            
            # Ensure primary is first, others follow in discovery order
            collections_to_process = collection_order + [c for c in formulas_by_collection.keys() if c not in collection_order]
            
            # Log all formulas and what they calculate (for dependency tracking)
            formula_outputs = {}  # Maps logicNameKey -> formula index
            for idx, formula in enumerate(formulas):
                logic_name_key = formula.get('logicNameKey', '')
                if logic_name_key:
                    formula_outputs[logic_name_key.upper()] = idx
            
            logger.info(f"📋 Processing {len(formulas)} formula(s) for report '{report_name}'")
            for idx, formula in enumerate(formulas):
                logic_name_key = formula.get('logicNameKey', '')
                formula_text = formula.get('formulaText', '')
                if logic_name_key:
                    logger.info(f"  Formula {idx + 1}: {logic_name_key} = {formula_text}")
            
            # Store formula_outputs for use in error messages
            self._formula_outputs_cache = formula_outputs
            
            # Build mapping key field names for all collections that will be processed
            # This is needed for index creation
            all_mapping_key_fields = {
                primary_base_name: primary_mapping_key_field
            }
            for collection_name in collections_to_process:
                base_name = collection_name.replace("_processed", "")
                if base_name not in all_mapping_key_fields:
                    # Generate mapping key field name for this collection
                    all_mapping_key_fields[base_name] = f"{base_name}_mapping_key"
            
            # Ensure indexes exist on target collection for optimal query performance
            # This creates indexes on mapping key fields used in $in queries
            try:
                index_result = mongodb_service.ensure_formula_indexes(
                    report_name=report_name,
                    mapping_key_fields=all_mapping_key_fields
                )
                if index_result.get("success"):
                    logger.info(
                        f"📊 Index creation: {index_result.get('indexes_created', 0)} created, "
                        f"{index_result.get('indexes_skipped', 0)} already existed"
                    )
                else:
                    logger.warning(
                        f"⚠️ Index creation had issues (processing will continue): "
                        f"{', '.join(index_result.get('errors', []))}"
                    )
            except Exception as e:
                logger.warning(f"⚠️ Failed to ensure indexes for report '{report_name}': {e}. Processing will continue.")
            
            # Use smaller batch size for formula calculations to handle large datasets
            batch_size = self.formula_batch_size
            total_processed = 0
            total_errors = 0
            
            for collection_name in collections_to_process:
                base_name = collection_name.replace("_processed", "")
                mapping_key_fields = mapping_keys.get(base_name, []) or []
                condition_list = conditions.get(base_name, []) or []
                collection_formulas = formulas_by_collection.get(collection_name, [])
                
                logger.info(f"📋 Processing collection '{collection_name}': Found {len(collection_formulas)} formula(s) before sorting")
                logger.info(f"📋 Formulas in '{collection_name}' before sorting: {[f.get('logicNameKey', 'N/A') for f in collection_formulas]}")
                
                if not collection_formulas:
                    logger.info(f"ℹ️ No formulas for collection '{collection_name}', skipping")
                    continue
                
                # Sort formulas by dependencies to ensure calculated fields are available when needed
                collection_formulas = self._sort_formulas_by_dependencies(collection_formulas)
                logger.info(f"📋 Formulas in '{collection_name}' after sorting: {[f.get('logicNameKey', 'N/A') for f in collection_formulas]}")
                
                source_collection = mongodb_service.db[collection_name]
                query_filter = self._build_condition_filter(condition_list)
                document_count = source_collection.count_documents(query_filter)
                if document_count == 0:
                    logger.info(f"📋 Source collection '{collection_name}' has no matching documents, skipping")
                    continue
                
                logger.info(
                    f"📋 Processing report '{report_name}' for collection '{collection_name}' "
                    f"with {len(collection_formulas)} formula(s) and mapping keys {mapping_key_fields}"
                )
                logger.info(f"📊 Total documents to process: {document_count:,} (batch size: {batch_size})")
                
                cursor = source_collection.find(query_filter).batch_size(batch_size)
                batch_docs: List[tuple] = []
                batch_number = 0
                estimated_batches = (document_count + batch_size - 1) // batch_size  # Ceiling division
                skipped_count = 0  # Track skipped documents
                
                try:
                    for doc in cursor:
                        try:
                            # Store _id before removing it (needed for fallback when mapping_keys is empty)
                            doc_id = doc.get('_id')
                            doc.pop('_id', None)
                            
                            # Build mapping key (pass doc_id for fallback when mapping_key_fields is empty)
                            mapping_key_value = self._build_mapping_key(doc, mapping_key_fields, doc_id=doc_id)
                            
                            # If still None after fallback, skip this document (should rarely happen)
                            if mapping_key_value is None:
                                skipped_count += 1
                                if skipped_count <= 5:  # Log first 5 skipped documents
                                    logger.warning(
                                        f"⚠️ Skipping document #{skipped_count}: No mapping key available. "
                                        f"Document has unique_id={doc.get('unique_id')}, _id={doc_id}, "
                                        f"mapping_key_fields={mapping_key_fields}, "
                                        f"doc_keys={list(doc.keys())[:10]}"
                                    )
                                continue
                            
                            batch_docs.append((doc, mapping_key_value))
                            
                            if len(batch_docs) >= batch_size:
                                batch_number += 1
                                logger.info(
                                    f"🔄 Processing batch {batch_number}/{estimated_batches} "
                                    f"for collection '{collection_name}' "
                                    f"({len(batch_docs)} documents)"
                                )
                                
                                processed, errors = await self._process_formula_batch(
                                    batch_docs,
                                    collection_name,
                                    primary_mapping_key_field,
                                    primary_collection_name,
                                    mapping_key_fields,
                                    collection_formulas,
                                    target_collection,
                                    batch_number
                                )
                                total_processed += processed
                                total_errors += errors
                                
                                # Clear batch and force garbage collection to free memory
                                batch_docs = []
                                del processed, errors
                                
                                # Force garbage collection to release memory
                                gc.collect()
                                
                                logger.info(
                                    f"✅ Batch {batch_number}/{estimated_batches} completed. "
                                    f"Total processed so far: {total_processed:,}, errors: {total_errors}"
                                )
                                
                                await asyncio.sleep(self.batch_delay_seconds)
                        except Exception as e:
                            total_errors += 1
                            logger.warning(f"⚠️ Error processing document in '{report_name}' for collection '{collection_name}': {e}")
                            continue
                    
                    # Final batch
                    if batch_docs:
                        batch_number += 1
                        logger.info(
                            f"🔄 Processing final batch {batch_number}/{estimated_batches} "
                            f"for collection '{collection_name}' "
                            f"({len(batch_docs)} documents)"
                        )
                        
                        processed, errors = await self._process_formula_batch(
                            batch_docs,
                            collection_name,
                            primary_mapping_key_field,
                            primary_collection_name,
                            mapping_key_fields,
                            collection_formulas,
                            target_collection,
                            batch_number
                        )
                        total_processed += processed
                        total_errors += errors
                        
                        # Clear batch and force garbage collection
                        batch_docs = []
                        del processed, errors
                        gc.collect()
                        
                        logger.info(
                            f"✅ Final batch {batch_number}/{estimated_batches} completed. "
                            f"Total processed: {total_processed:,}, errors: {total_errors}"
                        )
                
                finally:
                    # Ensure cursor is closed and memory is released
                    if cursor:
                        cursor.close()
                    # Clear any remaining references
                    batch_docs = []
                    gc.collect()
                    logger.info(f"🧹 Memory cleanup completed for collection '{collection_name}'")
                    
                    # Log summary of skipped documents if any
                    if skipped_count > 0:
                        logger.warning(
                            f"⚠️ Skipped {skipped_count} document(s) from '{collection_name}' "
                            f"due to missing mapping keys. Check if documents have 'unique_id' or '_id' fields."
                        )
            
            logger.info(
                f"✅ Processed {total_processed} documents for report '{report_name}': "
                f"{total_processed} calculated, {total_errors} errors"
            )
            
            # After all collections are processed, calculate delta columns and reasons
            # This ensures all fields from all collections are available
            if delta_columns or reasons:
                logger.info(
                    f"📊 Calculating delta columns and reasons for report '{report_name}' "
                    f"after all collections are processed"
                )
                delta_reason_count = await self._calculate_delta_columns_and_reasons(
                    target_collection,
                    primary_mapping_key_field,
                    delta_columns,
                    reasons
                )
                logger.info(
                    f"✅ Calculated delta columns and reasons for {delta_reason_count} record(s) "
                    f"in report '{report_name}'"
                )
            
            return {
                "report_name": report_name,
                "source_collection": primary_collection_name,
                "status": "success",
                "documents_processed": total_processed,
                "documents_with_errors": total_errors,
                "formulas_count": len(formulas)
            }
        
        except Exception as e:
            logger.error(f"❌ Error processing report formulas '{report_name}': {e}")
            raise

    async def _calculate_delta_columns_and_reasons(
        self,
        target_collection,
        primary_mapping_key_field: str,
        delta_columns: List[Dict[str, Any]],
        reasons: List[Dict[str, Any]]
    ) -> int:
        """
        Calculate delta columns and reasons for all records in target collection.
        This is called AFTER all collections are processed and merged.
        
        Args:
            target_collection: Target MongoDB collection
            primary_mapping_key_field: Primary mapping key field name
            delta_columns: List of delta column dictionaries
            reasons: List of reason dictionaries
        
        Returns:
            Number of records processed
        """
        from pymongo import UpdateOne
        
        if not delta_columns and not reasons:
            return 0
        
        batch_size = self.formula_batch_size
        processed_count = 0
        
        # Get all records from target collection
        total_records = target_collection.count_documents({})
        if total_records == 0:
            logger.info("📊 No records found in target collection for delta/reason calculation")
            return 0
        
        logger.info(f"📊 Processing {total_records:,} record(s) for delta columns and reasons")
        
        cursor = target_collection.find({}).batch_size(batch_size)
        batch_operations = []
        batch_number = 0
        
        try:
            for record in cursor:
                try:
                    # Get all fields from the record (excluding MongoDB _id)
                    calculated_fields = {k: v for k, v in record.items() if not k.startswith("_")}
                    
                    # Calculate delta columns
                    if delta_columns:
                        for delta_column in delta_columns:
                            try:
                                delta_column_name = delta_column.get('delta_column_name', '')
                                if not delta_column_name:
                                    continue
                                
                                delta_value = self._evaluate_delta_column(delta_column, calculated_fields)
                                calculated_fields[delta_column_name.lower()] = delta_value
                                
                            except Exception as e:
                                logger.warning(f"⚠️ Error calculating delta column '{delta_column.get('delta_column_name', 'unknown')}': {e}")
                                delta_column_name = delta_column.get('delta_column_name', '')
                                if delta_column_name:
                                    calculated_fields[delta_column_name.lower()] = 0
                    
                    # Evaluate reasons
                    if reasons:
                        try:
                            reason_result = self._evaluate_reasons(reasons, calculated_fields)
                            calculated_fields['reason'] = reason_result.get('reason', '')
                            calculated_fields['reconciliation_status'] = reason_result.get('reconciliation_status', 'RECONCILED')
                        except Exception as e:
                            logger.warning(f"⚠️ Error evaluating reasons: {e}")
                            calculated_fields['reason'] = ''
                            calculated_fields['reconciliation_status'] = 'RECONCILED'
                    else:
                        # No reasons configured, default to RECONCILED
                        calculated_fields['reason'] = ''
                        calculated_fields['reconciliation_status'] = 'RECONCILED'
                    
                    # Update processed_at timestamp
                    calculated_fields['processed_at'] = datetime.utcnow()
                    
                    # Build filter query using primary mapping key
                    mapping_key_value = record.get(primary_mapping_key_field)
                    if mapping_key_value:
                        filter_query = {primary_mapping_key_field: mapping_key_value}
                    else:
                        # Fallback to _id if mapping key not found
                        filter_query = {"_id": record.get("_id")}
                    
                    batch_operations.append(
                        UpdateOne(
                            filter_query,
                            {"$set": calculated_fields}
                        )
                    )
                    processed_count += 1
                    
                    # Execute batch when it reaches batch_size
                    if len(batch_operations) >= batch_size:
                        batch_number += 1
                        try:
                            target_collection.bulk_write(batch_operations, ordered=False)
                            logger.info(
                                f"📊 Processed batch {batch_number} for delta/reasons: "
                                f"{len(batch_operations)} record(s)"
                            )
                        except Exception as e:
                            logger.error(f"❌ Error executing batch operations for delta/reasons batch {batch_number}: {e}")
                        
                        batch_operations = []
                        await asyncio.sleep(self.batch_delay_seconds)
                
                except Exception as e:
                    logger.warning(f"⚠️ Error processing record for delta/reasons: {e}")
                    continue
            
            # Process remaining records in final batch
            if batch_operations:
                batch_number += 1
                try:
                    target_collection.bulk_write(batch_operations, ordered=False)
                    logger.info(
                        f"📊 Processed final batch {batch_number} for delta/reasons: "
                        f"{len(batch_operations)} record(s)"
                    )
                except Exception as e:
                    logger.error(f"❌ Error executing final batch operations for delta/reasons: {e}")
        
        finally:
            if cursor:
                cursor.close()
            gc.collect()
        
        return processed_count
    
    async def _process_formula_batch(
        self,
        batch_docs: List[tuple],
        current_collection: str,
        primary_mapping_key_field: str,
        primary_collection_name: str,
        mapping_key_fields: List[str],
        collection_formulas: List[Dict[str, Any]],
        target_collection,
        batch_number: int = 0
    ) -> tuple:
        """
        Process a batch of documents for a specific collection and upsert into target.
        Returns (processed_count, error_count).
        
        Args:
            batch_number: Batch number for logging purposes
        """
        from pymongo import UpdateOne
        
        current_base_name = current_collection.replace("_processed", "")
        current_mapping_key_field = f"{current_base_name}_mapping_key"
        
        batch_keys = [key for _, key in batch_docs]
        
        # Fetch existing target docs matching primary or current mapping keys
        existing_docs_cursor = target_collection.find(
            {
                "$or": [
                    {primary_mapping_key_field: {"$in": batch_keys}},
                    {current_mapping_key_field: {"$in": batch_keys}}
                ]
            }
        )
        existing_map_primary: Dict[str, Dict[str, Any]] = {}
        existing_map_current: Dict[str, Dict[str, Any]] = {}
        try:
            for existing in existing_docs_cursor:
                if primary_mapping_key_field in existing:
                    existing_map_primary[existing[primary_mapping_key_field]] = existing
                if current_mapping_key_field in existing:
                    existing_map_current[existing[current_mapping_key_field]] = existing
        finally:
            # Ensure cursor is closed
            if existing_docs_cursor:
                existing_docs_cursor.close()
        
        bulk_operations = []
        processed_count = 0
        error_count = 0
        
        for doc, mapping_key_value in batch_docs:
            try:
                existing_doc = existing_map_primary.get(mapping_key_value) or existing_map_current.get(mapping_key_value)
                
                calculated_fields: Dict[str, Any] = {}
                if existing_doc:
                    calculated_fields.update({k: v for k, v in existing_doc.items() if not k.startswith("_")})
                
                # Only log formula processing order for first batch to reduce log noise
                if batch_number == 1:
                    formula_order = []
                    for idx, f in enumerate(collection_formulas):
                        logic_name_key = f.get('logicNameKey', '')
                        if logic_name_key:
                            formula_order.append(f"{idx + 1}. {logic_name_key}")
                    if formula_order:
                        logger.debug(f"📝 Formula processing order for collection '{current_collection}': {', '.join(formula_order)}")
                
                for formula_idx, formula in enumerate(collection_formulas):
                    logic_name_key = formula.get('logicNameKey', '')
                    formula_text = formula.get('formulaText', '')
                    formula_conditions = formula.get('conditions', []) or []
                    
                    if not logic_name_key or not formula_text:
                        logger.warning("⚠️ Skipping formula with missing logicNameKey or formulaText")
                        continue
                    
                    calculated_field_name = logic_name_key.lower()
                    
                    # Reduced logging - only log for first document of first batch
                    if batch_number == 1 and processed_count == 0 and formula_idx == 0:
                        available_calc_fields = [k.upper() for k in calculated_fields.keys() 
                                              if k not in ['processed_at', 'updated_at'] and not k.endswith('_mapping_key')]
                        logger.debug(f"[Batch {batch_number}] Evaluating formula '{logic_name_key}': {formula_text}")
                        logger.debug(f"Available calculated fields: {available_calc_fields}")
                    
                    calculated_value = self._evaluate_formula(
                        formula_text,
                        doc,
                        calculated_fields,
                        formula_conditions if formula_conditions else None
                    )
                    
                    if calculated_value is None:
                        logger.warning(f"⚠️ Formula '{logic_name_key}' (position {formula_idx + 1}) returned None, using 0")
                        calculated_value = 0
                    
                    calculated_fields[calculated_field_name] = calculated_value
                
                # Note: Delta columns and reasons are calculated AFTER all collections are processed
                # This ensures all fields from all collections are available for delta/reason calculations
                
                calculated_fields[current_mapping_key_field] = mapping_key_value
                calculated_fields['processed_at'] = datetime.utcnow()
                
                if current_collection == primary_collection_name:
                    filter_query = {primary_mapping_key_field: mapping_key_value}
                else:
                    if mapping_key_value in existing_map_primary:
                        filter_query = {primary_mapping_key_field: mapping_key_value}
                    elif mapping_key_value in existing_map_current:
                        filter_query = {current_mapping_key_field: mapping_key_value}
                    else:
                        filter_query = {current_mapping_key_field: mapping_key_value}
                
                bulk_operations.append(
                    UpdateOne(
                        filter_query,
                        {"$set": calculated_fields},
                        upsert=True
                    )
                )
                processed_count += 1
            
            except Exception as e:
                error_count += 1
                logger.warning(f"⚠️ Error preparing batch operation for mapping key '{mapping_key_value}': {e}")
                continue
        
        if bulk_operations:
            try:
                target_collection.bulk_write(bulk_operations, ordered=False)
            except Exception as bulk_error:
                logger.error(f"❌ Error executing bulk operations for collection '{current_collection}' in batch {batch_number}: {bulk_error}")
                error_count += len(bulk_operations)
        
        # Clear large data structures to free memory
        batch_keys = None
        existing_map_primary = None
        existing_map_current = None
        bulk_operations = None
        
        return processed_count, error_count

