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

from app.services.mongodb_service import mongodb_service

logger = logging.getLogger(__name__)


class ScheduledJobsController:
    """Controller for scheduled data processing jobs"""
    
    def __init__(self):
        # Configurable batch processing settings
        # These can be adjusted via environment variables if needed
        self.batch_size = int(os.getenv("SCHEDULED_JOB_BATCH_SIZE", "5000"))  # Reduced to prevent blocking
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
            formula_text: Formula text like "zomato.net_amount + zomato.merchant_pack_charge"
        
        Returns:
            Dictionary with:
                - source_collection: Primary collection name for this formula (first one found, e.g., "zomato" -> "zomato_processed")
                - field_references: List of field names referenced
                - calculated_field_references: List of calculated field names referenced
                - all_collections: All collections referenced (base names, without _processed)
        """
        collection_field_pattern = r'(\w+)\.(\w+)'
        matches = re.findall(collection_field_pattern, formula_text)
        
        source_collections = []
        field_references = set()
        calculated_field_references = set()
        
        for collection_name, field_name in matches:
            if field_name.isupper() or field_name.startswith('CALCULATED_'):
                calculated_field_references.add(field_name.lower())
            else:
                if collection_name not in source_collections:
                    source_collections.append(collection_name)
                field_references.add(field_name)
        
        source_collection = None
        if source_collections:
            source_collection = f"{source_collections[0]}_processed"
        
        return {
            "source_collection": source_collection,
            "field_references": list(field_references),
            "calculated_field_references": list(calculated_field_references),
            "all_collections": source_collections
        }
    
    def _build_mapping_key(self, document: Dict[str, Any], key_fields: List[str]) -> Optional[str]:
        """
        Build a composite mapping key from the given document using provided fields.
        Returns None if any field is missing or empty.
        """
        if not key_fields:
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
                    logger.debug(f"Replaced {upper_key} with {value_str} in formula")
            
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
                    logger.debug(f"Replaced {calc_key} with {value_str} in formula")
            
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
                
                if not report_name_from_doc or not formulas:
                    logger.warning(f"⚠️ Skipping formula document with missing report_name or formulas: {formula_doc.get('_id')}")
                    continue
                
                try:
                    # Process this report's formulas
                    result = await self._process_single_report_formulas(
                        report_name_from_doc,
                        formulas,
                        mapping_keys,
                        conditions
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
        conditions: Dict[str, List[Dict[str, Any]]]
    ) -> Dict[str, Any]:
        """
        Process formulas for a single report with multi-collection mapping/upsert logic.
        
        Args:
            report_name: Target collection name
            formulas: List of formula dictionaries
            mapping_keys: Dict of collection -> list of fields for mapping keys
            conditions: Dict of collection -> list of filter conditions
        """
        if not mongodb_service.is_connected() or mongodb_service.db is None:
            raise ConnectionError("MongoDB is not connected")
        
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
                meta = self._parse_formula_text(formula.get("formulaText", ""))
                source_collection = meta.get("source_collection")
                meta["formula"] = formula
                if source_collection and source_collection not in collection_order:
                    collection_order.append(source_collection)
                formulas_meta.append(meta)
            
            # Group formulas by their primary source collection
            # Include ALL formulas - those with source_collection and those without
            formulas_by_collection: Dict[str, List[Dict[str, Any]]] = {}
            for meta in formulas_meta:
                source_collection = meta.get("source_collection")
                if source_collection:
                    formulas_by_collection.setdefault(source_collection, []).append(meta["formula"])
                else:
                    # Formulas without source_collection should be processed with the primary collection
                    formulas_without_collection.append(meta["formula"])
            
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
            
            # Add formulas without collection to the primary collection's formula list
            if formulas_without_collection:
                if primary_collection_name not in formulas_by_collection:
                    formulas_by_collection[primary_collection_name] = []
                formulas_by_collection[primary_collection_name].extend(formulas_without_collection)
                logger.info(f"ℹ️ Added {len(formulas_without_collection)} formula(s) without collection reference to primary collection '{primary_collection_name}'")
            
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
            
            batch_size = self.batch_size
            total_processed = 0
            total_errors = 0
            
            for collection_name in collections_to_process:
                base_name = collection_name.replace("_processed", "")
                mapping_key_fields = mapping_keys.get(base_name, []) or []
                condition_list = conditions.get(base_name, []) or []
                collection_formulas = formulas_by_collection.get(collection_name, [])
                
                if not collection_formulas:
                    logger.info(f"ℹ️ No formulas for collection '{collection_name}', skipping")
                    continue
                
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
                
                cursor = source_collection.find(query_filter).batch_size(batch_size)
                batch_docs: List[tuple] = []
                
                for doc in cursor:
                    try:
                        doc.pop('_id', None)
                        mapping_key_value = self._build_mapping_key(doc, mapping_key_fields)
                        if mapping_key_value is None:
                            continue
                        
                        batch_docs.append((doc, mapping_key_value))
                        
                        if len(batch_docs) >= batch_size:
                            processed, errors = await self._process_formula_batch(
                                batch_docs,
                                collection_name,
                                primary_mapping_key_field,
                                primary_collection_name,
                                mapping_key_fields,
                                collection_formulas,
                                target_collection
                            )
                            total_processed += processed
                            total_errors += errors
                            batch_docs = []
                            await asyncio.sleep(self.batch_delay_seconds)
                    except Exception as e:
                        total_errors += 1
                        logger.warning(f"⚠️ Error processing document in '{report_name}' for collection '{collection_name}': {e}")
                        continue
                
                # Final batch
                if batch_docs:
                    processed, errors = await self._process_formula_batch(
                        batch_docs,
                        collection_name,
                        primary_mapping_key_field,
                        primary_collection_name,
                        mapping_key_fields,
                        collection_formulas,
                        target_collection
                    )
                    total_processed += processed
                    total_errors += errors
            
            logger.info(
                f"✅ Processed {total_processed} documents for report '{report_name}': "
                f"{total_processed} calculated, {total_errors} errors"
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

    async def _process_formula_batch(
        self,
        batch_docs: List[tuple],
        current_collection: str,
        primary_mapping_key_field: str,
        primary_collection_name: str,
        mapping_key_fields: List[str],
        collection_formulas: List[Dict[str, Any]],
        target_collection
    ) -> tuple:
        """
        Process a batch of documents for a specific collection and upsert into target.
        Returns (processed_count, error_count).
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
        for existing in existing_docs_cursor:
            if primary_mapping_key_field in existing:
                existing_map_primary[existing[primary_mapping_key_field]] = existing
            if current_mapping_key_field in existing:
                existing_map_current[existing[current_mapping_key_field]] = existing
        
        bulk_operations = []
        processed_count = 0
        error_count = 0
        
        for doc, mapping_key_value in batch_docs:
            try:
                existing_doc = existing_map_primary.get(mapping_key_value) or existing_map_current.get(mapping_key_value)
                
                calculated_fields: Dict[str, Any] = {}
                if existing_doc:
                    calculated_fields.update({k: v for k, v in existing_doc.items() if not k.startswith("_")})
                
                # Log formula processing order for debugging
                formula_order = []
                for idx, f in enumerate(collection_formulas):
                    logic_name_key = f.get('logicNameKey', '')
                    if logic_name_key:
                        formula_order.append(f"{idx + 1}. {logic_name_key}")
                if formula_order:
                    logger.info(f"📝 Formula processing order for collection '{current_collection}': {', '.join(formula_order)}")
                
                for formula_idx, formula in enumerate(collection_formulas):
                    logic_name_key = formula.get('logicNameKey', '')
                    formula_text = formula.get('formulaText', '')
                    formula_conditions = formula.get('conditions', []) or []
                    
                    if not logic_name_key or not formula_text:
                        logger.warning("⚠️ Skipping formula with missing logicNameKey or formulaText")
                        continue
                    
                    calculated_field_name = logic_name_key.lower()
                    
                    # Log available calculated fields before evaluation (for debugging)
                    available_calc_fields = [k.upper() for k in calculated_fields.keys() 
                                          if k not in ['processed_at', 'updated_at'] and not k.endswith('_mapping_key')]
                    logger.info(f"[{formula_idx + 1}/{len(collection_formulas)}] Evaluating formula '{logic_name_key}': {formula_text}")
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
                    logger.info(f"✅ [{formula_idx + 1}/{len(collection_formulas)}] Calculated {logic_name_key} = {calculated_value}")
                
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
                logger.error(f"❌ Error executing bulk operations for collection '{current_collection}': {bulk_error}")
                error_count += len(bulk_operations)
        
        return processed_count, error_count

