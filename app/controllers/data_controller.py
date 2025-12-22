"""
Data Controller - Simple file upload and storage
Files are stored on server for further processing
"""

from fastapi import HTTPException, UploadFile, BackgroundTasks
from typing import Dict, Any, List
import logging
import os
import shutil
import glob
from datetime import datetime
import pandas as pd

from app.services.email_service import EmailService
from app.services.mongodb_service import mongodb_service

logger = logging.getLogger(__name__)

# Base directory for storing uploaded files
UPLOAD_BASE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data", "uploads")
os.makedirs(UPLOAD_BASE_DIR, exist_ok=True)

# Temporary directory for chunk storage
CHUNK_TEMP_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data", "temp", "chunks")
os.makedirs(CHUNK_TEMP_DIR, exist_ok=True)


class DataController:
    """Controller for file upload operations - stores files on server"""
    
    def __init__(self):
        self.email_service = EmailService()
    
    def _get_upload_directory(self, datasource: str) -> str:
        """Get upload directory for a datasource"""
        datasource_dir = os.path.join(UPLOAD_BASE_DIR, datasource.upper())
        os.makedirs(datasource_dir, exist_ok=True)
        return datasource_dir
    
    def _send_upload_notification(
        self,
        datasource: str,
        file_paths: List[str],
        is_success: bool = True,
        error_message: str = None
    ):
        """Send email notification after upload"""
        try:
            if not self.email_service.enabled:
                logger.info("📧 Email notifications are disabled")
                return
            
            file_names = [os.path.basename(path) for path in file_paths]
            
            subject = f"{'✅' if is_success else '❌'} {datasource} File Upload - {len(file_paths)} file(s)"
            html_content = f"""
            <html>
            <body>
                <h2>{'✅ File Upload Successful' if is_success else '❌ File Upload Failed'}</h2>
                <p><strong>Data Source:</strong> {datasource}</p>
                <p><strong>Files:</strong> {len(file_paths)}</p>
                <ul>
                    {''.join([f'<li>{name}</li>' for name in file_names])}
                </ul>
                {f'<p><strong>Error:</strong> {error_message}</p>' if not is_success and error_message else ''}
                <p><em>Files have been stored on server for further processing.</em></p>
            </body>
            </html>
            """
            
            # Get recipients from email service
            recipients = self.email_service.get_recipients_by_process(
                'Upload', 
                datasource, 
                'failure' if not is_success else 'success'
            )
            if recipients:
                emails = [r['email'] for r in recipients]
                self.email_service.send_email(emails, subject, html_content)
            else:
                logger.info("No email recipients configured")
                
        except Exception as e:
            logger.error(f"❌ Error sending email notification: {str(e)}")
    
    def _process_upload_background(
        self,
        datasource: str,
        file_paths: List[str]
    ):
        """Background task for file storage confirmation and email notification"""
        logger.info(f"✅ Files stored for {datasource}: {file_paths}")
        self._send_upload_notification(datasource, file_paths, is_success=True)
    
    async def _process_file_and_save_to_mongodb(
        self,
        file_path: str,
        filename: str,
        datasource: str,
        upload_id: str = None
    ):
        """
        Background task to process file and save data to MongoDB
        
        Args:
            file_path: Absolute path to the uploaded file
            filename: Name of the file
            datasource: Data source identifier (collection name)
            upload_id: Upload ID for status tracking (optional)
        """
        try:
            logger.info(f"🔄 Starting background processing for file: {filename}")
            
            # Get file size for metadata (if needed later)
            file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
            
            # Update status to "processing" if upload_id is provided
            if upload_id:
                mongodb_service.update_upload_status(
                    upload_id=upload_id,
                    status="processing",
                    metadata={"processing_started_at": datetime.utcnow()}
                )
            
            # Parse Excel/CSV file and extract row-wise data
            # Run CPU-intensive operations in thread pool to avoid blocking event loop
            try:
                logger.info(f"📊 Reading file: {filename}")
                
                # Run file reading in thread pool (non-blocking)
                loop = asyncio.get_event_loop()
                
                def _read_and_process_file():
                    """Synchronous function to read and process file"""
                    # Determine file type and read accordingly
                    if filename.lower().endswith('.csv'):
                        # For CSV, use header=0 to read first row as header
                        df = pd.read_csv(file_path, header=0, keep_default_na=False)
                    else:
                        # Excel file - read first sheet by default, header=0 means first row is header
                        # keep_default_na=False prevents pandas from treating empty cells as NaN
                        df = pd.read_excel(file_path, engine='openpyxl', header=0, keep_default_na=False)
                    
                    # Check if DataFrame is completely empty (no columns)
                    if df.empty and len(df.columns) == 0:
                        logger.warning(f"⚠️ File appears to be completely empty (no headers or data)")
                        return [], 0, []
                    
                    # Normalize column headers (clean special characters, spaces, etc.)
                    df = normalize_dataframe_columns(df, inplace=False)
                    columns_count = len(df.columns)
                    logger.info(f"📝 Normalized {columns_count} columns: {list(df.columns)[:5]}...")
                    
                    # Replace NaN/NaT with None for MongoDB compatibility
                    df = df.replace({pd.NA: None, pd.NaT: None})
                    
                    # Get total row count (excluding header row)
                    total_rows = len(df)
                    logger.info(f"📊 File contains {total_rows:,} data rows (excluding header)")
                    
                    # For very large files, convert to dict in chunks to manage memory
                    if total_rows > 100000:
                        logger.info(f"📦 Large file detected ({total_rows:,} rows). Processing in batches...")
                        # For large files, convert in smaller chunks
                        batch_size = 50000
                        all_row_data = []
                        total_converted = 0
                        
                        for i in range(0, total_rows, batch_size):
                            end_idx = min(i + batch_size, total_rows)
                            try:
                                df_batch = df.iloc[i:end_idx]
                                expected_batch_size = end_idx - i
                                batch_row_data = df_batch.to_dict('records')
                                actual_batch_size = len(batch_row_data)
                                
                                # Validate we got the expected number of rows
                                if actual_batch_size != expected_batch_size:
                                    logger.warning(
                                        f"⚠️ Batch conversion mismatch: Expected {expected_batch_size} rows, "
                                        f"got {actual_batch_size} rows (indices {i} to {end_idx})"
                                    )
                                
                                all_row_data.extend(batch_row_data)
                                total_converted += actual_batch_size
                                logger.info(f"📦 Converted rows {i:,} to {end_idx:,} ({actual_batch_size:,} rows, Total: {total_converted:,})")
                            except Exception as conv_error:
                                logger.error(f"❌ Error converting batch {i:,} to {end_idx:,}: {conv_error}", exc_info=True)
                                # Continue with next batch
                                continue
                        
                        row_data = all_row_data
                        
                        # Validate total conversion
                        if total_converted != total_rows:
                            logger.warning(
                                f"⚠️ Conversion mismatch: Expected {total_rows:,} rows, "
                                f"converted {total_converted:,} rows. Difference: {total_rows - total_converted:,} rows"
                            )
                    else:
                        # For smaller files, convert all at once
                        # For empty DataFrames, to_dict('records') returns empty list
                        row_data = df.to_dict('records')
                        logger.info(f"🔍 Debug: After to_dict('records'), row_data length: {len(row_data)}, total_rows: {total_rows}")
                        if len(row_data) != total_rows:
                            logger.warning(
                                f"⚠️ Conversion mismatch: Expected {total_rows:,} rows, "
                                f"got {len(row_data):,} rows"
                            )
                    
                    # Return row_data, columns_count, and normalized column names
                    normalized_column_names = list(df.columns)
                    logger.info(f"🔍 Debug: Returning - row_data length: {len(row_data)}, columns_count: {columns_count}, headers: {normalized_column_names[:3] if normalized_column_names else 'None'}...")
                    return row_data, columns_count, normalized_column_names
                
                # Run CPU-intensive operations in thread pool (non-blocking)
                row_data, columns_count, normalized_column_names = await loop.run_in_executor(
                    self.executor,
                    _read_and_process_file
                )
                
                logger.info(f"📊 Parsed file: {columns_count} columns, {len(row_data):,} rows ready for MongoDB")
                logger.info(f"🔍 Debug: row_data is empty: {not row_data}, columns_count: {columns_count}")
                
                # Handle empty file (only headers, no data rows)
                # Check if file has headers but no data rows
                if not row_data and columns_count > 0:
                    logger.info(f"✅ Detected empty file with headers - entering empty file handling block")
                    logger.info(f"📋 File contains only headers (no data rows). Storing {columns_count} header(s) in raw_data_collection.")
                    
                    # Get normalized column names (headers) - these are already normalized from _read_and_process_file
                    normalized_headers = normalized_column_names
                    
                    # Filter out any empty header names
                    normalized_headers = [h for h in normalized_headers if h and h.strip()]
                    if normalized_headers:
                        # Store headers in raw_data_collection as total_fields
                        try:
                            raw_data_collection = mongodb_service.db["raw_data_collection"]
                            collection_name_lower = datasource.lower()
                            logger.info(f"🔍 Debug: collection_name_lower: {collection_name_lower}")
                            logger.info(f"🔍 Debug: normalized_headers: {normalized_headers}")
                            # Update or create document in raw_data_collection
                            raw_data_collection.update_one(
                                {"collection_name": collection_name_lower},
                                {
                                    "$set": {
                                        "total_fields": normalized_headers,
                                        "updated_at": datetime.utcnow()
                                    },
                                    "$setOnInsert": {
                                        "collection_name": collection_name_lower,
                                        "created_at": datetime.utcnow()
                                    }
                                },
                                upsert=True
                            )
                            logger.info(f"✅ Stored {len(normalized_headers)} header(s) in raw_data_collection for '{collection_name_lower}': {normalized_headers[:5]}...")
                        except Exception as e:
                            logger.error(f"❌ Error storing headers in raw_data_collection: {e}", exc_info=True)
                    else:
                        logger.warning(f"⚠️ No valid headers found in file (all headers are empty)")
                    
                    # Update upload status to indicate file is empty (header-only)
                    if upload_id:
                        mongodb_service.update_upload_status(
                            upload_id=upload_id,
                            status="empty",
                            metadata={
                                "empty_file_type": "header_only",
                                "headers_count": columns_count,
                                "processed_at": datetime.utcnow()
                            }
                        )
                        logger.info(f"✅ Updated upload status to 'empty' for upload_id={upload_id}")
                    
                    # Don't process further - file is empty, no data to save
                    return
                elif not row_data and columns_count == 0:
                    logger.warning(f"⚠️ File appears to be completely empty (no headers or data). Skipping.")
                    
                    # Update upload status to indicate file is completely empty
                    if upload_id:
                        mongodb_service.update_upload_status(
                            upload_id=upload_id,
                            status="empty",
                            metadata={
                                "empty_file_type": "completely_empty",
                                "processed_at": datetime.utcnow()
                            }
                        )
                        logger.info(f"✅ Updated upload status to 'empty' for upload_id={upload_id}")
                    
                    return
                
                # File has data - upload_id should already be provided from upload_data method
                # If upload_id is not provided (shouldn't happen in normal flow), log warning
                if not upload_id:
                    logger.warning(f"⚠️ upload_id not provided for file with data: {filename}. Creating entry now.")
                    upload_id = mongodb_service.save_uploaded_file(
                        filename=filename,
                        datasource=datasource,
                        file_path=file_path,
                        file_size=file_size,
                        uploaded_by="api_user"
                    )
                    if upload_id:
                        logger.info(f"📝 File metadata saved to MongoDB: upload_id={upload_id}")
                
                # Update status to "processing" if upload_id is provided
                if upload_id:
                    mongodb_service.update_upload_status(
                        upload_id=upload_id,
                        status="processing",
                        metadata={"processing_started_at": datetime.utcnow()}
                    )
                
                # Save row-wise data to MongoDB collection (collection name = datasource lowercase)
                # MongoDB service will handle batching internally
                save_result = mongodb_service.save_excel_data_row_wise(
                    collection_name=datasource,
                    row_data=row_data
                )
                
                if save_result.get("success"):
                    rows_inserted = save_result.get("rows_inserted", 0)
                    logger.info(f"✅ Saved {rows_inserted:,} rows to MongoDB collection '{datasource.lower()}'")
                    
                    # Update status to "data_saved" if upload_id is provided
                    if upload_id:
                        mongodb_service.update_upload_status(
                            upload_id=upload_id,
                            status="data_saved",
                            metadata={
                                "data_saved_at": datetime.utcnow(),
                                "rows_inserted": rows_inserted,
                                "columns_count": save_result.get("columns_count", 0)
                            }
                        )
                    
                    # Trigger scheduled job processing after data is saved to MongoDB
                    # Task 3 will only start after Task 2 completes (MongoDB save is done)
                    # Since Task 2 is already a background task, awaiting Task 3 here won't block the main request thread
                    await self._process_collection_after_upload(datasource, upload_id)
                else:
                    logger.warning(f"⚠️ Failed to save Excel data to MongoDB: {save_result.get('message')}")
                    # Update status to "failed" if upload_id is provided
                    if upload_id:
                        mongodb_service.update_upload_status(
                            upload_id=upload_id,
                            status="failed",
                            error=save_result.get("message", "Failed to save data to MongoDB")
                        )
                    
            except MemoryError as mem_error:
                logger.error(f"❌ Memory error parsing large file: {str(mem_error)}")
                # Update status to "failed" if upload_id exists
                if upload_id:
                    mongodb_service.update_upload_status(
                        upload_id=upload_id,
                        status="failed",
                        error=f"Memory error: {str(mem_error)}"
                    )
            except Exception as parse_error:
                logger.error(f"❌ Error parsing Excel file: {str(parse_error)}", exc_info=True)
                
                # Try to read just headers if full parse failed (might be blank file)
                try:
                    logger.info(f"🔄 Attempting to read headers only after parse error...")
                    loop = asyncio.get_event_loop()
                    
                    def _read_headers_only():
                        """Try to read just the headers from the file"""
                        try:
                            if filename.lower().endswith('.csv'):
                                # Read only first row (header)
                                df = pd.read_csv(file_path, header=0, nrows=0, keep_default_na=False)
                            else:
                                # Read only header row for Excel
                                df = pd.read_excel(file_path, engine='openpyxl', header=0, nrows=0, keep_default_na=False)
                            
                            if len(df.columns) > 0:
                                df = normalize_dataframe_columns(df, inplace=False)
                                return list(df.columns), len(df.columns)
                            return [], 0
                        except Exception as e:
                            logger.error(f"❌ Error reading headers only: {e}")
                            return [], 0
                    
                    # Try to get headers even if full parse failed
                    headers_only, headers_count = await loop.run_in_executor(
                        self.executor,
                        _read_headers_only
                    )
                    
                    if headers_count > 0:
                        logger.info(f"📋 Found {headers_count} header(s) despite parse error. Storing in raw_data_collection.")
                        normalized_headers = [h for h in headers_only if h and h.strip()]
                        
                        if normalized_headers:
                            try:
                                raw_data_collection = mongodb_service.db["raw_data_collection"]
                                collection_name_lower = datasource.lower()
                                
                                raw_data_collection.update_one(
                                    {"collection_name": collection_name_lower},
                                    {
                                        "$set": {
                                            "total_fields": normalized_headers,
                                            "updated_at": datetime.utcnow()
                                        },
                                        "$setOnInsert": {
                                            "collection_name": collection_name_lower,
                                            "created_at": datetime.utcnow()
                                        }
                                    },
                                    upsert=True
                                )
                                logger.info(f"✅ Stored {len(normalized_headers)} header(s) in raw_data_collection for '{collection_name_lower}': {normalized_headers[:5]}...")
                                
                                # Update upload status to indicate file is empty (header-only) with parse error
                                if upload_id:
                                    mongodb_service.update_upload_status(
                                        upload_id=upload_id,
                                        status="empty",
                                        metadata={
                                            "empty_file_type": "header_only_parse_error",
                                            "headers_count": headers_count,
                                            "parse_error": str(parse_error),
                                            "processed_at": datetime.utcnow()
                                        }
                                    )
                                    logger.info(f"✅ Updated upload status to 'empty' for upload_id={upload_id}")
                                
                                return  # Exit early - headers stored, no need to continue
                            except Exception as e:
                                logger.error(f"❌ Error storing headers in raw_data_collection: {e}", exc_info=True)
                except Exception as header_error:
                    logger.error(f"❌ Error reading headers only: {header_error}", exc_info=True)
                
                # Update status to "failed" if upload_id exists
                if upload_id:
                    mongodb_service.update_upload_status(
                        upload_id=upload_id,
                        status="failed",
                        error=f"Error parsing file: {str(parse_error)}"
                    )
                
        except Exception as e:
            logger.error(f"❌ Error in background file processing for '{filename}': {e}", exc_info=True)
            # Don't create upload records for errors - only update if upload_id already exists
            # This ensures empty files don't get failed records created
    
    async def _process_collection_after_upload(self, datasource: str, upload_id: str = None):
        """
        Background task to process collection data after MongoDB save
        
        Args:
            datasource: Data source identifier (collection name)
            upload_id: Upload ID for status tracking (optional)
        """
        try:
            logger.info(f"🔄 Starting scheduled job processing for collection '{datasource}'")
            result = await self.scheduled_jobs_controller.process_collection_data(collection_name=datasource)
            
            if result.get("status") == 200:
                data = result.get("data", {})
                documents_processed = data.get("total_documents_processed", 0)
                logger.info(
                    f"✅ Scheduled job processing completed for '{datasource}': "
                    f"{documents_processed} document(s) processed"
                )
                
                # Update status to "processed" if upload_id is provided
                if upload_id:
                    mongodb_service.update_upload_status(
                        upload_id=upload_id,
                        status="processed",
                        metadata={
                            "processed_at": datetime.utcnow(),
                            "documents_processed": documents_processed
                        }
                    )
            else:
                logger.warning(f"⚠️ Scheduled job processing completed with status: {result.get('status')} for '{datasource}'")
                # Update status to indicate processing completed with warnings
                if upload_id:
                    mongodb_service.update_upload_status(
                        upload_id=upload_id,
                        status="processed",
                        metadata={
                            "processed_at": datetime.utcnow(),
                            "processing_warning": f"Status code: {result.get('status')}"
                        }
                    )
                
        except Exception as e:
            logger.error(f"❌ Error in scheduled job processing for '{datasource}': {e}", exc_info=True)
            # Update status to "failed" if upload_id is provided
            if upload_id:
                mongodb_service.update_upload_status(
                    upload_id=upload_id,
                    status="failed",
                    error=f"Error in scheduled job processing: {str(e)}"
                )
    
    async def upload_data(
        self,
        datasource: str,
        background_tasks: BackgroundTasks,
        files: List[UploadFile]
    ) -> Dict[str, Any]:
        """
        Upload Excel/CSV files and store on server.
        Files are saved to data/uploads/{datasource}/ directory.
        """
        try:
            logger.info(f"📤 Upload request received: datasource={datasource}, files={len(files)}")
            
            # Get upload directory
            upload_dir = self._get_upload_directory(datasource)
            
            # Save files
            file_paths = []
            upload_ids = []
            for file in files:
                # Generate filename if not provided
                if not file.filename:
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
                    content_type = getattr(file, 'content_type', '')
                    if 'excel' in content_type or 'spreadsheet' in content_type:
                        ext = '.xlsx'
                    elif 'csv' in content_type:
                        ext = '.csv'
                    else:
                        ext = '.xlsx'
                    filename = f"upload_{timestamp}{ext}"
                else:
                    filename = file.filename
                
                # Save file
                file_path = os.path.join(upload_dir, filename)
                chunk_size = 1024 * 1024  # 1MB chunks
                total_size = 0
                
                with open(file_path, "wb") as f:
                    while True:
                        chunk = await file.read(chunk_size)
                        if not chunk:
                            break
                        f.write(chunk)
                        total_size += len(chunk)
                
                absolute_path = os.path.abspath(file_path)
                file_paths.append(absolute_path)
                logger.info(f"✅ File saved: {absolute_path} (size: {total_size} bytes)")
                
                # Create uploaded_files entry immediately
                upload_id = mongodb_service.save_uploaded_file(
                    filename=filename,
                    datasource=datasource,
                    file_path=absolute_path,
                    file_size=total_size,
                    uploaded_by="api_user"
                )
                
                if upload_id:
                    upload_ids.append(upload_id)
                    logger.info(f"📝 File metadata saved to MongoDB: upload_id={upload_id}")
                else:
                    logger.warning(f"⚠️ Failed to save file metadata to MongoDB for {filename}")
                    # Continue processing even if metadata save fails
                
                # Trigger background task to process file and save data to MongoDB
                # upload_id is now provided upfront, background task will update status accordingly
                background_tasks.add_task(
                    self._process_file_and_save_to_mongodb,
                    absolute_path,
                    filename,
                    datasource,
                    upload_id  # upload_id is now created upfront
                )
            
            # Process in background (send notification)
            background_tasks.add_task(self._process_upload_background, datasource, file_paths)
            
            

            return {
                "status": 200,
                "message": f"{datasource} data file uploaded and stored",
                "data": {
                    "files_stored": len(file_paths),
                    "file_paths": file_paths,
                    "upload_ids": upload_ids if upload_ids else None,
                    "mongodb_connected": mongodb_service.is_connected(),
                    "collection_name": datasource.lower(),
                    "processing_status": "queued",
                    "message": "Files stored successfully. Data processing and MongoDB save will happen in background."
                }
            }
            
        except Exception as e:
            logger.error(f"Error during data upload: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail={
                    "error": "INTERNAL_SERVER_ERROR",
                    "message": f"An unexpected error occurred during data upload: {str(e)}",
                    "status": "error"
                }
            )
    
    async def upload_chunk(
        self,
        chunk: UploadFile,
        chunk_index: int,
        total_chunks: int,
        upload_id: str,
        file_name: str,
        datasource: str
    ) -> Dict[str, Any]:
        """Receive and store a single chunk of a file (for large files)"""
        try:
            chunk_dir = os.path.join(CHUNK_TEMP_DIR, upload_id)
            os.makedirs(chunk_dir, exist_ok=True)
            
            chunk_path = os.path.join(chunk_dir, f"chunk_{chunk_index:06d}")
            
            # Stream write chunk
            chunk_size = 1024 * 1024  # 1MB chunks
            with open(chunk_path, "wb") as f:
                while True:
                    data = await chunk.read(chunk_size)
                    if not data:
                        break
                    f.write(data)
            
            logger.info(f"📦 Chunk {chunk_index + 1}/{total_chunks} received for upload_id={upload_id}, file={file_name}")
            
            return {
                "status": 200,
                "message": "Chunk uploaded successfully",
                "data": {
                    "chunk_index": chunk_index,
                    "total_chunks": total_chunks,
                    "upload_id": upload_id
                }
            }
        except Exception as e:
            logger.error(f"Error uploading chunk: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail={
                    "error": "CHUNK_UPLOAD_ERROR",
                    "message": f"Failed to upload chunk: {str(e)}",
                    "status": "error"
                }
            )
    
    async def finalize_chunked_upload(
        self,
        upload_id: str,
        file_name: str,
        datasource: str,
        background_tasks: BackgroundTasks
    ) -> Dict[str, Any]:
        """Reassemble chunks into final file and store on server"""
        try:
            chunk_dir = os.path.join(CHUNK_TEMP_DIR, upload_id)
            
            # Get all chunks and sort by index
            chunk_files = sorted(glob.glob(os.path.join(chunk_dir, "chunk_*")))
            
            if not chunk_files:
                raise HTTPException(
                    status_code=400,
                    detail={
                        "error": "NO_CHUNKS_FOUND",
                        "message": f"No chunks found for upload_id: {upload_id}",
                        "status": "error"
                    }
                )
            
            # Get upload directory
            upload_dir = self._get_upload_directory(datasource)
            
            # Reassemble file
            final_path = os.path.join(upload_dir, file_name)
            os.makedirs(os.path.dirname(final_path), exist_ok=True)
            
            logger.info(f"🔗 Reassembling {len(chunk_files)} chunks into {file_name}")
            
            with open(final_path, "wb") as outfile:
                for chunk_path in chunk_files:
                    with open(chunk_path, "rb") as chunk_file:
                        shutil.copyfileobj(chunk_file, outfile)
            
            absolute_path = os.path.abspath(final_path)
            file_size = os.path.getsize(absolute_path)
            
            # Check if upload_id exists in database, if not create it
            # For chunked uploads, upload_id is provided by client but may not exist in DB yet
            existing_record = None
            if mongodb_service.is_connected():
                try:
                    existing_record = mongodb_service.db.uploaded_files.find_one({"upload_id": upload_id})
                except Exception as e:
                    logger.warning(f"⚠️ Error checking for existing upload_id: {e}")
            
            if not existing_record:
                # Create uploaded_files entry if it doesn't exist, using the provided upload_id
                created_upload_id = mongodb_service.save_uploaded_file(
                    filename=file_name,
                    datasource=datasource,
                    file_path=absolute_path,
                    file_size=file_size,
                    uploaded_by="api_user",
                    upload_id=upload_id  # Use the provided upload_id from client
                )
                
                if created_upload_id:
                    logger.info(f"📝 File metadata saved to MongoDB: upload_id={created_upload_id}")
                else:
                    logger.warning(f"⚠️ Failed to save file metadata to MongoDB for {file_name}")
            else:
                # Update existing record with final file path and size
                try:
                    mongodb_service.update_upload_status(
                        upload_id=upload_id,
                        status="stored",
                        metadata={
                            "file_path": absolute_path,
                            "file_size": file_size,
                            "reassembled_at": datetime.utcnow()
                        }
                    )
                    logger.info(f"✅ Updated existing upload record: upload_id={upload_id}")
                except Exception as e:
                    logger.warning(f"⚠️ Error updating existing upload record: {e}")
            
            # Trigger background task to process file and save data to MongoDB
            # upload_id is now provided upfront, background task will update status accordingly
            background_tasks.add_task(
                self._process_file_and_save_to_mongodb,
                absolute_path,
                file_name,
                datasource,
                upload_id  # upload_id is now provided upfront
            )
            
            # Cleanup chunks
            try:
                shutil.rmtree(chunk_dir)
                logger.info(f"🧹 Cleaned up chunks for upload_id={upload_id}")
            except Exception as e:
                logger.warning(f"Failed to cleanup chunks: {e}")
            
            # Process in background (send notification)
            background_tasks.add_task(self._process_upload_background, datasource, [absolute_path])
            
            return {
                "status": 200,
                "message": f"{datasource} file reassembled and stored",
                "data": {
                    "file_path": absolute_path,
                    "upload_id": upload_id,
                    "mongodb_connected": mongodb_service.is_connected(),
                    "collection_name": datasource.lower(),
                    "processing_status": "queued",
                    "message": "File stored successfully. Data processing and MongoDB save will happen in background."
                }
            }
        except HTTPException as he:
            raise he
        except Exception as e:
            logger.error(f"Error finalizing chunked upload: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail={
                    "error": "FINALIZE_ERROR",
                    "message": f"Failed to finalize upload: {str(e)}",
                    "status": "error"
                }
            )

