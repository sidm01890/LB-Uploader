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
from app.core.header_normalizer import normalize_dataframe_columns
from app.controllers.scheduled_jobs_controller import ScheduledJobsController

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
        self.scheduled_jobs_controller = ScheduledJobsController()
    
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
    
    async def _process_collection_after_upload(self, datasource: str):
        """
        Background task to process collection data immediately after upload
        
        Args:
            datasource: Data source identifier (collection name)
        """
        try:
            logger.info(f"🔄 Starting immediate processing for collection '{datasource}' after upload")
            result = await self.scheduled_jobs_controller.process_collection_data(collection_name=datasource)
            
            if result.get("status") == 200:
                data = result.get("data", {})
                documents_processed = data.get("total_documents_processed", 0)
                logger.info(
                    f"✅ Immediate processing completed for '{datasource}': "
                    f"{documents_processed} document(s) processed"
                )
            else:
                logger.warning(f"⚠️ Immediate processing completed with status: {result.get('status')} for '{datasource}'")
                
        except Exception as e:
            logger.error(f"❌ Error in immediate processing for '{datasource}': {e}", exc_info=True)
    
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
            data_save_results = []
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
                
                # Parse Excel/CSV file and extract row-wise data
                try:
                    logger.info(f"📊 Reading file: {filename}")
                    
                    # Determine file type and read accordingly
                    if filename.lower().endswith('.csv'):
                        df = pd.read_csv(absolute_path)
                    else:
                        # Excel file - read first sheet by default
                        df = pd.read_excel(absolute_path, engine='openpyxl')
                    
                    # Normalize column headers (clean special characters, spaces, etc.)
                    df = normalize_dataframe_columns(df, inplace=False)
                    columns_count = len(df.columns)
                    logger.info(f"📝 Normalized {columns_count} columns: {list(df.columns)[:5]}...")
                    
                    # Replace NaN/NaT with None for MongoDB compatibility
                    df = df.replace({pd.NA: None, pd.NaT: None})
                    
                    # Get total row count
                    total_rows = len(df)
                    logger.info(f"📊 File contains {total_rows:,} rows")
                    
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
                        row_data = df.to_dict('records')
                        if len(row_data) != total_rows:
                            logger.warning(
                                f"⚠️ Conversion mismatch: Expected {total_rows:,} rows, "
                                f"got {len(row_data):,} rows"
                            )
                    
                    logger.info(f"📊 Parsed file: {columns_count} columns, {len(row_data):,} rows ready for MongoDB (Expected: {total_rows:,})")
                    
                    # Save row-wise data to MongoDB collection (collection name = datasource lowercase)
                    # MongoDB service will handle batching internally
                    if row_data:
                        save_result = mongodb_service.save_excel_data_row_wise(
                            collection_name=datasource,
                            row_data=row_data
                        )
                        data_save_results.append({
                            "filename": filename,
                            "success": save_result.get("success", False),
                            "rows_inserted": save_result.get("rows_inserted", 0),
                            "columns_count": save_result.get("columns_count", 0),
                            "collection_name": save_result.get("collection_name", datasource.lower()),
                            "message": save_result.get("message", "")
                        })
                        if save_result.get("success"):
                            logger.info(f"✅ Saved {save_result.get('rows_inserted', 0):,} rows to MongoDB collection '{datasource.lower()}'")
                            
                            # Trigger immediate processing in background after data is saved
                            background_tasks.add_task(
                                self._process_collection_after_upload,
                                datasource
                            )
                        else:
                            logger.warning(f"⚠️ Failed to save Excel data to MongoDB: {save_result.get('message')}")
                    else:
                        logger.warning("⚠️ No row data found in Excel file")
                        data_save_results.append({
                            "filename": filename,
                            "success": False,
                            "message": "No row data found in Excel file"
                        })
                        
                except MemoryError as mem_error:
                    logger.error(f"❌ Memory error parsing large file: {str(mem_error)}")
                    data_save_results.append({
                        "filename": filename,
                        "success": False,
                        "message": f"File too large to process in memory. Consider splitting the file or increasing server memory. Error: {str(mem_error)}"
                    })
                except Exception as parse_error:
                    logger.error(f"❌ Error parsing Excel file: {str(parse_error)}", exc_info=True)
                    data_save_results.append({
                        "filename": filename,
                        "success": False,
                        "message": f"Error parsing Excel file: {str(parse_error)}"
                    })
                    # Continue even if parsing fails - file is still saved
                
                # Save metadata to MongoDB
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
            
            # Process in background (send notification)
            background_tasks.add_task(self._process_upload_background, datasource, file_paths)
            
            return {
                "status": 200,
                "message": f"{datasource} data file uploaded",
                "data": {
                    "files_stored": len(file_paths),
                    "file_paths": file_paths,
                    "upload_ids": upload_ids if upload_ids else None,
                    "mongodb_connected": mongodb_service.is_connected(),
                    "collection_name": datasource.lower(),
                    "data_save_results": data_save_results if data_save_results else None
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
            
            # Get file size
            file_size = os.path.getsize(absolute_path)
            
            # Save metadata to MongoDB
            mongo_upload_id = mongodb_service.save_uploaded_file(
                filename=file_name,
                datasource=datasource,
                file_path=absolute_path,
                file_size=file_size,
                uploaded_by="api_user"
            )
            if mongo_upload_id:
                logger.info(f"📝 File metadata saved to MongoDB: upload_id={mongo_upload_id}")
            
            # Parse Excel/CSV file and extract row-wise data
            # For large files, we'll process in batches when saving to MongoDB
            data_save_result = None
            try:
                logger.info(f"📊 Reading file: {file_name}")
                
                # Determine file type and read accordingly
                if file_name.lower().endswith('.csv'):
                    # CSV files can be read in chunks for memory efficiency
                    logger.info("📊 Reading CSV file in chunks...")
                    
                    # Read first chunk to get column structure
                    df_sample = pd.read_csv(absolute_path, nrows=1000)
                    df = normalize_dataframe_columns(df_sample, inplace=False)
                    columns_count = len(df.columns)
                    logger.info(f"📝 Detected {columns_count} columns")
                    
                    # Read entire CSV (pandas handles this efficiently)
                    # For very large CSVs, we could use chunksize, but let's try full read first
                    df = pd.read_csv(absolute_path)
                else:
                    # Excel file - read with openpyxl
                    # For large Excel files, pandas/openpyxl will handle memory management
                    logger.info("📊 Reading Excel file...")
                    df = pd.read_excel(absolute_path, engine='openpyxl')
                
                # Normalize column headers (clean special characters, spaces, etc.)
                df = normalize_dataframe_columns(df, inplace=False)
                columns_count = len(df.columns)
                logger.info(f"📝 Normalized {columns_count} columns: {list(df.columns)[:5]}...")
                
                # Replace NaN/NaT with None for MongoDB compatibility
                df = df.replace({pd.NA: None, pd.NaT: None})
                
                # Get total row count
                total_rows = len(df)
                logger.info(f"📊 File contains {total_rows:,} rows")
                
                # For very large files, convert to dict in chunks to manage memory
                # Convert DataFrame to row-wise format (list of dictionaries)
                # We'll process in batches when saving to MongoDB
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
                    row_data = df.to_dict('records')
                    if len(row_data) != total_rows:
                        logger.warning(
                            f"⚠️ Conversion mismatch: Expected {total_rows:,} rows, "
                            f"got {len(row_data):,} rows"
                        )
                
                logger.info(f"📊 Parsed file: {columns_count} columns, {len(row_data):,} rows ready for MongoDB (Expected: {total_rows:,})")
                
                # Save row-wise data to MongoDB collection (collection name = datasource lowercase)
                # MongoDB service will handle batching internally
                if row_data:
                    save_result = mongodb_service.save_excel_data_row_wise(
                        collection_name=datasource,
                        row_data=row_data
                    )
                    data_save_result = {
                        "filename": file_name,
                        "success": save_result.get("success", False),
                        "rows_inserted": save_result.get("rows_inserted", 0),
                        "columns_count": save_result.get("columns_count", 0),
                        "collection_name": save_result.get("collection_name", datasource.lower()),
                        "message": save_result.get("message", "")
                    }
                    if save_result.get("success"):
                        logger.info(f"✅ Saved {save_result.get('rows_inserted', 0):,} rows to MongoDB collection '{datasource.lower()}'")
                        
                        # Trigger immediate processing in background after data is saved
                        background_tasks.add_task(
                            self._process_collection_after_upload,
                            datasource
                        )
                    else:
                        logger.warning(f"⚠️ Failed to save Excel data to MongoDB: {save_result.get('message')}")
                else:
                    logger.warning("⚠️ No row data found in Excel file")
                    data_save_result = {
                        "filename": file_name,
                        "success": False,
                        "message": "No row data found in Excel file"
                    }
                    
            except MemoryError as mem_error:
                logger.error(f"❌ Memory error parsing large file: {str(mem_error)}")
                data_save_result = {
                    "filename": file_name,
                    "success": False,
                    "message": f"File too large to process in memory. Consider splitting the file or increasing server memory. Error: {str(mem_error)}"
                }
            except Exception as parse_error:
                logger.error(f"❌ Error parsing Excel file: {str(parse_error)}", exc_info=True)
                data_save_result = {
                    "filename": file_name,
                    "success": False,
                    "message": f"Error parsing Excel file: {str(parse_error)}"
                }
                # Continue even if parsing fails - file is still saved
            
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
                    "upload_id": mongo_upload_id,
                    "mongodb_connected": mongodb_service.is_connected(),
                    "collection_name": datasource.lower(),
                    "data_save_result": data_save_result
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

