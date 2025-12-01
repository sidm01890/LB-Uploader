"""
Devyani-style Upload Routes
Matches the devyani_python upload API pattern
"""
import logging
import os
import traceback
import time
import smtplib
import shutil
import glob
from datetime import datetime
from typing import List, Optional
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from fastapi import APIRouter, Depends, UploadFile, File, HTTPException, BackgroundTasks, Form, Query
from sqlalchemy.orm import Session

from app.core.enum.DataSource import DataSource
from app.core.database import db_manager
from app.services.upload.UploadServiceMap import serviceMap
from app.services.DataSourceAbstractService import DataSourceService

log = logging.getLogger(__name__)

# Temporary directory for chunk storage
CHUNK_TEMP_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data", "temp", "chunks")
os.makedirs(CHUNK_TEMP_DIR, exist_ok=True)

router = APIRouter(prefix='/devyani-service/api', tags=["Upload Data"])

# Alias router for frontend compatibility (maps /api/uploader/* to /devyani-service/api/*)
alias_router = APIRouter(prefix='/api/uploader', tags=["Upload Data (Alias)"])


def get_db():
    """Get database session"""
    with db_manager.get_sync_session() as db:
        yield db


async def _get_datasources_impl():
    """
    Implementation for getting datasources.
    Returns all datasources with their status (whether service is available).
    """
    try:
        datasources = []
        for ds in DataSource:
            # Check if service is available for this datasource
            service_available = ds in serviceMap
            service_info = None
            if service_available:
                service = serviceMap.get(ds)
                service_info = {
                    "type": type(service).__name__,
                    "available": True
                }
            
            datasources.append({
                "name": ds.name,
                "value": ds.value,
                "service_available": service_available,
                "service_info": service_info
            })
        
        return {
            "status": 200,
            "message": "Data sources retrieved successfully",
            "data": datasources
        }
    except Exception as e:
        log.error(f"Error getting datasources: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail={
                "error": "INTERNAL_SERVER_ERROR",
                "message": f"An error occurred while retrieving datasources: {str(e)}",
                "status": "error"
            }
        )


# Register the endpoint on both routers
@router.get("/datasource")
async def get_datasources():
    """Get list of available datasources - Devyani service endpoint"""
    return await _get_datasources_impl()


@alias_router.get("/datasource")
async def get_datasources_alias():
    """Get list of available datasources - Frontend alias endpoint"""
    return await _get_datasources_impl()


def send_upload_notification_email(datasource: DataSource, rows_inserted: int, processing_time: float, file_paths: List[str], is_success: bool = True, error_message: str = None):
    """Send email notification after upload (success or failure)"""
    try:
        from app.core.config import config
        import smtplib
        from email.mime.text import MIMEText
        from email.mime.multipart import MIMEMultipart
        
        # Check if email is enabled (handle both string and bool)
        email_enabled = os.getenv("EMAIL_ENABLED", "").lower() == "true" or \
                       os.getenv("email.enabled", "").lower() == "true" or \
                       config.email.enabled
        
        if not email_enabled:
            log.warning("📧 Email notifications are disabled")
            return
        
        # Get SMTP settings
        smtp_host = os.getenv("SMTP_HOST", config.email.smtp_host)
        smtp_port = int(os.getenv("SMTP_PORT", config.email.smtp_port))
        smtp_user = os.getenv("SMTP_USER", config.email.smtp_user)
        smtp_password = os.getenv("SMTP_PASSWORD", config.email.smtp_password)
        from_email = os.getenv("FROM_EMAIL", config.email.sender_email)
        from_name = os.getenv("FROM_NAME", config.email.from_name)
        
        if not smtp_user or not smtp_password:
            log.warning("📧 SMTP credentials not configured")
            return
        
        # Recipient emails
        recipients = [
            "siddharth.mishra@corepeelers.com",
            "s.venkat@corepeelers.com"
        ]
        
        # Format time
        if processing_time < 60:
            time_str = f"{processing_time:.1f} seconds"
        elif processing_time < 3600:
            minutes = int(processing_time // 60)
            seconds = int(processing_time % 60)
            time_str = f"{minutes}m {seconds}s"
        else:
            hours = int(processing_time // 3600)
            minutes = int((processing_time % 3600) // 60)
            time_str = f"{hours}h {minutes}m"
        
        # Get file names
        file_names = [path.split('/')[-1] for path in file_paths]
        
        # Determine status
        if is_success:
            header_color = "linear-gradient(135deg, #28a745 0%, #20c997 100%)"
            header_title = "✅ Data Upload Successful"
            border_color = "#28a745"
        else:
            header_color = "linear-gradient(135deg, #dc3545 0%, #c82333 100%)"
            header_title = "❌ Data Upload Failed"
            border_color = "#dc3545"
        
        # Create HTML email
        html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <style>
        body {{
            font-family: Arial, sans-serif;
            line-height: 1.6;
            color: #333;
            max-width: 600px;
            margin: 0 auto;
            padding: 20px;
        }}
        .header {{
            background: {header_color};
            color: white;
            padding: 20px;
            border-radius: 8px 8px 0 0;
            text-align: center;
        }}
        .content {{
            background: #f9f9f9;
            padding: 20px;
            border-radius: 0 0 8px 8px;
        }}
        .stat-box {{
            background: white;
            padding: 15px;
            margin: 10px 0;
            border-radius: 5px;
            border-left: 4px solid {border_color};
        }}
        .stat-label {{
            font-size: 12px;
            color: #666;
            text-transform: uppercase;
            margin-bottom: 5px;
        }}
        .stat-value {{
            font-size: 24px;
            font-weight: bold;
            color: #333;
        }}
        .error-box {{
            background: #fff3cd;
            border: 1px solid #ffc107;
            padding: 15px;
            margin: 15px 0;
            border-radius: 5px;
        }}
        .footer {{
            text-align: center;
            margin-top: 20px;
            font-size: 12px;
            color: #666;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>{header_title}</h1>
        <p>{datetime.now().strftime('%B %d, %Y at %I:%M %p')}</p>
    </div>
    <div class="content">
        <div class="stat-box">
            <div class="stat-label">Data Source</div>
            <div class="stat-value">{datasource.name}</div>
        </div>
        {f'<div class="stat-box"><div class="stat-label">Rows Inserted/Updated</div><div class="stat-value">{rows_inserted:,}</div></div>' if is_success and rows_inserted > 0 else ''}
        {f'<div class="stat-box"><div class="stat-label">Processing Time</div><div class="stat-value">{time_str}</div></div>' if is_success else ''}
        <div class="stat-box">
            <div class="stat-label">Files Processed</div>
            <div class="stat-value">{len(file_paths)}</div>
        </div>
        {f'<div class="error-box"><strong>Error:</strong><br>{error_message}</div>' if not is_success and error_message else ''}
        <div style="margin-top: 15px; padding: 10px; background: white; border-radius: 5px;">
            <strong>Files:</strong><br>
            {''.join([f'• {name}<br>' for name in file_names])}
        </div>
    </div>
    <div class="footer">
        <p>This is an automated notification from Devyani Upload System</p>
    </div>
</body>
</html>
        """
        
        if is_success:
            subject = f"✅ {datasource.name} Upload Complete - {rows_inserted:,} rows in {time_str}"
        else:
            subject = f"❌ {datasource.name} Upload Failed"
        
        # Send email directly using SMTP
        try:
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = f"{from_name} <{from_email}>"
            msg['To'] = ", ".join(recipients)
            
            msg.attach(MIMEText(html_content, 'html'))
            
            server = smtplib.SMTP(smtp_host, smtp_port, timeout=10)
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.send_message(msg)
            server.quit()
            
            log.info(f"📧 Email notification sent successfully to {len(recipients)} recipients")
        except Exception as email_error:
            log.error(f"❌ Failed to send email: {str(email_error)}")
            raise
    except Exception as e:
        log.error(f"❌ Error sending email notification: {str(e)}")
        log.error(f"Traceback: {traceback.format_exc()}")


def process_upload_background(datasource: DataSource, file_paths: List[str], username: str):
    """Background task for processing uploads"""
    start_time = time.time()
    log.info(f"🚀 Background task started: datasource={datasource.name}, files={len(file_paths)}")
    rows_inserted = 0
    error_message = None
    try:
        with db_manager.get_sync_session() as db:
            service = serviceMap.get(datasource)
            if service and isinstance(service, DataSourceService):
                log.info(f"📊 Processing {datasource.name} data from {file_paths}...")
                rows_inserted = service.upload(file_paths, db, username)
                processing_time = time.time() - start_time
                log.info(f"✅ Background processing completed for {datasource.name} file(s). Rows: {rows_inserted}, Time: {processing_time:.1f}s")
                
                # Send email notification on success (always send, even if 0 rows)
                send_upload_notification_email(datasource, rows_inserted, processing_time, file_paths, is_success=True)
            else:
                error_message = f"Service not found or invalid for datasource {datasource.name}"
                log.error(f"❌ {error_message}")
                processing_time = time.time() - start_time
                send_upload_notification_email(datasource, 0, processing_time, file_paths, is_success=False, error_message=error_message)
    except HTTPException as he:
        # HTTPException in background task - log but don't raise (response already sent)
        error_message = str(he.detail) if isinstance(he.detail, str) else str(he.detail.get('message', he.detail) if isinstance(he.detail, dict) else he.detail)
        log.error(f"❌ HTTP Error during background processing for {datasource.name}: {error_message}")
        log.error(f"Traceback: {traceback.format_exc()}")
        processing_time = time.time() - start_time
        send_upload_notification_email(datasource, 0, processing_time, file_paths, is_success=False, error_message=error_message)
    except Exception as e:
        # Other exceptions - log but don't raise (response already sent)
        error_message = str(e)
        log.error(f"❌ Error during background processing for {datasource.name}: {error_message}")
        log.error(f"Traceback: {traceback.format_exc()}")
        processing_time = time.time() - start_time
        send_upload_notification_email(datasource, 0, processing_time, file_paths, is_success=False, error_message=error_message)


async def _upload_data_impl(
    datasource: DataSource,
    background_tasks: BackgroundTasks,
    files: List[UploadFile] = File(...),
    db: Session = Depends(get_db)
):
    """
    Implementation for uploading files for a specific datasource.
    Matches devyani_python API pattern.
    
    Example:
        POST /devyani-service/api/upload?datasource=ZOMATO
        POST /devyani-service/api/upload?datasource=POS_ORDERS
        POST /api/uploader/upload?datasource=POS_ORDERS (alias)
    """
    try:
        log.info(f"📤 Upload request received: datasource={datasource.name}, files={len(files)}")
        service = serviceMap.get(datasource)
        if service:
            if isinstance(service, DataSourceService):
                # Save files asynchronously with streaming (handles large files)
                log.info(f"💾 Saving {len(files)} file(s) for {datasource.name}...")
                file_paths = await service.upload_file(files)
                log.info(f"✅ Files saved: {file_paths}")
                
                # Process in background
                log.info(f"🔄 Starting background processing for {datasource.name}...")
                background_tasks.add_task(process_upload_background, datasource, file_paths, "Anonymous")
                
                # Return immediate response matching devyani_python format
                message = f"{datasource.name} data file uploaded"
                if datasource == DataSource.ZOMATO:
                    message = "ZOMATO data file uploaded"
                elif datasource == DataSource.POS_ORDERS:
                    message = "Orders data file uploaded"
                
                return {
                    "status": 200,
                    "message": message,
                    "data": None
                }
            else:
                raise HTTPException(
                    status_code=500,
                    detail={
                        "error": "SERVICE_TYPE_ERROR",
                        "message": "Service is not a valid DataSourceService instance",
                        "status": "error"
                    }
                )
        else:
            raise HTTPException(
                status_code=400,
                detail={
                    "error": "SERVICE_NOT_FOUND",
                    "message": f"No service is available for DataSource : {datasource.name}",
                    "status": "error"
                }
            )
    except HTTPException as he:
        raise he
    except Exception as e:
        db.rollback()
        log.error(f"Error during data upload: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail={
                "error": "INTERNAL_SERVER_ERROR",
                "message": f"An unexpected error occurred during data upload: {str(e)}",
                "status": "error"
            }
        )


# Register the upload endpoint on both routers
@router.post("/upload")
async def upload_data(
    datasource: DataSource,
    background_tasks: BackgroundTasks,
    files: List[UploadFile] = File(...),
    db: Session = Depends(get_db)
):
    """Upload files - Devyani service endpoint"""
    return await _upload_data_impl(datasource, background_tasks, files, db)


@alias_router.post("/upload")
async def upload_data_alias(
    datasource: DataSource,
    background_tasks: BackgroundTasks,
    files: List[UploadFile] = File(...),
    db: Session = Depends(get_db)
):
    """Upload files - Frontend alias endpoint"""
    return await _upload_data_impl(datasource, background_tasks, files, db)


# ============================================================================
# CHUNKED UPLOAD ENDPOINTS (for large files)
# ============================================================================

@router.post("/upload-chunk")
async def upload_chunk(
    chunk: UploadFile = File(...),
    chunk_index: int = Form(...),
    total_chunks: int = Form(...),
    upload_id: str = Form(...),
    file_name: str = Form(...),
    datasource: DataSource = Query(...)
):
    """
    Receive and store a single chunk of a file.
    Used for chunked uploads of large files.
    """
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
        
        log.info(f"📦 Chunk {chunk_index + 1}/{total_chunks} received for upload_id={upload_id}, file={file_name}")
        
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
        log.error(f"Error uploading chunk: {str(e)}")
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail={
                "error": "CHUNK_UPLOAD_ERROR",
                "message": f"Failed to upload chunk: {str(e)}",
                "status": "error"
            }
        )


@router.post("/upload-finalize")
async def finalize_chunked_upload(
    upload_id: str = Query(...),
    file_name: str = Query(...),
    datasource: DataSource = Query(...),
    background_tasks: BackgroundTasks = BackgroundTasks(),
    db: Session = Depends(get_db)
):
    """
    Reassemble chunks into final file and process.
    Called after all chunks have been uploaded.
    """
    try:
        service = serviceMap.get(datasource)
        if not service or not isinstance(service, DataSourceService):
            raise HTTPException(
                status_code=400,
                detail={
                    "error": "SERVICE_NOT_FOUND",
                    "message": f"No service available for DataSource: {datasource.name}",
                    "status": "error"
                }
            )
        
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
        
        # Reassemble file
        final_path = os.path.join(service.directory_path, file_name)
        os.makedirs(os.path.dirname(final_path), exist_ok=True)
        
        log.info(f"🔗 Reassembling {len(chunk_files)} chunks into {file_name}")
        
        with open(final_path, "wb") as outfile:
            for chunk_path in chunk_files:
                with open(chunk_path, "rb") as chunk_file:
                    shutil.copyfileobj(chunk_file, outfile)
        
        # Cleanup chunks
        try:
            shutil.rmtree(chunk_dir)
            log.info(f"🧹 Cleaned up chunks for upload_id={upload_id}")
        except Exception as e:
            log.warning(f"Failed to cleanup chunks: {e}")
        
        # Process in background
        log.info(f"🔄 Starting background processing for {datasource.name}...")
        background_tasks.add_task(process_upload_background, datasource, [final_path], "Anonymous")
        
        return {
            "status": 200,
            "message": f"{datasource.name} file reassembled and processing started",
            "data": None
        }
    except HTTPException as he:
        raise he
    except Exception as e:
        log.error(f"Error finalizing chunked upload: {str(e)}")
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail={
                "error": "FINALIZE_ERROR",
                "message": f"Failed to finalize upload: {str(e)}",
                "status": "error"
            }
        )


# Register chunked upload endpoints on alias router too
@alias_router.post("/upload-chunk")
async def upload_chunk_alias(
    chunk: UploadFile = File(...),
    chunk_index: int = Form(...),
    total_chunks: int = Form(...),
    upload_id: str = Form(...),
    file_name: str = Form(...),
    datasource: DataSource = Query(...)
):
    """Upload chunk - Frontend alias endpoint"""
    return await upload_chunk(chunk, chunk_index, total_chunks, upload_id, file_name, datasource)


@alias_router.post("/upload-finalize")
async def finalize_chunked_upload_alias(
    upload_id: str = Query(...),
    file_name: str = Query(...),
    datasource: DataSource = Query(...),
    background_tasks: BackgroundTasks = BackgroundTasks(),
    db: Session = Depends(get_db)
):
    """Finalize chunked upload - Frontend alias endpoint"""
    return await finalize_chunked_upload(upload_id, file_name, datasource, background_tasks, db)

