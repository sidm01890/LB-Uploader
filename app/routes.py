"""
Routes module - File Upload Routes Only
Handles Excel/CSV file uploads and storage on server for further processing
"""

from fastapi import APIRouter, UploadFile, Form, Query, File, BackgroundTasks, Body, status
from fastapi.responses import JSONResponse
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field
import logging

# Import controllers
from app.controllers.data_controller import DataController

logger = logging.getLogger(__name__)

# Initialize router
router = APIRouter()  # Main API router with /api prefix

# Initialize controllers
data_controller = DataController()


# ============================================================================
# FILE UPLOAD ROUTES
# ============================================================================

@router.post(
    "/upload",
    tags=["File Upload"],
    summary="Upload Excel/CSV files",
    description="Upload one or more Excel/CSV files and store them on the server for further processing. Files are saved to `data/uploads/{datasource}/` directory.",
    response_description="Upload status and file information"
)
async def upload_files(
    datasource: str = Query(
        ...,
        description="Data source identifier (e.g., ZOMATO, POS_ORDERS, TRM, MPR)",
        example="ZOMATO"
    ),
    background_tasks: BackgroundTasks = BackgroundTasks(),
    files: List[UploadFile] = File(
        ...,
        description="One or more Excel/CSV files to upload"
    )
):
    """
    Upload Excel/CSV files and store on server for further processing.
    
    **Features:**
    - Supports multiple file uploads in a single request
    - Files are automatically saved to `data/uploads/{datasource}/` directory
    - File metadata is stored in MongoDB
    - Email notifications are sent after successful upload
    
    **Example:**
    ```bash
    curl -X POST 'http://localhost:8080/api/upload?datasource=ZOMATO' \\
         -F 'files=@file1.xlsx' -F 'files=@file2.csv'
    ```
    """
    return await data_controller.upload_data(datasource, background_tasks, files)


# ============================================================================
# CHUNKED UPLOAD ROUTES (for large files)
# ============================================================================

@router.post(
    "/upload-chunk",
    tags=["File Upload"],
    summary="Upload file chunk",
    description="Receive and store a single chunk of a file. Used for chunked uploads when files are too large to upload in one request.",
    response_description="Chunk upload status"
)
async def upload_chunk(
    chunk: UploadFile = File(..., description="File chunk data"),
    chunk_index: int = Form(..., description="Current chunk index (0-based)", example=0),
    total_chunks: int = Form(..., description="Total number of chunks", example=10),
    upload_id: str = Form(..., description="Unique upload identifier for this file", example="550e8400-e29b-41d4-a716-446655440000"),
    file_name: str = Form(..., description="Original file name", example="large_file.xlsx"),
    datasource: str = Query(..., description="Data source identifier", example="ZOMATO")
):
    """
    Receive and store a single chunk of a file (for large Excel/CSV files).
    
    **Usage:**
    1. Split your file into chunks
    2. Upload each chunk sequentially using this endpoint
    3. Call `/upload-finalize` after all chunks are uploaded
    
    **Note:** All chunks must use the same `upload_id` and `file_name`.
    """
    return await data_controller.upload_chunk(chunk, chunk_index, total_chunks, upload_id, file_name, datasource)


@router.post(
    "/upload-finalize",
    tags=["File Upload"],
    summary="Finalize chunked upload",
    description="Reassemble all uploaded chunks into the final file and store it on the server. Call this endpoint after all chunks have been uploaded via `/upload-chunk`.",
    response_description="Finalized upload status"
)
async def finalize_chunked_upload(
    upload_id: str = Query(..., description="Unique upload identifier used during chunk uploads", example="550e8400-e29b-41d4-a716-446655440000"),
    file_name: str = Query(..., description="Original file name", example="large_file.xlsx"),
    datasource: str = Query(..., description="Data source identifier", example="ZOMATO"),
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    """
    Reassemble chunks into final file and store on server.
    
    **Usage:**
    1. Upload all chunks using `/upload-chunk`
    2. Call this endpoint with the same `upload_id` and `file_name`
    3. The server will reassemble all chunks and save the complete file
    
    **Note:** This endpoint must be called after all chunks are uploaded.
    """
    return await data_controller.finalize_chunked_upload(upload_id, file_name, datasource, background_tasks)


# ============================================================================
# HEALTH CHECK ROUTES
# ============================================================================

@router.get(
    "/health",
    tags=["Health Check"],
    summary="Health check",
    description="Check the health status of the service and MongoDB connection",
    response_description="Service health status and MongoDB connection information"
)
async def health_check():
    """
    Health check endpoint - checks MongoDB connection and service status.
    
    **Returns:**
    - Service status
    - MongoDB connection status
    - MongoDB configuration details (host, port, database)
    
    **Use Cases:**
    - Monitoring and alerting
    - Load balancer health checks
    - Service discovery
    """
    from app.services.mongodb_service import mongodb_service
    from app.core.config import config
    
    mongodb_status = "connected" if mongodb_service.is_connected() else "disconnected"
    
    return {
        "status": "healthy",
        "service": "File Upload Service",
        "mongodb": {
            "status": mongodb_status,
            "host": config.mongodb.host,
            "port": config.mongodb.port,
            "database": config.mongodb.database
        }
    }
