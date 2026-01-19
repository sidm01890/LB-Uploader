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
from app.controllers.db_setup_controller import DBSetupController

logger = logging.getLogger(__name__)

# Initialize router
router = APIRouter()  # Main API router with /api prefix

# Initialize controllers
data_controller = DataController()
db_setup_controller = DBSetupController()


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
# DATABASE SETUP ROUTES
# ============================================================================

class CreateCollectionRequest(BaseModel):
    """Request model for creating a MongoDB collection"""
    collection_name: str = Field(
        ...,
        description="Name of the collection to create (will be converted to lowercase)",
        example="Zomato",
        min_length=1
    )
    
    class Config:
        json_schema_extra = {
            "example": {
                "collection_name": "Zomato"
            }
        }


class CreateCollectionResponse(BaseModel):
    """Response model for collection creation"""
    status: int = Field(..., description="HTTP status code", example=200)
    message: str = Field(..., description="Response message", example="Collection 'zomato' created successfully")
    data: Dict[str, Any] = Field(
        ...,
        description="Response data",
        example={
            "collection_name": "zomato",
            "mongodb_connected": True
        }
    )


@router.post(
    "/uploader/setup/new",
    tags=["Database Setup"],
    summary="Create new MongoDB collection",
    description="Create a new MongoDB collection in the database. The collection name will be automatically converted to lowercase. Returns an error if the collection already exists.",
    response_model=CreateCollectionResponse,
    status_code=status.HTTP_200_OK,
    responses={
        200: {
            "description": "Collection created successfully",
            "content": {
                "application/json": {
                    "example": {
                        "status": 200,
                        "message": "Collection 'zomato' created successfully",
                        "data": {
                            "collection_name": "zomato",
                            "mongodb_connected": True
                        }
                    }
                }
            }
        },
        409: {
            "description": "Collection already exists",
            "content": {
                "application/json": {
                    "example": {
                        "detail": "Collection 'zomato' already exists"
                    }
                }
            }
        },
        503: {
            "description": "MongoDB connection error",
            "content": {
                "application/json": {
                    "example": {
                        "detail": "MongoDB connection error: MongoDB is not connected"
                    }
                }
            }
        }
    }
)
async def create_collection(request: CreateCollectionRequest = Body(...)):
    """
    Create a new MongoDB collection.
    
    **Features:**
    - Collection name is automatically converted to lowercase (e.g., "Zomato" → "zomato")
    - Returns error if collection already exists
    - Validates MongoDB connection before creating collection
    
    **Request Body:**
    ```json
    {
        "collection_name": "Zomato"
    }
    ```
    
    **Success Response (200):**
    ```json
    {
        "status": 200,
        "message": "Collection 'zomato' created successfully",
        "data": {
            "collection_name": "zomato",
            "mongodb_connected": true
        }
    }
    ```
    
    **Error Responses:**
    - **409 Conflict**: Collection already exists
    - **503 Service Unavailable**: MongoDB not connected
    - **500 Internal Server Error**: Other errors
    """
    return await db_setup_controller.create_collection(request.collection_name)


class ListCollectionsResponse(BaseModel):
    """Response model for listing collections"""
    status: int = Field(..., description="HTTP status code", example=200)
    message: str = Field(..., description="Response message", example="Found 2 collection(s)")
    data: Dict[str, Any] = Field(
        ...,
        description="Response data",
        example={
            "collections": ["swiggy", "zomato"],
            "count": 2,
            "mongodb_connected": True
        }
    )


@router.get(
    "/uploader/setup/collections",
    tags=["Database Setup"],
    summary="List all MongoDB collections",
    description="Get a list of all collection names in the MongoDB database. Returns an empty array if no collections exist or if MongoDB is not connected.",
    response_model=ListCollectionsResponse,
    status_code=status.HTTP_200_OK,
    responses={
        200: {
            "description": "List of collections retrieved successfully",
            "content": {
                "application/json": {
                    "example": {
                        "status": 200,
                        "message": "Found 2 collection(s)",
                        "data": {
                            "collections": ["swiggy", "zomato"],
                            "count": 2,
                            "mongodb_connected": True
                        }
                    }
                }
            }
        },
        500: {
            "description": "Internal server error",
            "content": {
                "application/json": {
                    "example": {
                        "detail": "Failed to list collections: <error message>"
                    }
                }
            }
        }
    }
)
async def list_all_collections():
    """
    Get all MongoDB collection names.
    
    **Features:**
    - Returns all collection names in the database
    - Collections are returned in alphabetical order
    - System collections (starting with "system.") are excluded
    - Returns empty array if no collections exist or MongoDB is not connected
    
    **Success Response (200):**
    ```json
    {
        "status": 200,
        "message": "Found 2 collection(s)",
        "data": {
            "collections": ["swiggy", "zomato"],
            "count": 2,
            "mongodb_connected": true
        }
    }
    ```
    
    **Note:** The actual response will reflect the current collections in your MongoDB database.
    
    **Note:** If MongoDB is not connected, the `collections` array will be empty and `mongodb_connected` will be `false`.
    """
    return await db_setup_controller.list_all_collections()


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


# ============================================================================
# COLLECTION PROCESSING ROUTES
# ============================================================================

@router.post(
    "/process-collections",
    tags=["Collection Processing"],
    summary="Process collections manually",
    description="Manually trigger processing of collections to calculate and save processed data. Processes all collections or a specific one, then calculates formulas.",
    response_description="Processing status and results"
)
async def process_collections(
    collection_name: Optional[str] = Query(
        None,
        description="Optional specific collection name to process. If not provided, processes all collections.",
        example="zomato"
    )
):
    """
    Manually trigger collection processing and formula calculations.
    
    **What it does:**
    1. Processes data from collections (e.g., `zomato`, `pos`) 
    2. Applies field mappings and data sanitization
    3. Saves processed data to `{collection_name}_processed` collections
    4. Calculates formulas from the formulas collection
    5. Saves calculated results to report collections
    
    **Parameters:**
    - `collection_name` (optional): Process only this collection. If omitted, processes all collections.
    
    **Example:**
    ```bash
    # Process all collections and calculate formulas
    curl -X POST 'http://localhost:8010/api/process-collections'
    
    # Process specific collection and calculate formulas
    curl -X POST 'http://localhost:8010/api/process-collections?collection_name=zomato'
    ```
    """
    from app.controllers.scheduled_jobs_controller import ScheduledJobsController
    
    try:
        scheduled_jobs_controller = ScheduledJobsController()
        
        # Step 1: Process collections
        result = await scheduled_jobs_controller.process_collection_data(collection_name=collection_name)
        
        # Step 2: Calculate formulas
        formula_result = await scheduled_jobs_controller.process_formula_calculations(report_name=None)
        
        return {
            "status": 200,
            "message": "Collections processed and formulas calculated",
            "data": {
                "collection_processing": result.get("data", {}),
                "formula_calculations": formula_result.get("data", {})
            }
        }
    except Exception as e:
        logger.error(f"Error processing collections: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to process collections: {str(e)}"
        )


@router.post(
    "/process-formulas",
    tags=["Collection Processing"],
    summary="Calculate formulas manually",
    description="Manually trigger formula calculations from the formulas collection. Processes all formulas or a specific report.",
    response_description="Formula calculation status and results"
)
async def process_formulas(
    report_name: Optional[str] = Query(
        None,
        description="Optional specific report name to process. If not provided, processes all reports.",
        example="zomato_vs_pos"
    )
):
    """
    Manually trigger formula calculations.
    
    **What it does:**
    - Reads formulas from the formulas collection
    - Evaluates formulas using data from processed collections
    - Calculates delta columns and reasons if configured
    - Saves calculated results to report collections
    
    **Parameters:**
    - `report_name` (optional): Process only this report. If omitted, processes all reports.
    
    **Example:**
    ```bash
    # Calculate all formulas
    curl -X POST 'http://localhost:8010/api/process-formulas'
    
    # Calculate formulas for specific report
    curl -X POST 'http://localhost:8010/api/process-formulas?report_name=zomato_vs_pos'
    ```
    """
    from app.controllers.scheduled_jobs_controller import ScheduledJobsController
    
    try:
        scheduled_jobs_controller = ScheduledJobsController()
        result = await scheduled_jobs_controller.process_formula_calculations(report_name=report_name)
        
        return result
    except Exception as e:
        logger.error(f"Error processing formulas: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to process formulas: {str(e)}"
        )
