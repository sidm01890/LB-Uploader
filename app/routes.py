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
from app.controllers.formulas_controller import FormulasController

logger = logging.getLogger(__name__)

# Initialize router
router = APIRouter()  # Main API router with /api prefix

# Initialize controllers
data_controller = DataController()
db_setup_controller = DBSetupController()
formulas_controller = FormulasController()


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
        example="zomato",
        min_length=1
    )
    unique_ids: List[str] = Field(
        default_factory=list,
        description="List of field names that form unique identifiers for this collection (can be empty array)",
        example=["order_id", "order_date"]
    )
    
    class Config:
        json_schema_extra = {
            "example": {
                "collection_name": "zomato",
                "unique_ids": ["order_id", "order_date"]
            }
        }


class CreateCollectionResponse(BaseModel):
    """Response model for collection creation"""
    status: int = Field(..., description="HTTP status code", example=200)
    message: str = Field(..., description="Response message", example="Collection 'zomato' and processed collection 'zomato_processed' created successfully")
    data: Dict[str, Any] = Field(
        ...,
        description="Response data",
        example={
            "collection_name": "zomato",
            "processed_collection_name": "zomato_processed",
            "unique_ids": ["order_id", "order_date"],
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
        "collection_name": "zomato",
        "unique_ids": ["order_id", "order_date"]
    }
    ```
    
    **Note:** `unique_ids` is optional and can be an empty array `[]`.
    
    **Success Response (200):**
    ```json
    {
        "status": 200,
        "message": "Collection 'zomato' and processed collection 'zomato_processed' created successfully",
        "data": {
            "collection_name": "zomato",
            "processed_collection_name": "zomato_processed",
            "unique_ids": ["order_id", "order_date"],
            "mongodb_connected": true
        }
    }
    ```
    
    **Features:**
    - Automatically creates both the main collection and a processed version (e.g., `zomato` → `zomato_processed`)
    - Stores unique_ids for later use in deduplication/processing
    - Both collections are created even if unique_ids is empty
    
    **Error Responses:**
    - **409 Conflict**: Collection already exists
    - **503 Service Unavailable**: MongoDB not connected
    - **500 Internal Server Error**: Other errors
    """
    return await db_setup_controller.create_collection(
        request.collection_name,
        request.unique_ids
    )


class UpdateUniqueIdsRequest(BaseModel):
    """Request model for updating unique_ids"""
    collection_name: str = Field(
        ...,
        description="Name of the collection (will be converted to lowercase)",
        example="zomato",
        min_length=1
    )
    unique_ids: List[str] = Field(
        ...,
        description="List of field names that form unique identifiers for this collection",
        example=["order_id", "order_date"]
    )
    
    class Config:
        json_schema_extra = {
            "example": {
                "collection_name": "zomato",
                "unique_ids": ["order_id", "order_date"]
            }
        }


class UpdateUniqueIdsResponse(BaseModel):
    """Response model for updating unique_ids"""
    status: int = Field(..., description="HTTP status code", example=200)
    message: str = Field(..., description="Response message", example="Unique IDs updated successfully for collection 'zomato'")
    data: Dict[str, Any] = Field(
        ...,
        description="Response data",
        example={
            "collection_name": "zomato",
            "unique_ids": ["order_id", "order_date"],
            "mongodb_connected": True
        }
    )


@router.put(
    "/uploader/setup/new",
    tags=["Database Setup"],
    summary="Update unique_ids for a collection",
    description="Update the unique_ids field for an existing collection in raw_data_collection. The collection must already exist.",
    response_model=UpdateUniqueIdsResponse,
    status_code=status.HTTP_200_OK,
    responses={
        200: {
            "description": "Unique IDs updated successfully",
            "content": {
                "application/json": {
                    "example": {
                        "status": 200,
                        "message": "Unique IDs updated successfully for collection 'zomato'",
                        "data": {
                            "collection_name": "zomato",
                            "unique_ids": ["order_id", "order_date"],
                            "mongodb_connected": True
                        }
                    }
                }
            }
        },
        404: {
            "description": "Collection not found",
            "content": {
                "application/json": {
                    "example": {
                        "detail": "Collection 'zomato' not found in raw_data_collection. Please create the collection first."
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
async def update_collection_unique_ids(request: UpdateUniqueIdsRequest = Body(...)):
    """
    Update unique_ids for an existing collection.
    
    **Features:**
    - Updates the unique_ids field in raw_data_collection
    - Collection name is automatically converted to lowercase
    - Collection must already exist (created via POST /uploader/setup/new)
    
    **Request Body:**
    ```json
    {
        "collection_name": "zomato",
        "unique_ids": ["order_id", "order_date"]
    }
    ```
    
    **Success Response (200):**
    ```json
    {
        "status": 200,
        "message": "Unique IDs updated successfully for collection 'zomato'",
        "data": {
            "collection_name": "zomato",
            "unique_ids": ["order_id", "order_date"],
            "mongodb_connected": true
        }
    }
    ```
    
    **Error Responses:**
    - **404 Not Found**: Collection doesn't exist in raw_data_collection
    - **503 Service Unavailable**: MongoDB not connected
    - **500 Internal Server Error**: Other errors
    
    **Note:** This endpoint only updates the unique_ids field. It does not create a new collection. Use POST /uploader/setup/new to create a new collection.
    """
    return await db_setup_controller.update_collection_unique_ids(
        request.collection_name,
        request.unique_ids
    )


class GetUniqueIdsResponse(BaseModel):
    """Response model for getting unique_ids"""
    status: int = Field(..., description="HTTP status code", example=200)
    message: str = Field(..., description="Response message", example="Unique IDs retrieved successfully for collection 'zomato'")
    data: Dict[str, Any] = Field(
        ...,
        description="Response data",
        example={
            "collection_name": "zomato",
            "unique_ids": ["order_id", "order_date"],
            "unique_ids_count": 2,
            "mongodb_connected": True
        }
    )


@router.get(
    "/uploader/setup/new/{collection_name}",
    tags=["Database Setup"],
    summary="Get unique_ids for a collection",
    description="Retrieve the unique_ids field for an existing collection from raw_data_collection.",
    response_model=GetUniqueIdsResponse,
    status_code=status.HTTP_200_OK,
    responses={
        200: {
            "description": "Unique IDs retrieved successfully",
            "content": {
                "application/json": {
                    "example": {
                        "status": 200,
                        "message": "Unique IDs retrieved successfully for collection 'zomato'",
                        "data": {
                            "collection_name": "zomato",
                            "unique_ids": ["order_id", "order_date"],
                            "unique_ids_count": 2,
                            "mongodb_connected": True
                        }
                    }
                }
            }
        },
        404: {
            "description": "Collection not found",
            "content": {
                "application/json": {
                    "example": {
                        "detail": "Collection 'zomato' not found in raw_data_collection"
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
async def get_collection_unique_ids(collection_name: str):
    """
    Get unique_ids for an existing collection.
    
    **Features:**
    - Retrieves the unique_ids field from raw_data_collection
    - Collection name is automatically converted to lowercase
    - Returns empty array if unique_ids is not set
    
    **Path Parameter:**
    - `collection_name`: Name of the collection (e.g., "zomato")
    
    **Success Response (200):**
    ```json
    {
        "status": 200,
        "message": "Unique IDs retrieved successfully for collection 'zomato'",
        "data": {
            "collection_name": "zomato",
            "unique_ids": ["order_id", "order_date"],
            "unique_ids_count": 2,
            "mongodb_connected": true
        }
    }
    ```
    
    **Error Responses:**
    - **404 Not Found**: Collection doesn't exist in raw_data_collection
    - **503 Service Unavailable**: MongoDB not connected
    - **500 Internal Server Error**: Other errors
    
    **Example:**
    ```
    GET /api/uploader/setup/new/zomato
    ```
    
    **Note:** This endpoint retrieves unique_ids from the raw_data_collection. If the collection was not created via POST /uploader/setup/new, it will return 404.
    """
    return await db_setup_controller.get_collection_unique_ids(collection_name)


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
    description="Get a list of all collection names from the raw_data_collection. Only collections created via the API are included. Returns an empty array if no collections exist or if MongoDB is not connected.",
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
    Get all MongoDB collection names from raw_data_collection.
    
    **Features:**
    - Returns only collections that were created via the API (stored in raw_data_collection)
    - Collections are returned in alphabetical order
    - Returns empty array if no collections exist or MongoDB is not connected
    - Only collections created through `/uploader/setup/new` endpoint are included
    
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
    
    **Note:** This endpoint only returns collections that were created via the API and are registered in `raw_data_collection`. Collections created manually or outside the API will not appear in this list.
    
    **Note:** If MongoDB is not connected, the `collections` array will be empty and `mongodb_connected` will be `false`.
    """
    return await db_setup_controller.list_all_collections()


class GetCollectionKeysRequest(BaseModel):
    """Request model for getting collection keys"""
    collection_name: str = Field(
        ...,
        description="Name of the collection to get keys from (will be converted to lowercase)",
        example="zomato",
        min_length=1
    )
    
    class Config:
        json_schema_extra = {
            "example": {
                "collection_name": "zomato"
            }
        }


class GetCollectionKeysResponse(BaseModel):
    """Response model for collection keys"""
    status: int = Field(..., description="HTTP status code", example=200)
    message: str = Field(..., description="Response message", example="Found 5 unique key(s) in collection 'zomato'")
    data: Dict[str, Any] = Field(
        ...,
        description="Response data",
        example={
            "collection_name": "zomato",
            "keys": ["order_id", "order_amount", "store_code"],
            "count": 3,
            "mongodb_connected": True
        }
    )


@router.post(
    "/uploader/setup/collection/keys",
    tags=["Database Setup"],
    summary="Get unique keys from a collection",
    description="Get a list of all unique keys/fields from documents in a MongoDB collection. Excludes system fields (_id, created_at, updated_at).",
    response_model=GetCollectionKeysResponse,
    status_code=status.HTTP_200_OK,
    responses={
        200: {
            "description": "Collection keys retrieved successfully",
            "content": {
                "application/json": {
                    "example": {
                        "status": 200,
                        "message": "Found 5 unique key(s) in collection 'zomato'",
                        "data": {
                            "collection_name": "zomato",
                            "keys": ["order_id", "order_amount", "store_code", "order_date", "customer_name"],
                            "count": 5,
                            "mongodb_connected": True
                        }
                    }
                }
            }
        },
        404: {
            "description": "Collection not found",
            "content": {
                "application/json": {
                    "example": {
                        "detail": "Collection 'zomato' does not exist"
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
async def get_collection_keys(request: GetCollectionKeysRequest = Body(...)):
    """
    Get unique keys from a MongoDB collection.
    
    **Features:**
    - Returns all unique keys/fields found in documents of the specified collection
    - Automatically excludes system fields: `_id`, `created_at`, `updated_at`
    - Collection name is converted to lowercase
    - Keys are returned in alphabetical order
    - Analyzes up to 1000 documents to extract keys
    
    **Request Body:**
    ```json
    {
        "collection_name": "zomato"
    }
    ```
    
    **Success Response (200):**
    ```json
    {
        "status": 200,
        "message": "Found 5 unique key(s) in collection 'zomato'",
        "data": {
            "collection_name": "zomato",
            "keys": ["order_id", "order_amount", "store_code", "order_date", "customer_name"],
            "count": 5,
            "mongodb_connected": true
        }
    }
    ```
    
    **Error Responses:**
    - **404 Not Found**: Collection doesn't exist
    - **503 Service Unavailable**: MongoDB not connected
    - **500 Internal Server Error**: Other errors
    
    **Note:** The keys are extracted from the first 1000 documents in the collection. If your collection has varying schemas, make sure the sample documents represent all possible keys.
    """
    return await db_setup_controller.get_collection_keys(request.collection_name)


# ============================================================================
# COLLECTION FIELD MAPPING ROUTES
# ============================================================================

class SaveFieldMappingRequest(BaseModel):
    """Request model for saving field mapping"""
    collection_name: str = Field(
        ...,
        description="Name of the collection (will be converted to lowercase)",
        example="zomato",
        min_length=1
    )
    selected_fields: List[str] = Field(
        ...,
        description="List of field names to use for this collection",
        example=["order_id", "order_amount", "store_code", "order_date"],
        min_items=1
    )
    
    class Config:
        json_schema_extra = {
            "example": {
                "collection_name": "zomato",
                "selected_fields": ["order_id", "order_amount", "store_code", "order_date", "customer_name"]
            }
        }


class SaveFieldMappingResponse(BaseModel):
    """Response model for saving field mapping"""
    status: int = Field(..., description="HTTP status code", example=200)
    message: str = Field(..., description="Response message", example="Field mapping created successfully for collection 'zomato'")
    data: Dict[str, Any] = Field(
        ...,
        description="Response data",
        example={
            "collection_name": "zomato",
            "selected_fields": ["order_id", "order_amount", "store_code"],
            "selected_fields_count": 3,
            "total_available_fields": 50,
            "mongodb_connected": True
        }
    )


@router.post(
    "/uploader/setup/collection/fields",
    tags=["Database Setup"],
    summary="Save field mapping for a collection",
    description="Save or update which fields to use from a collection. For example, if zomato has 50 columns but you only want 15, specify those 15 fields here.",
    response_model=SaveFieldMappingResponse,
    status_code=status.HTTP_200_OK,
    responses={
        200: {
            "description": "Field mapping saved successfully",
            "content": {
                "application/json": {
                    "example": {
                        "status": 200,
                        "message": "Field mapping created successfully for collection 'zomato'",
                        "data": {
                            "collection_name": "zomato",
                            "selected_fields": ["order_id", "order_amount", "store_code", "order_date"],
                            "selected_fields_count": 4,
                            "total_available_fields": 50,
                            "mongodb_connected": True
                        }
                    }
                }
            }
        },
        400: {
            "description": "Invalid request or fields don't exist in collection",
            "content": {
                "application/json": {
                    "example": {
                        "detail": "Invalid fields for collection 'zomato': invalid_field. Available fields: order_id, order_amount, store_code"
                    }
                }
            }
        },
        404: {
            "description": "Collection not found",
            "content": {
                "application/json": {
                    "example": {
                        "detail": "Collection 'zomato' does not exist"
                    }
                }
            }
        }
    }
)
async def save_collection_field_mapping(request: SaveFieldMappingRequest = Body(...)):
    """
    Save or update field mapping for a collection.
    
    **Use Case:**
    - If a collection (e.g., zomato) has 50 columns but you only need 15, use this API to specify which 15 fields to use.
    - The mapping is stored in `collection_field_mappings` collection.
    - If a mapping already exists, it will be updated.
    
    **Features:**
    - Validates that all selected fields exist in the collection
    - Automatically converts collection name to lowercase
    - Creates or updates the mapping as needed
    
    **Request Body:**
    ```json
    {
        "collection_name": "zomato",
        "selected_fields": ["order_id", "order_amount", "store_code", "order_date", "customer_name"]
    }
    ```
    
    **Success Response (200):**
    ```json
    {
        "status": 200,
        "message": "Field mapping created successfully for collection 'zomato'",
        "data": {
            "collection_name": "zomato",
            "selected_fields": ["order_id", "order_amount", "store_code", "order_date", "customer_name"],
            "selected_fields_count": 5,
            "total_available_fields": 50,
            "mongodb_connected": true
        }
    }
    ```
    
    **Error Responses:**
    - **400 Bad Request**: Invalid fields or empty selection
    - **404 Not Found**: Collection doesn't exist
    - **503 Service Unavailable**: MongoDB not connected
    """
    return await db_setup_controller.save_collection_field_mapping(
        request.collection_name,
        request.selected_fields
    )


@router.get(
    "/uploader/setup/collection/fields/{collection_name}",
    tags=["Database Setup"],
    summary="Get field mapping for a collection",
    description="Retrieve the saved field mapping for a specific collection.",
    status_code=status.HTTP_200_OK,
    responses={
        200: {
            "description": "Field mapping retrieved successfully",
            "content": {
                "application/json": {
                    "example": {
                        "status": 200,
                        "message": "Field mapping found for collection 'zomato'",
                        "data": {
                            "collection_name": "zomato",
                            "selected_fields": ["order_id", "order_amount", "store_code"],
                            "total_available_fields": 50,
                            "created_at": "2024-01-01T12:00:00",
                            "updated_at": "2024-01-01T12:00:00"
                        }
                    }
                }
            }
        },
        200: {
            "description": "Field mapping retrieved (empty if not found)",
            "content": {
                "application/json": {
                    "example": {
                        "status": 200,
                        "message": "No field mapping found for collection 'pos'",
                        "data": {
                            "collection_name": "pos",
                            "selected_fields": [],
                            "selected_fields_count": 0,
                            "total_available_fields": 0
                        }
                    }
                }
            }
        }
    }
)
async def get_collection_field_mapping(collection_name: str):
    """
    Get field mapping for a specific collection.
    
    **Returns:**
    - Collection name
    - Selected fields array (empty array if no mapping exists)
    - Total available fields count
    - Created and updated timestamps (if mapping exists)
    
    **Note:** If no field mapping exists for the collection, returns 200 with empty selected_fields array instead of 404.
    
    **Example:**
    ```
    GET /api/uploader/setup/collection/fields/zomato
    ```
    
    **Response when mapping exists:**
    ```json
    {
        "status": 200,
        "message": "Field mapping found for collection 'zomato'",
        "data": {
            "collection_name": "zomato",
            "selected_fields": ["order_id", "order_amount", "store_code"],
            "selected_fields_count": 3,
            "total_available_fields": 50,
            "created_at": "2024-01-01T12:00:00",
            "updated_at": "2024-01-01T12:00:00"
        }
    }
    ```
    
    **Response when no mapping exists:**
    ```json
    {
        "status": 200,
        "message": "No field mapping found for collection 'pos'",
        "data": {
            "collection_name": "pos",
            "selected_fields": [],
            "selected_fields_count": 0,
            "total_available_fields": 0
        }
    }
    ```
    """
    return await db_setup_controller.get_collection_field_mapping(collection_name)


@router.get(
    "/uploader/setup/collection/fields",
    tags=["Database Setup"],
    summary="List all field mappings",
    description="Get a list of all field mappings for all collections.",
    status_code=status.HTTP_200_OK
)
async def list_all_field_mappings():
    """
    List all field mappings for all collections.
    
    **Returns:**
    - List of all field mappings
    - Total count of mappings
    - MongoDB connection status
    
    **Use Case:**
    - View all configured field mappings at once
    - Audit which collections have mappings configured
    """
    return await db_setup_controller.list_all_field_mappings()


class ListUploadedFilesResponse(BaseModel):
    """Response model for listing uploaded files"""
    status: int = Field(..., description="HTTP status code", example=200)
    message: str = Field(..., description="Response message", example="Found 5 uploaded file(s)")
    data: Dict[str, Any] = Field(
        ...,
        description="Response data",
        example={
            "uploaded_files": [
                {
                    "upload_id": "abc123",
                    "filename": "zomato_data.xlsx",
                    "datasource": "ZOMATO",
                    "file_path": "/path/to/file.xlsx",
                    "file_size": 1024000,
                    "status": "processed",
                    "uploaded_at": "2024-01-15T10:00:00",
                    "processed_at": "2024-01-15T10:05:00"
                }
            ],
            "count": 5,
            "mongodb_connected": True
        }
    )


@router.get(
    "/uploader/setup/uploaded-files",
    tags=["Database Setup"],
    summary="List all uploaded files",
    description="Get all data from the uploaded_files collection. Returns all file upload records with their metadata, status, and processing information.",
    response_model=ListUploadedFilesResponse,
    status_code=status.HTTP_200_OK,
    responses={
        200: {
            "description": "Uploaded files retrieved successfully",
            "content": {
                "application/json": {
                    "example": {
                        "status": 200,
                        "message": "Found 5 uploaded file(s)",
                        "data": {
                            "uploaded_files": [
                                {
                                    "upload_id": "abc123",
                                    "filename": "zomato_data.xlsx",
                                    "datasource": "ZOMATO",
                                    "file_path": "/path/to/file.xlsx",
                                    "file_size": 1024000,
                                    "status": "processed",
                                    "uploaded_at": "2024-01-15T10:00:00",
                                    "processed_at": "2024-01-15T10:05:00",
                                    "documents_processed": 800000
                                }
                            ],
                            "count": 5,
                            "mongodb_connected": True
                        }
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
async def list_all_uploaded_files():
    """
    Get all data from the uploaded_files collection.
    
    **Features:**
    - Returns all uploaded file records
    - Includes file metadata, status, and processing information
    - Files are sorted by upload time (newest first)
    - Returns empty array if no files exist or MongoDB is not connected
    
    **Success Response (200):**
    ```json
    {
        "status": 200,
        "message": "Found 5 uploaded file(s)",
        "data": {
            "uploaded_files": [
                {
                    "upload_id": "abc123",
                    "filename": "zomato_data.xlsx",
                    "datasource": "ZOMATO",
                    "file_path": "/path/to/file.xlsx",
                    "file_size": 1024000,
                    "file_type": "xlsx",
                    "status": "processed",
                    "uploaded_at": "2024-01-15T10:00:00",
                    "uploaded_by": "api_user",
                    "processed_at": "2024-01-15T10:05:00",
                    "documents_processed": 800000,
                    "created_at": "2024-01-15T10:00:00",
                    "updated_at": "2024-01-15T10:05:00"
                }
            ],
            "count": 5,
            "mongodb_connected": true
        }
    }
    ```
    
    **Note:** If MongoDB is not connected, the `uploaded_files` array will be empty and `mongodb_connected` will be `false`.
    """
    return await db_setup_controller.list_all_uploaded_files()


# ============================================================================
# REPORT FORMULAS ROUTES
# ============================================================================

class FormulaField(BaseModel):
    """Model for a single field in a formula"""
    type: str = Field(..., description="Field type (data_field, operator, etc.)", example="data_field")
    dataset_type: str = Field(default="Dataset", description="Dataset type", example="Dataset")
    selectedDataSetValue: str = Field(default="", description="Selected dataset value", example="zvd")
    selectedFieldValue: str = Field(default="", description="Selected field value", example="zomato")
    customFieldValue: str = Field(default="", description="Custom field value", example="")
    startBrackets: List[str] = Field(default_factory=list, description="Start brackets", example=[])
    endBrackets: List[str] = Field(default_factory=list, description="End brackets", example=[])
    selectedTableName: str = Field(default="", description="Selected table name", example="zomato")
    selectedTableColumn: str = Field(default="", description="Selected table column", example="zvd")


class ConditionItem(BaseModel):
    """Model for a single condition"""
    column: str = Field(..., description="Column name to apply condition on", example="order_id")
    operator: str = Field(..., description="Comparison operator (equal, not_equal, greater_than, less_than, etc.)", example="not_equal")
    value: Any = Field(..., description="Value to compare against", example="NULL")


class FormulaConditionItem(BaseModel):
    """Model for a single formula condition"""
    conditionType: str = Field(..., description="Type of condition (e.g., 'between')", example="between")
    value1: Optional[str] = Field(default=None, description="First value for condition", example="10")
    value2: Optional[str] = Field(default=None, description="Second value for condition (used in 'between' type)", example="200")
    formulaValue: Optional[str] = Field(default=None, description="Formula value for condition", example="50")


class FormulaItem(BaseModel):
    """Model for a single formula"""
    id: int = Field(..., description="Formula ID", example=1)
    logicName: str = Field(..., description="Logic name of the formula", example="Total Amount")
    fields: List[FormulaField] = Field(..., description="List of fields in the formula", min_items=1)
    formulaText: str = Field(..., description="Formula text expression", example="zomato.zvd + zomato.merchant_pack_charge")
    logicNameKey: str = Field(..., description="Logic name key", example="TOTAL_AMOUNT")
    multipleColumn: bool = Field(default=False, description="Whether formula uses multiple columns")
    conditions: List[FormulaConditionItem] = Field(default_factory=list, description="List of conditions for this formula", example=[])
    active_group_index: int = Field(default=0, description="Active group index", example=0)
    excelFormulaText: str = Field(default="", description="Excel formula text", example=" zomato.zvd + zomato.merchant_pack_charge")
    
    class Config:
        json_schema_extra = {
            "example": {
                "id": 1,
                "logicName": "Total Amount",
                "fields": [
                    {
                        "type": "data_field",
                        "dataset_type": "Dataset",
                        "selectedDataSetValue": "zvd",
                        "selectedFieldValue": "zomato",
                        "customFieldValue": "",
                        "startBrackets": [],
                        "endBrackets": [],
                        "selectedTableName": "zomato",
                        "selectedTableColumn": "zvd"
                    }
                ],
                "formulaText": "zomato.zvd + zomato.merchant_pack_charge",
                "logicNameKey": "TOTAL_AMOUNT",
                "multipleColumn": False,
                "conditions": [
                    {
                        "conditionType": "between",
                        "value1": "10",
                        "value2": "200",
                        "formulaValue": "50"
                    }
                ],
                "active_group_index": 0,
                "excelFormulaText": " zomato.zvd + zomato.merchant_pack_charge"
            }
        }


class SaveReportFormulasRequest(BaseModel):
    """Request model for saving report formulas"""
    report_name: str = Field(
        ...,
        description="Name of the report (will be used as collection name, converted to lowercase)",
        example="zomato_vs_pos_summary",
        min_length=1
    )
    formulas: List[FormulaItem] = Field(
        ...,
        description="List of formulas with logicName, formulaText, fields, etc. (can be empty)",
        default_factory=list
    )
    mapping_keys: Dict[str, List[str]] = Field(
        default_factory=dict,
        description="Mapping keys for joining collections (e.g., {'zomato': ['order_id', 'order_date'], 'pos': ['paytm_counter']})"
    )
    conditions: Dict[str, List[ConditionItem]] = Field(
        default_factory=dict,
        description="Conditions to filter data from collections (e.g., {'zomato': [{'column': 'order_id', 'operator': 'not_equal', 'value': 'NULL'}]})"
    )
    
    class Config:
        json_schema_extra = {
            "example": {
                "report_name": "zomato_vs_pos_summary",
                "formulas": [
                    {
                        "id": 1,
                        "logicName": "Total Amount",
                        "fields": [
                            {
                                "type": "data_field",
                                "dataset_type": "Dataset",
                                "selectedDataSetValue": "zvd",
                                "selectedFieldValue": "zomato",
                                "customFieldValue": "",
                                "startBrackets": [],
                                "endBrackets": [],
                                "selectedTableName": "zomato",
                                "selectedTableColumn": "zvd"
                            },
                            {
                                "type": "operator",
                                "dataset_type": "Dataset",
                                "selectedFieldValue": "+",
                                "selectedTableName": ""
                            },
                            {
                                "type": "data_field",
                                "dataset_type": "Dataset",
                                "selectedDataSetValue": "merchant_pack_charge",
                                "selectedFieldValue": "zomato",
                                "customFieldValue": "",
                                "startBrackets": [],
                                "endBrackets": [],
                                "selectedTableName": "zomato",
                                "selectedTableColumn": "merchant_pack_charge"
                            }
                        ],
                        "formulaText": "zomato.zvd + zomato.merchant_pack_charge",
                        "logicNameKey": "TOTAL_AMOUNT",
                        "multipleColumn": False,
                        "active_group_index": 0,
                        "excelFormulaText": " zomato.zvd + zomato.merchant_pack_charge"
                    }
                ],
                "mapping_keys": {
                    "zomato": ["order_id", "order_date"],
                    "pos": ["paytm_counter"]
                },
                "conditions": {
                    "zomato": [
                        {
                            "column": "order_id",
                            "operator": "not_equal",
                            "value": "NULL"
                        }
                    ],
                    "pos": [
                        {
                            "column": "sales_type",
                            "operator": "equal",
                            "value": "ZOMATO"
                        }
                    ]
                }
            }
        }


class SaveReportFormulasResponse(BaseModel):
    """Response model for saving report formulas"""
    status: int = Field(..., description="HTTP status code", example=200)
    message: str = Field(..., description="Response message", example="Report formulas created successfully in collection 'zomato_vs_pos_summary'")
    data: Dict[str, Any] = Field(
        ...,
        description="Response data",
        example={
            "report_name": "zomato_vs_pos_summary",
            "formulas_count": 1,
            "formulas": [
                {
                    "id": 1,
                    "logicName": "Total Amount",
                    "fields": [
                        {
                            "type": "data_field",
                            "dataset_type": "Dataset",
                            "selectedDataSetValue": "zvd",
                            "selectedFieldValue": "zomato",
                            "customFieldValue": "",
                            "startBrackets": [],
                            "endBrackets": [],
                            "selectedTableName": "zomato",
                            "selectedTableColumn": "zvd"
                        }
                    ],
                    "formulaText": "zomato.zvd + zomato.merchant_pack_charge",
                    "logicNameKey": "TOTAL_AMOUNT",
                    "multipleColumn": False,
                    "active_group_index": 0,
                    "excelFormulaText": " zomato.zvd + zomato.merchant_pack_charge"
                }
            ],
            "mapping_keys": {
                "zomato": ["order_id", "order_date"],
                "pos": ["paytm_counter"]
            },
            "conditions": {
                "zomato": [
                    {
                        "column": "order_id",
                        "operator": "not_equal",
                        "value": "NULL"
                    }
                ],
                "pos": [
                    {
                        "column": "sales_type",
                        "operator": "equal",
                        "value": "ZOMATO"
                    }
                ]
            },
            "collection_existed": False,
            "mongodb_connected": True
        }
    )


@router.post(
    "/uploader/reports/formulas",
    tags=["Report Formulas"],
    summary="Save report formulas",
    description="Save report formulas to a MongoDB collection. If the collection doesn't exist, it will be created automatically. If it exists, the formulas will be updated.",
    response_model=SaveReportFormulasResponse,
    status_code=status.HTTP_200_OK,
    responses={
        200: {
            "description": "Report formulas saved successfully",
            "content": {
                "application/json": {
                    "example": {
                        "status": 200,
                        "message": "Report formulas created successfully in collection 'zomato_vs_pos_summary'",
                        "data": {
                            "report_name": "zomato_vs_pos_summary",
                            "formulas_count": 4,
                            "formulas": [
                                {"formula_name": "pos_order_id", "formula_value": "pos.order_id"},
                                {"formula_name": "zomato_order_id", "formula_value": "zomato.order_id"},
                                {"formula_name": "pos_net_amount", "formula_value": "pos.net_amount"},
                                {"formula_name": "zomato_net_amount", "formula_value": "zomato.sub_total - zomato.mvd + zomato.gst"}
                            ],
                            "collection_existed": False,
                            "mongodb_connected": True
                        }
                    }
                }
            }
        },
        400: {
            "description": "Invalid request",
            "content": {
                "application/json": {
                    "example": {
                        "detail": "At least one formula is required"
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
async def save_report_formulas(request: SaveReportFormulasRequest = Body(...)):
    """
    Save report formulas to the 'formulas' collection.
    
    **Features:**
    - Saves all formulas to a single 'formulas' collection
    - Automatically creates the 'formulas' collection if it doesn't exist
    - Updates existing formulas if a document with the report name already exists
    - Creates a new document if the report doesn't exist
    - Report name is converted to lowercase
    - Validates that all formulas have required fields (if provided)
    - Allows empty formulas array to clear all formulas for a report
    
    **Request Body:**
    ```json
    {
        "report_name": "zomato_vs_pos_summary",
        "formulas": [
            {
                "formula_name": "pos_order_id",
                "formula_value": "pos.order_id"
            },
            {
                "formula_name": "zomato_order_id",
                "formula_value": "zomato.order_id"
            },
            {
                "formula_name": "pos_net_amount",
                "formula_value": "pos.net_amount"
            },
            {
                "formula_name": "zomato_net_amount",
                "formula_value": "zomato.sub_total - zomato.mvd + zomato.gst"
            }
        ]
    }
    ```
    
    **Note:** The `formulas` array can be empty `[]` to clear all formulas for a report.
    
    **Success Response (200):**
    ```json
    {
        "status": 200,
        "message": "Report formulas created successfully for 'zomato_vs_pos_summary' in 'formulas' collection",
        "data": {
            "report_name": "zomato_vs_pos_summary",
            "formulas_count": 4,
            "formulas": [...],
            "collection_existed": false,
            "mongodb_connected": true
        }
    }
    ```
    
    **Error Responses:**
    - **400 Bad Request**: Invalid request or missing required fields
    - **503 Service Unavailable**: MongoDB not connected
    - **500 Internal Server Error**: Other errors
    """
    # Convert FormulaItem Pydantic models to dictionaries
    formulas_dict = [f.model_dump() for f in request.formulas]
    # Convert ConditionItem Pydantic models to dictionaries
    conditions_dict = {
        key: [c.model_dump() for c in conditions_list]
        for key, conditions_list in request.conditions.items()
    }
    
    return await formulas_controller.save_report_formulas(
        request.report_name,
        formulas_dict,
        request.mapping_keys,
        conditions_dict
    )


class GetReportFormulasResponse(BaseModel):
    """Response model for getting report formulas"""
    status: int = Field(..., description="HTTP status code", example=200)
    message: str = Field(..., description="Response message", example="Report formulas retrieved successfully")
    data: Dict[str, Any] = Field(
        ...,
        description="Response data",
        example={
            "report_name": "zomato_vs_pos_summary",
            "formulas": [
                {
                    "id": 1,
                    "logicName": "Total Amount",
                    "fields": [
                        {
                            "type": "data_field",
                            "dataset_type": "Dataset",
                            "selectedDataSetValue": "zvd",
                            "selectedFieldValue": "zomato",
                            "customFieldValue": "",
                            "startBrackets": [],
                            "endBrackets": [],
                            "selectedTableName": "zomato",
                            "selectedTableColumn": "zvd"
                        }
                    ],
                    "formulaText": "zomato.zvd + zomato.merchant_pack_charge",
                    "logicNameKey": "TOTAL_AMOUNT",
                    "multipleColumn": False,
                    "active_group_index": 0,
                    "excelFormulaText": " zomato.zvd + zomato.merchant_pack_charge"
                }
            ],
            "formulas_count": 1,
            "mapping_keys": {
                "zomato": ["order_id", "order_date"],
                "pos": ["paytm_counter"]
            },
            "conditions": {
                "zomato": [
                    {
                        "column": "order_id",
                        "operator": "not_equal",
                        "value": "NULL"
                    }
                ],
                "pos": [
                    {
                        "column": "sales_type",
                        "operator": "equal",
                        "value": "ZOMATO"
                    }
                ]
            },
            "created_at": "2024-01-01T12:00:00",
            "updated_at": "2024-01-01T12:00:00",
            "mongodb_connected": True
        }
    )


@router.get(
    "/uploader/reports/{report_name}",
    tags=["Report Formulas"],
    summary="Get report formulas",
    description="Retrieve all formulas for a specific report collection.",
    response_model=GetReportFormulasResponse,
    status_code=status.HTTP_200_OK,
    responses={
        200: {
            "description": "Report formulas retrieved successfully",
            "content": {
                "application/json": {
                    "example": {
                        "status": 200,
                        "message": "Report formulas retrieved successfully",
                        "data": {
                            "report_name": "zomato_vs_pos_summary",
                            "formulas": [
                                {
                                    "id": 1,
                                    "logicName": "Total Amount",
                                    "fields": [
                                        {
                                            "type": "data_field",
                                            "dataset_type": "Dataset",
                                            "selectedDataSetValue": "zvd",
                                            "selectedFieldValue": "zomato",
                                            "customFieldValue": "",
                                            "startBrackets": [],
                                            "endBrackets": [],
                                            "selectedTableName": "zomato",
                                            "selectedTableColumn": "zvd"
                                        }
                                    ],
                                    "formulaText": "zomato.zvd + zomato.merchant_pack_charge",
                                    "logicNameKey": "TOTAL_AMOUNT",
                                    "multipleColumn": False,
                                    "active_group_index": 0,
                                    "excelFormulaText": " zomato.zvd + zomato.merchant_pack_charge"
                                }
                            ],
                            "formulas_count": 1,
                            "created_at": "2024-01-15T10:30:00.123456",
                            "updated_at": "2024-01-15T10:30:00.123456",
                            "mongodb_connected": True
                        }
                    }
                }
            }
        },
        404: {
            "description": "Report collection not found",
            "content": {
                "application/json": {
                    "example": {
                        "detail": "Report collection 'zomato_vs_pos_summary' does not exist"
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
async def get_report_formulas(report_name: str):
    """
    Get report formulas by report name from the 'formulas' collection.
    
    **Features:**
    - Retrieves all formulas for a specific report from the 'formulas' collection
    - Returns formulas, timestamps, and metadata
    - Report name is converted to lowercase
    
    **Example:**
    ```
    GET /api/uploader/reports/zomato_vs_pos_summary
    ```
    
    **Success Response (200):**
    ```json
    {
        "status": 200,
        "message": "Report formulas retrieved successfully",
        "data": {
            "report_name": "zomato_vs_pos_summary",
            "formulas": [
                {
                    "id": 1,
                    "logicName": "Total Amount",
                    "fields": [
                        {
                            "type": "data_field",
                            "dataset_type": "Dataset",
                            "selectedDataSetValue": "zvd",
                            "selectedFieldValue": "zomato",
                            "customFieldValue": "",
                            "startBrackets": [],
                            "endBrackets": [],
                            "selectedTableName": "zomato",
                            "selectedTableColumn": "zvd"
                        },
                        {
                            "type": "operator",
                            "dataset_type": "Dataset",
                            "selectedFieldValue": "+",
                            "selectedTableName": ""
                        },
                        {
                            "type": "data_field",
                            "dataset_type": "Dataset",
                            "selectedDataSetValue": "merchant_pack_charge",
                            "selectedFieldValue": "zomato",
                            "customFieldValue": "",
                            "startBrackets": [],
                            "endBrackets": [],
                            "selectedTableName": "zomato",
                            "selectedTableColumn": "merchant_pack_charge"
                        }
                    ],
                    "formulaText": "zomato.zvd + zomato.merchant_pack_charge",
                    "logicNameKey": "TOTAL_AMOUNT",
                    "multipleColumn": False,
                    "active_group_index": 0,
                    "excelFormulaText": " zomato.zvd + zomato.merchant_pack_charge"
                }
            ],
            "formulas_count": 1,
            "created_at": "2024-01-15T10:30:00.123456",
            "updated_at": "2024-01-15T10:30:00.123456",
            "mongodb_connected": true
        }
    }
    ```
    
    **Error Responses:**
    - **404 Not Found**: Collection or document doesn't exist
    - **503 Service Unavailable**: MongoDB not connected
    - **500 Internal Server Error**: Other errors
    """
    return await formulas_controller.get_report_formulas(report_name)


class UpdateReportFormulasRequest(BaseModel):
    """Request model for updating report formulas"""
    formulas: List[FormulaItem] = Field(
        ...,
        description="List of formulas with logicName, formulaText, fields, etc. (can be empty)",
        default_factory=list
    )
    mapping_keys: Dict[str, List[str]] = Field(
        default_factory=dict,
        description="Mapping keys for joining collections (e.g., {'zomato': ['order_id', 'order_date'], 'pos': ['paytm_counter']})"
    )
    conditions: Dict[str, List[ConditionItem]] = Field(
        default_factory=dict,
        description="Conditions to filter data from collections (e.g., {'zomato': [{'column': 'order_id', 'operator': 'not_equal', 'value': 'NULL'}]})"
    )
    
    class Config:
        json_schema_extra = {
            "example": {
                "formulas": [
                    {
                        "id": 1,
                        "logicName": "Total Amount",
                        "fields": [
                            {
                                "type": "data_field",
                                "dataset_type": "Dataset",
                                "selectedDataSetValue": "zvd",
                                "selectedFieldValue": "zomato",
                                "customFieldValue": "",
                                "startBrackets": [],
                                "endBrackets": [],
                                "selectedTableName": "zomato",
                                "selectedTableColumn": "zvd"
                            },
                            {
                                "type": "operator",
                                "dataset_type": "Dataset",
                                "selectedFieldValue": "+",
                                "selectedTableName": ""
                            }
                        ],
                        "formulaText": "zomato.zvd + zomato.merchant_pack_charge",
                        "logicNameKey": "TOTAL_AMOUNT",
                        "multipleColumn": False,
                        "active_group_index": 0,
                        "excelFormulaText": " zomato.zvd + zomato.merchant_pack_charge"
                    }
                ],
                "mapping_keys": {
                    "zomato": ["order_id", "order_date"],
                    "pos": ["paytm_counter"]
                },
                "conditions": {
                    "zomato": [
                        {
                            "column": "order_id",
                            "operator": "not_equal",
                            "value": "NULL"
                        }
                    ],
                    "pos": [
                        {
                            "column": "sales_type",
                            "operator": "equal",
                            "value": "ZOMATO"
                        }
                    ]
                }
            }
        }


class UpdateReportFormulasResponse(BaseModel):
    """Response model for updating report formulas"""
    status: int = Field(..., description="HTTP status code", example=200)
    message: str = Field(..., description="Response message", example="Report formulas updated successfully in collection 'zomato_vs_pos_summary'")
    data: Dict[str, Any] = Field(
        ...,
        description="Response data",
        example={
            "report_name": "zomato_vs_pos_summary",
            "formulas_count": 4,
            "formulas": [
                {"formula_name": "pos_order_id", "formula_value": "pos.order_id"},
                {"formula_name": "zomato_order_id", "formula_value": "zomato.order_id"}
            ],
            "mapping_keys": {
                "zomato": ["order_id", "order_date"],
                "pos": ["paytm_counter"]
            },
            "conditions": {
                "zomato": [
                    {
                        "column": "order_id",
                        "operator": "not_equal",
                        "value": "NULL"
                    }
                ],
                "pos": [
                    {
                        "column": "sales_type",
                        "operator": "equal",
                        "value": "ZOMATO"
                    }
                ]
            },
            "mongodb_connected": True
        }
    )


@router.put(
    "/uploader/reports/{report_name}/formulas",
    tags=["Report Formulas"],
    summary="Update report formulas",
    description="Update report formulas in an existing MongoDB collection. The collection must exist. This will replace all existing formulas with the new set.",
    response_model=UpdateReportFormulasResponse,
    status_code=status.HTTP_200_OK,
    responses={
        200: {
            "description": "Report formulas updated successfully",
            "content": {
                "application/json": {
                    "example": {
                        "status": 200,
                        "message": "Report formulas updated successfully in collection 'zomato_vs_pos_summary'",
                        "data": {
                            "report_name": "zomato_vs_pos_summary",
                            "formulas_count": 4,
                            "formulas": [
                                {"formula_name": "pos_order_id", "formula_value": "pos.order_id"},
                                {"formula_name": "zomato_order_id", "formula_value": "zomato.order_id"},
                                {"formula_name": "pos_net_amount", "formula_value": "pos.net_amount"},
                                {"formula_name": "zomato_net_amount", "formula_value": "zomato.sub_total - zomato.mvd + zomato.gst"}
                            ],
                            "mongodb_connected": True
                        }
                    }
                }
            }
        },
        400: {
            "description": "Invalid request",
            "content": {
                "application/json": {
                    "example": {
                        "detail": "At least one formula is required"
                    }
                }
            }
        },
        404: {
            "description": "Report collection not found",
            "content": {
                "application/json": {
                    "example": {
                        "detail": "Report collection 'zomato_vs_pos_summary' does not exist"
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
async def update_report_formulas(
    report_name: str,
    request: UpdateReportFormulasRequest = Body(...)
):
    """
    Update report formulas in the 'formulas' collection.
    
    **Features:**
    - Updates formulas in the 'formulas' collection (document must exist)
    - Replaces all existing formulas with the new set
    - Report name is converted to lowercase
    - Validates that all formulas have required fields (if provided)
    - Allows empty formulas array to clear all formulas for a report
    - Updates the `updated_at` timestamp
    
    **Request Body:**
    ```json
    {
        "formulas": [
            {
                "id": 1,
                "logicName": "Total Amount",
                "fields": [
                    {
                        "type": "data_field",
                        "dataset_type": "Dataset",
                        "selectedDataSetValue": "zvd",
                        "selectedFieldValue": "zomato",
                        "customFieldValue": "",
                        "startBrackets": [],
                        "endBrackets": [],
                        "selectedTableName": "zomato",
                        "selectedTableColumn": "zvd"
                    },
                    {
                        "type": "operator",
                        "dataset_type": "Dataset",
                        "selectedFieldValue": "+",
                        "selectedTableName": ""
                    },
                    {
                        "type": "data_field",
                        "dataset_type": "Dataset",
                        "selectedDataSetValue": "merchant_pack_charge",
                        "selectedFieldValue": "zomato",
                        "customFieldValue": "",
                        "startBrackets": [],
                        "endBrackets": [],
                        "selectedTableName": "zomato",
                        "selectedTableColumn": "merchant_pack_charge"
                    }
                ],
                "formulaText": "zomato.zvd + zomato.merchant_pack_charge",
                "logicNameKey": "TOTAL_AMOUNT",
                "multipleColumn": false,
                "active_group_index": 0,
                "excelFormulaText": " zomato.zvd + zomato.merchant_pack_charge"
            }
        ]
    }
    ```
    
    **Note:** The `formulas` array can be empty `[]` to clear all formulas for a report.
    
    **Example:**
    ```
    PUT /api/uploader/reports/zomato_vs_pos_summary/formulas
    ```
    
    **Success Response (200):**
    ```json
    {
        "status": 200,
        "message": "Report formulas updated successfully for 'zomato_vs_pos_summary' in 'formulas' collection",
        "data": {
            "report_name": "zomato_vs_pos_summary",
            "formulas_count": 4,
            "formulas": [...],
            "mongodb_connected": true
        }
    }
    ```
    
    **Error Responses:**
    - **400 Bad Request**: Invalid request or missing required fields
    - **404 Not Found**: Document doesn't exist in 'formulas' collection
    - **503 Service Unavailable**: MongoDB not connected
    - **500 Internal Server Error**: Other errors
    
    **Note:** This endpoint requires the document to exist in the 'formulas' collection. Use the POST endpoint to create a new report document.
    """
    # Convert FormulaItem Pydantic models to dictionaries
    formulas_dict = [f.model_dump() for f in request.formulas]
    # Convert ConditionItem Pydantic models to dictionaries
    conditions_dict = {
        key: [c.model_dump() for c in conditions_list]
        for key, conditions_list in request.conditions.items()
    }
    
    return await formulas_controller.update_report_formulas(
        report_name,
        formulas_dict,
        request.mapping_keys,
        conditions_dict
    )


class DeleteReportResponse(BaseModel):
    """Response model for deleting report formulas"""
    status: int = Field(..., description="HTTP status code", example=200)
    message: str = Field(..., description="Response message", example="Report formulas for 'zomato_vs_pos_summary' deleted successfully from 'formulas' collection")
    data: Dict[str, Any] = Field(
        ...,
        description="Response data",
        example={
            "report_name": "zomato_vs_pos_summary",
            "collection_name": "formulas",
            "mongodb_connected": True
        }
    )


@router.delete(
    "/uploader/reports/{report_name}",
    tags=["Report Formulas"],
    summary="Delete report formulas",
    description="Delete a report document from the 'formulas' collection. This will permanently delete the report's formulas.",
    response_model=DeleteReportResponse,
    status_code=status.HTTP_200_OK,
    responses={
        200: {
            "description": "Report formulas deleted successfully",
            "content": {
                "application/json": {
                    "example": {
                        "status": 200,
                        "message": "Report formulas for 'zomato_vs_pos_summary' deleted successfully from 'formulas' collection",
                        "data": {
                            "report_name": "zomato_vs_pos_summary",
                            "collection_name": "formulas",
                            "mongodb_connected": True
                        }
                    }
                }
            }
        },
        404: {
            "description": "Report document not found",
            "content": {
                "application/json": {
                    "example": {
                        "detail": "Report document not found for 'zomato_vs_pos_summary' in 'formulas' collection"
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
async def delete_report_collection(report_name: str):
    """
    Delete a report document from the 'formulas' collection.
    
    **Features:**
    - Permanently deletes the report's formulas document from the 'formulas' collection
    - Report name is converted to lowercase
    - Returns error if document doesn't exist
    - Only deletes the specific report document, not the entire collection
    
    **Example:**
    ```
    DELETE /api/uploader/reports/zomato_vs_pos_summary
    ```
    
    **Success Response (200):**
    ```json
    {
        "status": 200,
        "message": "Report formulas for 'zomato_vs_pos_summary' deleted successfully from 'formulas' collection",
        "data": {
            "report_name": "zomato_vs_pos_summary",
            "collection_name": "formulas",
            "mongodb_connected": true
        }
    }
    ```
    
    **Error Responses:**
    - **404 Not Found**: Report document doesn't exist
    - **503 Service Unavailable**: MongoDB not connected
    - **500 Internal Server Error**: Other errors
    
    **Warning:** This operation is irreversible. The report's formulas will be permanently deleted.
    """
    return await formulas_controller.delete_report_collection(report_name)


class GetAllFormulasResponse(BaseModel):
    """Response model for getting all formulas"""
    status: int = Field(..., description="HTTP status code", example=200)
    message: str = Field(..., description="Response message", example="Retrieved 3 report formula(s) successfully")
    data: Dict[str, Any] = Field(
        ...,
        description="Response data",
        example={
            "formulas": [
                {
                    "_id": "507f1f77bcf86cd799439011",
                    "report_name": "zomato_vs_pos_summary",
                    "formulas": [
                        {"formula_name": "pos_order_id", "formula_value": "pos.order_id"},
                        {"formula_name": "zomato_order_id", "formula_value": "zomato.order_id"}
                    ],
                    "formulas_count": 2,
                    "created_at": "2024-01-15T10:30:00.123456",
                    "updated_at": "2024-01-15T10:30:00.123456"
                }
            ],
            "count": 1,
            "mongodb_connected": True
        }
    )


@router.get(
    "/uploader/reports/formulas/all",
    tags=["Report Formulas"],
    summary="Get all report formulas",
    description="Retrieve all report formulas from the 'formulas' collection.",
    response_model=GetAllFormulasResponse,
    status_code=status.HTTP_200_OK,
    responses={
        200: {
            "description": "All report formulas retrieved successfully",
            "content": {
                "application/json": {
                    "example": {
                        "status": 200,
                        "message": "Retrieved 3 report formula(s) successfully",
                        "data": {
                            "formulas": [
                                {
                                    "_id": "507f1f77bcf86cd799439011",
                                    "report_name": "zomato_vs_pos_summary",
                                    "formulas": [
                                        {"formula_name": "pos_order_id", "formula_value": "pos.order_id"},
                                        {"formula_name": "zomato_order_id", "formula_value": "zomato.order_id"}
                                    ],
                                    "formulas_count": 4,
                                    "created_at": "2024-01-15T10:30:00.123456",
                                    "updated_at": "2024-01-15T10:30:00.123456"
                                }
                            ],
                            "count": 1,
                            "mongodb_connected": True
                        }
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
async def get_all_formulas():
    """
    Get all report formulas from the 'formulas' collection.
    
    **Features:**
    - Retrieves all report formulas stored in the 'formulas' collection
    - Returns formulas sorted by report name
    - Returns empty list if collection doesn't exist or is empty
    
    **Example:**
    ```
    GET /api/uploader/reports/formulas/all
    ```
    
    **Success Response (200):**
    ```json
    {
        "status": 200,
        "message": "Retrieved 3 report formula(s) successfully",
        "data": {
            "formulas": [
                {
                    "_id": "507f1f77bcf86cd799439011",
                    "report_name": "zomato_vs_pos_summary",
                    "formulas": [
                        {
                            "formula_name": "pos_order_id",
                            "formula_value": "pos.order_id"
                        },
                        {
                            "formula_name": "zomato_order_id",
                            "formula_value": "zomato.order_id"
                        }
                    ],
                    "formulas_count": 2,
                    "created_at": "2024-01-15T10:30:00.123456",
                    "updated_at": "2024-01-15T10:30:00.123456"
                }
            ],
            "count": 1,
            "mongodb_connected": true
        }
    }
    ```
    
    **Error Responses:**
    - **503 Service Unavailable**: MongoDB not connected
    - **500 Internal Server Error**: Other errors
    """
    return await formulas_controller.get_all_formulas()


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
