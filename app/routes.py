"""
Routes module - Similar to Node.js routes
Only defines endpoints and calls controllers (no business logic here)
"""

from fastapi import APIRouter, UploadFile, Form, Query, File
from typing import Optional
import logging

# Import controllers (similar to Node.js: const uploadController = require('./controllers/uploadController'))
from app.controllers.upload_controller import UploadController
from app.controllers.mapping_controller import MappingController

logger = logging.getLogger(__name__)

# Initialize router
router = APIRouter()

# Initialize controllers (similar to Node.js: instantiate controllers)
upload_controller = UploadController()
mapping_controller = MappingController()

@router.get("/")
async def root():
    """
    Root endpoint - API information
    """
    return mapping_controller.get_root_info()

@router.post("/map-columns")
async def map_columns(
    file: UploadFile,
    table_name: str = Form(...),
    include_sample_data: bool = Form(False)
):
    """
    Upload a file and specify the MySQL table name.
    The API will:
      1. Extract headers from file
      2. Fetch MySQL table column metadata
      3. Use AI to intelligently map headers to DB columns
      4. Optionally include sample data for better mapping
    """
    # Route only calls controller (similar to Node.js: router.post('/', uploadController.mapColumns))
    return await upload_controller.map_columns(file, table_name, include_sample_data)

@router.post("/upload-data")
async def upload_data(
    file: UploadFile,
    table_name: str = Form(...),
    mapping: str = Form(...),  # JSON string of column mappings
    validate_before_upload: bool = Form(True)
):
    """
    Upload and process data with intelligent column mapping.
    The API will:
      1. Process the uploaded file
      2. Apply column mapping
      3. Validate data (optional)
      4. Upload to database
    """
    # Route only calls controller
    return await upload_controller.upload_data(file, table_name, mapping, validate_before_upload)

@router.post("/validate-mapping")
async def validate_mapping(
    file: UploadFile,
    table_name: str = Form(...),
    mapping: str = Form(...)
):
    """
    Validate column mapping and data quality before upload.
    """
    # Route only calls controller
    return await upload_controller.validate_mapping(file, table_name, mapping)

@router.post("/upload")
async def upload_by_datasource(
    file: UploadFile = File(...),
    datasource: str = Query(..., description="Data source identifier (e.g., ZOMATO, TRM, MPR)"),
    chunk_size: Optional[int] = Query(1000, description="Chunk size for processing (for compatibility)"),
    client: Optional[str] = Query(None, description="Client identifier (for tracking)"),
    validate_before_upload: bool = Query(True, description="Validate data before upload")
):
    """
    Upload file based on datasource configuration.
    The API will:
      1. Look up datasource configuration
      2. Extract target table from config
      3. Get or generate column mappings
      4. Process and upload data
    
    Example:
        curl -X POST 'http://localhost:8080/api/upload?datasource=ZOMATO&chunk_size=1000' \\
             -F 'file=@file.xlsx'
    """
    # Route only calls controller (all business logic is in controller)
    return await upload_controller.upload_by_datasource(
        file, datasource, chunk_size, client, validate_before_upload
    )

@router.get("/health")
async def health_check():
    """
    Health check endpoint.
    """
    # Route only calls controller
    return mapping_controller.get_health_status()

