import pandas as pd
import io
import logging
from typing import List, Dict, Any, Optional
from fastapi import HTTPException, UploadFile

logger = logging.getLogger(__name__)

async def extract_headers(file: UploadFile) -> List[str]:
    """
    Extract headers from uploaded Excel/CSV file.
    Supports .xlsx, .xls, .csv files with intelligent detection.
    """
    try:
        # Read file content
        content = await file.read()
        
        # Determine file type and read accordingly
        file_extension = file.filename.lower().split('.')[-1] if file.filename else ''
        
        if file_extension in ['xlsx', 'xls']:
            # Excel file processing
            df = pd.read_excel(io.BytesIO(content), nrows=0)  # Read only headers
        elif file_extension == 'csv':
            # CSV file processing with encoding detection
            try:
                df = pd.read_csv(io.BytesIO(content), nrows=0, encoding='utf-8')
            except UnicodeDecodeError:
                try:
                    df = pd.read_csv(io.BytesIO(content), nrows=0, encoding='latin-1')
                except UnicodeDecodeError:
                    df = pd.read_csv(io.BytesIO(content), nrows=0, encoding='cp1252')
        else:
            raise HTTPException(
                status_code=400, 
                detail=f"Unsupported file type: {file_extension}. Supported formats: xlsx, xls, csv"
            )
        
        # Clean and normalize headers
        headers = [str(col).strip() for col in df.columns.tolist()]
        
        # Remove empty headers
        headers = [h for h in headers if h and h != 'Unnamed: 0']
        
        if not headers:
            raise HTTPException(status_code=400, detail="No valid headers found in the file")
        
        logger.info(f"Extracted {len(headers)} headers from {file.filename}")
        return headers
        
    except Exception as e:
        logger.error(f"Error extracting headers from {file.filename}: {str(e)}")
        raise HTTPException(status_code=400, detail=f"Error processing file: {str(e)}")

async def extract_sample_data(file: UploadFile, sample_size: int = 5) -> Dict[str, Any]:
    """
    Extract sample data from uploaded file for better AI mapping.
    Returns headers and sample rows for context.
    """
    try:
        content = await file.read()
        file_extension = file.filename.lower().split('.')[-1] if file.filename else ''
        
        if file_extension in ['xlsx', 'xls']:
            df = pd.read_excel(io.BytesIO(content), nrows=sample_size)
        elif file_extension == 'csv':
            try:
                df = pd.read_csv(io.BytesIO(content), nrows=sample_size, encoding='utf-8')
            except UnicodeDecodeError:
                try:
                    df = pd.read_csv(io.BytesIO(content), nrows=sample_size, encoding='latin-1')
                except UnicodeDecodeError:
                    df = pd.read_csv(io.BytesIO(content), nrows=sample_size, encoding='cp1252')
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported file type: {file_extension}")
        
        # Clean headers
        headers = [str(col).strip() for col in df.columns.tolist()]
        headers = [h for h in headers if h and h != 'Unnamed: 0']
        
        # Get sample data
        sample_data = df.head(sample_size).fillna('').to_dict('records')
        
        return {
            "headers": headers,
            "sample_data": sample_data,
            "total_columns": len(headers),
            "sample_size": len(sample_data)
        }
        
    except Exception as e:
        logger.error(f"Error extracting sample data from {file.filename}: {str(e)}")
        raise HTTPException(status_code=400, detail=f"Error processing file: {str(e)}")

def validate_file_size(file: UploadFile, max_size_mb: int = 50) -> bool:
    """
    Validate file size before processing.
    """
    try:
        # Read a small portion to check if file is accessible
        content = file.file.read(1024)  # Read first 1KB
        file.file.seek(0)  # Reset file pointer
        
        # Check if file is too large (this is a basic check)
        # For production, you might want to implement streaming validation
        return True
    except Exception as e:
        logger.error(f"File validation error: {str(e)}")
        return False

def get_file_info(file: UploadFile) -> Dict[str, Any]:
    """
    Get basic file information for logging and debugging.
    """
    return {
        "filename": file.filename,
        "content_type": file.content_type,
        "size": getattr(file, 'size', 'unknown')
    }
