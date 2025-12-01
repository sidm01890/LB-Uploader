import pandas as pd
import io
import logging
from typing import Dict, List, Any, Optional
from pathlib import Path
from sqlalchemy import text, insert
from app.db import get_mysql_connection
from app.services.validation_service import validator
from app.services.file_utils import extract_sample_data
from app.services.file_manager import FileManager, FileSource
from app.services.mongodb_service import (
    save_uploaded_sheet,
    update_upload_status
)

logger = logging.getLogger(__name__)

class DataUploader:
    """
    Service for uploading and processing data with intelligent column mapping.
    Integrates with FileManager for organized file handling.
    """
    
    def __init__(self):
        self.engine = get_mysql_connection()
        self.file_manager = FileManager()
    
    async def process_upload(
        self, 
        file, 
        table_name: str, 
        mapping: Dict[str, str],
        validation_config: Optional[Dict] = None,
        source: FileSource = FileSource.SFTP,
        vendor_folder: Optional[str] = None,
        datasource: Optional[str] = None,
        uploaded_by: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Process file upload with intelligent mapping and validation.
        Saves incoming sheet to MongoDB before processing.
        
        Args:
            file: File to upload
            table_name: Target database table
            mapping: Column mapping dictionary
            validation_config: Validation rules
            source: FileSource (SFTP or EMAIL)
            vendor_folder: Vendor subfolder name
            datasource: Data source identifier (e.g., ZOMATO, TRM)
            uploaded_by: Username who uploaded the file
        """
        file_path = None
        temp_path = None
        upload_id = None
        
        try:
            # Read file content for MongoDB storage
            file_content = await file.read()
            file_size = len(file_content)
            file_extension = file.filename.lower().split('.')[-1] if file.filename else ''
            
            # Reset file pointer
            try:
                await file.seek(0)
            except Exception:
                try:
                    file.file.seek(0)
                except Exception:
                    pass
            
            # Extract and process file data
            file_data = await self._extract_file_data(file)
            
            # Extract headers
            headers = list(file_data.columns)
            
            # Convert DataFrame to column-based format for MongoDB storage
            # Format: {column_name: [value1, value2, ...], ...}
            raw_data = file_data.to_dict('list')  # 'list' gives column-based format
            
            # Save to MongoDB before processing
            try:
                upload_id = save_uploaded_sheet(
                    filename=file.filename or "unknown",
                    datasource=datasource or "UNKNOWN",
                    table_name=table_name,
                    file_size=file_size,
                    file_type=file_extension,
                    headers=headers,
                    raw_data=raw_data,
                    uploaded_by=uploaded_by,
                    column_mapping=mapping
                )
                logger.info(f"✅ Saved sheet to MongoDB: upload_id={upload_id}")
            except Exception as mongo_error:
                logger.warning(f"⚠️ Failed to save to MongoDB (continuing with upload): {mongo_error}")
                # Continue with upload even if MongoDB save fails
            
            # Validate data before upload
            validation_results = await self._validate_upload_data(
                file_data, mapping, table_name, validation_config
            )
            
            # Skip MongoDB status updates - only raw_data is saved
            # if upload_id:
            #     try:
            #         update_upload_status(
            #             upload_id=upload_id,
            #             status="processing",
            #             validation_results=validation_results,
            #             datasource=datasource
            #         )
            #     except Exception as mongo_error:
            #         logger.warning(f"⚠️ Failed to update MongoDB validation results: {mongo_error}")
            
            if not validation_results.get("valid", True):
                # Skip MongoDB status updates - only raw_data is saved
                # if upload_id:
                #     try:
                #         update_upload_status(
                #             upload_id=upload_id,
                #             status="failed",
                #             validation_results=validation_results,
                #             error="Data validation failed",
                #             datasource=datasource
                #         )
                #     except Exception as mongo_error:
                #         logger.warning(f"⚠️ Failed to update MongoDB status: {mongo_error}")
                
                return {
                    "success": False,
                    "error": "Data validation failed",
                    "validation_results": validation_results,
                    "recommendations": validation_results.get("recommendations", []),
                    "upload_id": upload_id
                }
            
            # Upload data to database
            upload_results = await self._upload_to_database(
                file_data, table_name, mapping
            )
            
            # Skip MongoDB status updates - only raw_data is saved
            # if upload_id:
            #     try:
            #         if upload_results.get("successful_rows", 0) > 0:
            #             update_upload_status(
            #                 upload_id=upload_id,
            #                 status="completed",
            #                 mysql_upload_results=upload_results,
            #                 datasource=datasource
            #             )
            #             logger.info(f"✅ Updated MongoDB: upload completed, upload_id={upload_id}")
            #         else:
            #             update_upload_status(
            #                 upload_id=upload_id,
            #                 status="failed",
            #                 mysql_upload_results=upload_results,
            #                 error="No rows successfully uploaded",
            #                 datasource=datasource
            #             )
            #     except Exception as mongo_error:
            #         logger.warning(f"⚠️ Failed to update MongoDB upload results: {mongo_error}")
            
            # Archive file if we have file_path and vendor info
            if hasattr(file, 'file_path') and vendor_folder:
                try:
                    if upload_results.get("successful_rows", 0) > 0:
                        # Move to processed folder organized by month/year
                        processed_path = self.file_manager.move_to_processed(
                            Path(file.file_path),
                            source=source,
                            vendor_folder=vendor_folder
                        )
                        logger.info(f"File archived to: {processed_path}")
                except Exception as archive_error:
                    logger.warning(f"Could not archive file: {archive_error}")
            
            return {
                "success": True,
                "upload_results": upload_results,
                "validation_results": validation_results,
                "upload_id": upload_id,
                "summary": {
                    "total_rows": len(file_data),
                    "successful_rows": upload_results.get("successful_rows", 0),
                    "failed_rows": upload_results.get("failed_rows", 0),
                    "data_quality_score": validation_results.get("data_quality_score", 0.0)
                }
            }
            
        except Exception as e:
            logger.error(f"Upload processing error: {str(e)}")
            
            # Skip MongoDB status updates - only raw_data is saved
            # if upload_id:
            #     try:
            #         update_upload_status(
            #             upload_id=upload_id,
            #             status="failed",
            #             error=str(e),
            #             datasource=datasource
            #         )
            #     except Exception as mongo_error:
            #         logger.warning(f"⚠️ Failed to update MongoDB error status: {mongo_error}")
            
            # Move to failed folder if we have file_path
            if hasattr(file, 'file_path'):
                try:
                    failed_path = self.file_manager.move_to_failed(
                        Path(file.file_path),
                        failure_type="upload",
                        error_message=str(e)
                    )
                    logger.info(f"Failed file moved to: {failed_path}")
                except Exception as move_error:
                    logger.warning(f"Could not move failed file: {move_error}")
            
            return {
                "success": False,
                "error": str(e),
                "upload_results": {},
                "validation_results": {},
                "upload_id": upload_id
            }
    
    async def _extract_file_data(self, file) -> pd.DataFrame:
        """
        Extract data from uploaded file.
        """
        try:
            content = await file.read()
            file_extension = file.filename.lower().split('.')[-1] if file.filename else ''
            
            if file_extension in ['xlsx', 'xls']:
                df = pd.read_excel(io.BytesIO(content))
            elif file_extension == 'csv':
                try:
                    df = pd.read_csv(io.BytesIO(content), encoding='utf-8')
                except UnicodeDecodeError:
                    try:
                        df = pd.read_csv(io.BytesIO(content), encoding='latin-1')
                    except UnicodeDecodeError:
                        df = pd.read_csv(io.BytesIO(content), encoding='cp1252')
            else:
                raise ValueError(f"Unsupported file type: {file_extension}")
            
            # Clean the data
            df = self._clean_dataframe(df)
            
            logger.info(f"Extracted {len(df)} rows from {file.filename}")
            return df
            
        except Exception as e:
            logger.error(f"Error extracting file data: {str(e)}")
            raise
    
    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and normalize dataframe.
        """
        # Remove completely empty rows
        df = df.dropna(how='all')
        
        # Clean column names
        df.columns = [str(col).strip() for col in df.columns]
        
        # Remove unnamed columns
        df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
        
        # Fill NaN values with empty strings for string columns
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].fillna('')
        
        return df
    
    async def _validate_upload_data(
        self, 
        df: pd.DataFrame, 
        mapping: Dict[str, str], 
        table_name: str,
        validation_config: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """
        Validate data before upload.
        """
        try:
            # Get table schema
            db_columns = await self._get_table_schema(table_name)
            
            # Run validation
            validation_results = await validator.validate_mapped_data(
                df, mapping, db_columns
            )
            
            return validation_results
            
        except Exception as e:
            logger.error(f"Validation error: {str(e)}")
            return {
                "valid": False,
                "errors": [f"Validation failed: {str(e)}"],
                "warnings": [],
                "data_quality_score": 0.0
            }
    
    async def _get_table_schema(self, table_name: str) -> List[Dict]:
        """
        Get table schema information.
        """
        from app.services.db_service import get_table_columns
        return get_table_columns(table_name)
    
    async def _upload_to_database(
        self, 
        df: pd.DataFrame, 
        table_name: str, 
        mapping: Dict[str, str]
    ) -> Dict[str, Any]:
        """
        Upload data to database with error handling.
        """
        try:
            # Get table columns to check if UID is needed
            from app.services.db_service import get_table_columns
            table_columns = get_table_columns(table_name)
            # Extract column names (get_table_columns returns dicts with 'COLUMN_NAME' key)
            if table_columns and len(table_columns) > 0:
                table_column_names = [col.get('COLUMN_NAME', col.get('name', str(col))) for col in table_columns]
            else:
                table_column_names = []
            
            # Prepare data for insertion
            mapped_df = self._prepare_mapped_data(df, mapping)
            
            if mapped_df.empty:
                return {
                    "successful_rows": 0,
                    "failed_rows": 0,
                    "errors": ["No data to upload after mapping"]
                }
            
            # Generate UID if table has uid column and it's not already in the dataframe
            if 'uid' in table_column_names and 'uid' not in mapped_df.columns:
                from datetime import datetime
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                # Use order_id if available, otherwise use res_id, otherwise use index
                if 'order_id' in mapped_df.columns:
                    mapped_df.insert(0, 'uid', [
                        f"ZOMATO_{timestamp}_{str(row.get('order_id', i)).replace(' ', '_').replace('/', '_')}_{i+1}"
                        for i, (_, row) in enumerate(mapped_df.iterrows())
                    ])
                elif 'res_id' in mapped_df.columns:
                    mapped_df.insert(0, 'uid', [
                        f"ZOMATO_{timestamp}_{str(row.get('res_id', i)).replace(' ', '_').replace('/', '_')}_{i+1}"
                        for i, (_, row) in enumerate(mapped_df.iterrows())
                    ])
                else:
                    mapped_df.insert(0, 'uid', [
                        f"ZOMATO_{timestamp}_{i+1}" for i in range(len(mapped_df))
                    ])
                logger.info(f"Generated {len(mapped_df)} UIDs for {table_name} table")
            
            # Insert data in batches
            batch_size = 1000
            successful_rows = 0
            failed_rows = 0
            errors = []
            
            for i in range(0, len(mapped_df), batch_size):
                batch = mapped_df.iloc[i:i + batch_size]
                
                try:
                    # Convert to list of dictionaries
                    batch_data = batch.to_dict('records')
                    
                    # Insert batch
                    with self.engine.connect() as conn:
                        # Use SQLAlchemy's bulk insert
                        result = conn.execute(
                            text(f"INSERT INTO {table_name} ({', '.join(mapped_df.columns)}) VALUES ({', '.join([':' + col for col in mapped_df.columns])})"),
                            batch_data
                        )
                        conn.commit()
                        
                    successful_rows += len(batch)
                    logger.info(f"Successfully inserted batch {i//batch_size + 1}")
                    
                except Exception as e:
                    failed_rows += len(batch)
                    error_msg = f"Batch {i//batch_size + 1} failed: {str(e)}"
                    errors.append(error_msg)
                    logger.error(error_msg)
            
            return {
                "successful_rows": successful_rows,
                "failed_rows": failed_rows,
                "errors": errors,
                "total_batches": (len(mapped_df) + batch_size - 1) // batch_size
            }
            
        except Exception as e:
            logger.error(f"Database upload error: {str(e)}")
            return {
                "successful_rows": 0,
                "failed_rows": len(df),
                "errors": [f"Upload failed: {str(e)}"]
            }
    
    def _prepare_mapped_data(self, df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
        """
        Prepare data with column mapping.
        """
        try:
            # Create mapped dataframe
            mapped_data = {}
            
            for file_header, db_column in mapping.items():
                if db_column and file_header in df.columns:
                    mapped_data[db_column] = df[file_header]
            
            if not mapped_data:
                logger.warning("No valid mappings found")
                return pd.DataFrame()
            
            mapped_df = pd.DataFrame(mapped_data)
            
            # Clean data types
            mapped_df = self._clean_data_types(mapped_df)
            
            return mapped_df
            
        except Exception as e:
            logger.error(f"Error preparing mapped data: {str(e)}")
            return pd.DataFrame()
    
    def _clean_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and convert data types, handling NaN values and Excel dates properly.
        """
        import numpy as np
        from datetime import datetime, timedelta
        
        for col in df.columns:
            col_lower = col.lower()
            
            # Convert date columns first - handle Excel serial dates
            if 'date' in col_lower or col_lower in ['created_at', 'updated_at', 'timestamp']:
                # Try multiple date conversion strategies
                try:
                    # First, try if it's already a datetime (pandas Timestamp)
                    if pd.api.types.is_datetime64_any_dtype(df[col]):
                        # Already datetime - just convert to date
                        df[col] = df[col].dt.date
                    else:
                        # Check if values are numeric (Excel serial dates or integer dates)
                        numeric_vals = pd.to_numeric(df[col], errors='coerce')
                        if numeric_vals.notna().sum() > 0:
                            # Check what format the numeric values are in
                            sample = numeric_vals.dropna().iloc[0] if len(numeric_vals.dropna()) > 0 else None
                            if sample and 10000000 <= sample <= 99999999:
                                # Integer date format (YYYYMMDD) - like 20241204
                                df[col] = pd.to_datetime(df[col].astype(str), format='%Y%m%d', errors='coerce').dt.date
                            elif sample and 1000 < sample < 100000:
                                # Excel serial date: convert from days since 1899-12-30
                                base_date = datetime(1899, 12, 30)
                                date_list = [base_date + timedelta(days=int(d)) if pd.notna(d) else None for d in numeric_vals]
                                df[col] = pd.Series(date_list).dt.date
                            else:
                                # Regular numeric - try datetime conversion (might be Unix timestamp)
                                # But first check if it's a huge number (nanoseconds) - if so, skip
                                if sample and sample > 1e12:
                                    # Too large, likely error - try as string
                                    df[col] = pd.to_datetime(df[col].astype(str), errors='coerce').dt.date
                                else:
                                    df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
                        else:
                            # String dates - parse normally
                            df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
                except Exception as e:
                    logger.warning(f"Date conversion error for column {col}: {e}")
                    # Fallback: try simple datetime parsing
                    try:
                        df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
                    except:
                        # Last resort: set to None
                        df[col] = None
            
            # Try to convert to numeric for columns that might be numeric
            # Check if column contains numeric data
            elif col_lower not in ['id', 'res_id', 'order_id', 'store_code', 'sap_code', 'city_id', 'service_id', 'action', 'status', 'payment_method', 'promo_code', 'utr_number', 'res_name', 'store_name', 'city_name']:
                numeric_series = pd.to_numeric(df[col], errors='coerce')
                non_null_numeric = numeric_series.notna().sum()
                
                # If more than 50% of values are numeric, treat as numeric column
                if non_null_numeric > len(df) * 0.5 or col_lower in ['amount', 'price', 'quantity', 'count', 'rate', 'percent', 'charge', 'discount', 'value', 'fee', 'tax', 'cost', 'adj', 'refund', 'compensation', 'deduction', 'handling', 'recovery', 'penalty', 'credit', 'voucher', 'subtotal', 'commission', 'logistics', 'delivery', 'pack', 'zvd', 'mvd', 'tcs', 'tds', 'pgcharge', 'pg_chgs_percent', 'ls']:
                    df[col] = numeric_series
            
            # Clean string columns
            else:
                df[col] = df[col].astype(str).str.strip()
                # Replace empty strings and 'nan' strings with None
                df[col] = df[col].replace(['nan', 'NaN', 'None', 'null', ''], None)
        
        # Final pass: replace all NaN/NaT values with None (will become NULL in MySQL)
        df = df.replace([np.nan, pd.NaT], None)
        df = df.replace(['nan', 'NaN'], None)
        
        return df

# Global uploader instance
uploader = DataUploader()
