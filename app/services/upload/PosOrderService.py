"""
POS Order Service - OPTIMIZED V2
Handles POS order data file uploads with performance optimizations:
1. Increased batch size (5000)
2. Single DB connection for entire upload
3. executemany() instead of row-by-row execute
4. Pre-computed column mappings
5. Sets for O(1) type checking
6. CSV streaming with csv.reader (NO iterrows!)
7. openpyxl streaming for Excel files
"""
import csv
import logging
import os
import traceback
from datetime import datetime, timedelta
from typing import List, Dict, Set, Optional

import pandas as pd
from fastapi import UploadFile, HTTPException
from sqlalchemy.orm import Session

from app.core.config import config
from app.core.enum.DataSource import DataSource as DS
from app.services.DataSourceAbstractService import DataSourceService
from app.core.database import db_manager

log = logging.getLogger(__name__)


# ============================================================================
# OPTIMIZATION: Define column type sets at module level for O(1) lookup
# ============================================================================
DATE_COLUMNS: Set[str] = {
    'date', 'original_date', 'business_date',
    # Removed: 'shift_date', 'void_date', 'last_printing_date' - storing as VARCHAR to preserve raw values
}

DATETIME_COLUMNS: Set[str] = {
    'bill_time', 'online_void_time',
    'time_when_total_pressed', 'time_when_trans_closed',
    # Removed: 'order_received_time', 'dispatch_time', 'deliver_time', 'food_ready', 'load_time'
    # - storing as VARCHAR to preserve raw values including placeholders
}

DECIMAL_COLUMNS: Set[str] = {
    'net_sale', 'cost_amount', 'gross_amount', 'payment', 
    'discount', 'service_tax_amount', 'rounded'  # rounded is decimal, not boolean
}

INTEGER_COLUMNS: Set[str] = {
    'no_of_items', 'no_of_item_lines', 'no_of_payment_lines',
    'no_of_covers', 'replication_counter', 'paytm_counter',
    # Removed: 'pre_receipt_counter' - storing as VARCHAR to preserve alphanumeric values
}

BOOLEAN_COLUMNS: Set[str] = {'open_drawer', 'tax_liable'}  # rounded moved to DECIMAL_COLUMNS

STRING_PRESERVE_COLUMNS: Set[str] = {'delivered', 'no_of_covers_aib'}


class PosOrderService(DataSourceService):
    """Service for uploading POS order data files - OPTIMIZED VERSION V2"""
    
    # OPTIMIZATION: Increased batch size
    CHUNK_SIZE = 5000
    data_upload_dir = config.data_upload_dir
    directory_path = os.path.join(data_upload_dir, "orders", "pos")

    @staticmethod
    def _normalize_column_name(column_name: str) -> str:
        """Normalize column name for comparison"""
        if not isinstance(column_name, str):
            return str(column_name)
        normalized = column_name.replace('\n', ' ').replace('\r', ' ')
        normalized = ''.join(c.lower() for c in normalized if c.isalnum() or c.isspace())
        normalized = ' '.join(normalized.strip().split())
        return normalized

    def convert_excel_date(self, value, is_time_column=False):
        """Convert Excel date serial number or string date to datetime - OPTIMIZED.
        
        Args:
            value: The value to convert
            is_time_column: If True, handles special placeholder dates (1753/1754) by 
                           extracting time and combining with today's date
        """
        if value is None or value == '':
                return None
            
        try:
            # Handle datetime objects directly
            if isinstance(value, datetime):
                return value
            
            # Handle numeric values
            if isinstance(value, (int, float)):
                # Value 0 is not a valid datetime
                if value == 0:
                    return None
                excel_date = float(value)
                if excel_date < 1 or excel_date > 100000:
                    return None
                if excel_date > 59:
                    excel_date -= 1
                return datetime(1899, 12, 30) + timedelta(days=excel_date)
            
            # Handle string dates
            if isinstance(value, str):
                value = value.strip()
                if not value or value == '0':
                    return None
                
                # OPTIMIZATION: Try the most common format FIRST (CSV uses this)
                # "Sunday, 1 December 2024" format
                if ',' in value and len(value) > 15:
                    try:
                        return datetime.strptime(value, '%A, %d %B %Y')
                    except ValueError:
                        pass
                
                # Handle datetime strings with placeholder years (1753, 1754)
                # These are used in POS data where only time is relevant
                # Format: "1754-01-01 11:10:07.033" or "1753-01-01 00:00:00.000"
                if value.startswith('1753-') or value.startswith('1754-'):
                    try:
                        # Parse the datetime
                        if '.' in value:
                            parsed = datetime.strptime(value, '%Y-%m-%d %H:%M:%S.%f')
                        else:
                            parsed = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
                        
                        # If time is 00:00:00, it's a null placeholder
                        if parsed.hour == 0 and parsed.minute == 0 and parsed.second == 0:
                            return None
                        
                        # Return just the time as a datetime with today's date
                        # This preserves the time information
                        today = datetime.now().date()
                        return datetime.combine(today, parsed.time())
                    except ValueError:
                        return None
                
                # Try numeric string (Excel serial)
                if value.replace('.', '').replace('-', '').isdigit():
                    try:
                        num_val = float(value)
                        if num_val == 0:
                            return None
                        if 1 <= num_val <= 100000:
                            if num_val > 59:
                                num_val -= 1
                            return datetime(1899, 12, 30) + timedelta(days=num_val)
                    except ValueError:
                        pass
                
                # Standard datetime format: YYYY-MM-DD HH:MM:SS
                if '-' in value and ':' in value:
                    try:
                        if '.' in value:
                            parsed = datetime.strptime(value, '%Y-%m-%d %H:%M:%S.%f')
                        else:
                            parsed = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
                        if 1900 <= parsed.year <= 2100:
                            return parsed
                    except ValueError:
                        pass
                
                # Other common date formats (less frequent)
                if '-' in value:
                    try:
                        return datetime.strptime(value, '%Y-%m-%d')
                    except ValueError:
                        try:
                            return datetime.strptime(value, '%d-%m-%Y')
                        except ValueError:
                            pass
                
                if '/' in value:
                    try:
                        return datetime.strptime(value, '%d/%m/%Y')
                    except ValueError:
                        pass
                
                return None
            
            return None
        except:
            return None

    async def upload_file(self, files: List[UploadFile]) -> List[str]:
        """Save uploaded files to disk using streaming (handles large files efficiently)"""
        os.makedirs(self.directory_path, exist_ok=True)
        uploaded_file_paths = []
        
        for file in files:
            filename = file.filename
            if not filename:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                content_type = getattr(file, 'content_type', '')
                if 'excel' in content_type or 'spreadsheet' in content_type:
                    ext = '.xlsx'
                elif 'csv' in content_type:
                    ext = '.csv'
                else:
                    ext = '.xlsx'
                filename = f"upload_{timestamp}{ext}"
            
            file_path = os.path.join(self.directory_path, filename)
            chunk_size = 1024 * 1024  # 1MB chunks
            total_size = 0
            
            # Stream write using async read (prevents loading entire file into memory)
            with open(file_path, "wb") as f:
                while True:
                    chunk = await file.read(chunk_size)
                    if not chunk:
                        break
                    f.write(chunk)
                    total_size += len(chunk)
            
            # Reset file pointer for potential future reads
            await file.seek(0)
            
            absolute_path = os.path.abspath(file_path)
            log.info(f"File saved at path: {absolute_path} (size: {total_size} bytes)")
            uploaded_file_paths.append(absolute_path)
        
        return uploaded_file_paths

    def _build_normalized_mapping(self, fieldsMap: Dict[str, str], 
                                   mapped_excel_columns: List[str]) -> Dict[str, str]:
        """Pre-compute normalized column name mapping"""
        normalized_to_original = {}
        for excel_col in mapped_excel_columns:
            normalized = self._normalize_column_name(excel_col)
            normalized_to_original[normalized] = excel_col
        return normalized_to_original

    def _clean_value(self, val):
        """Convert numpy/pandas types to Python native types"""
        if val is None:
            return None
        if hasattr(val, 'item'):
            return val.item()
        if isinstance(val, float) and (val != val):  # NaN check
            return None
        if isinstance(val, (pd.Timestamp, pd.Timedelta)):
            return val.to_pydatetime() if hasattr(val, 'to_pydatetime') else None
        return val

    def _process_value(self, db_col: str, value) -> any:
        """Process a single value based on column type - OPTIMIZED V2"""
        # Fast null check
        if value is None or value == '':
            return None
        
        # OPTIMIZATION: O(1) set lookups - most common types first
        if db_col in DATE_COLUMNS or db_col in DATETIME_COLUMNS:
            return self.convert_excel_date(value)
        
        if db_col in DECIMAL_COLUMNS:
            try:
                return float(value)
            except:
                return None
        
        if db_col in INTEGER_COLUMNS:
            try:
                int_value = int(float(value))
                return int_value if int_value <= 9223372036854775807 else None
            except:
                return None
        
        if db_col in BOOLEAN_COLUMNS:
            if isinstance(value, bool):
                return value
            if isinstance(value, (int, float)):
                return bool(value)
            if isinstance(value, str):
                v = value.lower().strip()
                if v in ('true', '1', 'yes', 'y'):
                    return True
                if v in ('false', '0', 'no', 'n', ''):
                    return False
            return None
        
        # Default: string conversion
        return str(value) if value else None

    def _process_row(self, row_dict: Dict, fieldsMap: Dict[str, str],
                     normalized_mapping: Dict[str, str],
                     header_normalized: Dict[str, str],
                     uid_column_map: Dict[str, str]) -> Optional[Dict]:
        """Process a single row and return data dict or None if invalid"""
        data = {}
        
        # Map columns using pre-computed normalized mapping
        for excel_col, value in row_dict.items():
            normalized_excel_col = header_normalized.get(excel_col)
            if normalized_excel_col is None:
                normalized_excel_col = self._normalize_column_name(str(excel_col))
            
            matching_col = normalized_mapping.get(normalized_excel_col)
            
            if matching_col:
                db_col = fieldsMap[matching_col]
                data[db_col] = self._process_value(db_col, value)
        
        # Create UID using pre-computed uid column map
        uid_parts = []
        for uid_col, actual_col in uid_column_map.items():
            if actual_col:
                value = row_dict.get(actual_col, '')
                str_value = str(value) if value is not None else ''
                if str_value and str_value != 'nan' and str_value != 'None' and str_value != '':
                    uid_parts.append(str_value)
        
        if uid_parts:
            data['id'] = '_'.join(uid_parts)
            return data
        
        return None

    def _bulk_insert(self, cursor, batch: List[Dict], table_name: str = 'orders') -> int:
        """Bulk insert using executemany with prepared statement"""
        if not batch:
            return 0
        
        columns = list(batch[0].keys())
        placeholders = ', '.join(['%s'] * len(columns))
        update_clause = ', '.join([f"{col} = VALUES({col})" for col in columns if col != 'id'])
        
        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {update_clause}"
        
        values_list = []
        for record in batch:
            values = tuple(self._clean_value(record.get(col)) for col in columns)
            values_list.append(values)
        
        cursor.executemany(query, values_list)
        return len(batch)

    def upload(self, files: List[str], db: Session, username: str) -> int:
        """
        Upload and process POS order data files - OPTIMIZED VERSION V2
        
        Key optimizations for CSV:
        - Uses csv.reader for streaming (NO iterrows!)
        - Single DB connection for entire upload
        - executemany() for bulk inserts
        """
        total_rows_inserted = 0
        
        try:
            # Get table column mappings from database
            with db_manager.get_mysql_connector() as connection:
                cursor = connection.cursor(dictionary=True)
                
                cursor.execute("""
                    SELECT excel_column_name, db_column_name 
                    FROM table_columns_mapping 
                    WHERE data_source = %s
                """, (DS.POS_ORDERS.name,))
                mappings = cursor.fetchall()
                
                if not mappings:
                    raise HTTPException(
                        status_code=400,
                        detail=f"No field mappings found for {DS.POS_ORDERS.name}"
                    )
                
                cursor.execute("""
                    SELECT COLUMN_NAME 
                    FROM information_schema.COLUMNS
                    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'orders'
                """)
                orders_table_columns = set(row['COLUMN_NAME'] for row in cursor.fetchall())
                
                fieldsMap = {}
                for m in mappings:
                    excel_col = m['excel_column_name'].strip()
                    db_col = m['db_column_name'].strip()
                    if db_col in orders_table_columns:
                        fieldsMap[excel_col] = db_col
                
                mapped_excel_columns = list(fieldsMap.keys())
                log.info(f"✅ Using {len(fieldsMap)} valid mappings")
                
                cursor.execute("""
                    SELECT uid_columns FROM data_source WHERE name = %s
                """, (DS.POS_ORDERS.name,))
                data_source = cursor.fetchone()
                
                if not data_source or not data_source.get('uid_columns'):
                    raise HTTPException(
                        status_code=400,
                        detail=f"No uid_columns found in DataSource table for {DS.POS_ORDERS.name}"
                    )
                
                uid_columns = [col.strip() for col in data_source['uid_columns'].split(',')]
                log.info(f"Using uid columns: {uid_columns}")
            
            normalized_mapping = self._build_normalized_mapping(fieldsMap, mapped_excel_columns)
            log.info(f"📋 Pre-computed {len(normalized_mapping)} column mappings")
            
            for file_path in files:
                log.info(f"🚀 Processing file: {file_path}")
                
                try:
                    if file_path.endswith(('.xlsx', '.xls')):
                        import openpyxl
                        
                        workbook = openpyxl.load_workbook(file_path, read_only=True, data_only=True)
                        sheet = workbook.active
                        
                        rows_iter = sheet.iter_rows(values_only=True)
                        header = next(rows_iter)
                        
                        col_map = {i: col for i, col in enumerate(header) if col is not None}
                        header_normalized = {col: self._normalize_column_name(str(col)) for col in col_map.values()}
                        
                        uid_column_map = {}
                        for uid_col in uid_columns:
                            uid_normalized = self._normalize_column_name(uid_col)
                            for actual_col in col_map.values():
                                if self._normalize_column_name(str(actual_col)) == uid_normalized:
                                    uid_column_map[uid_col] = actual_col
                                    break
                            if uid_col not in uid_column_map:
                                uid_column_map[uid_col] = None
                        
                        with db_manager.get_mysql_connector() as db_connection:
                            db_cursor = db_connection.cursor()
                            
                            batch_to_insert = []
                            processed_count = 0
                            batch_count = 0
                            
                            for row in rows_iter:
                                row_dict = {col_map[i]: val for i, val in enumerate(row) if i in col_map}
                                
                                data = self._process_row(
                                    row_dict, fieldsMap, normalized_mapping,
                                    header_normalized, uid_column_map
                            )
                            
                                if data:
                                    batch_to_insert.append(data)
                                
                                processed_count += 1
                                
                                if len(batch_to_insert) >= self.CHUNK_SIZE:
                                    batch_count += 1
                                    rows_inserted = self._bulk_insert(db_cursor, batch_to_insert)
                                    db_connection.commit()
                                    total_rows_inserted += rows_inserted
                                    log.info(f"⚡ Batch {batch_count}: {rows_inserted} rows. Total: {total_rows_inserted:,}")
                                    batch_to_insert = []
                            
                            if batch_to_insert:
                                batch_count += 1
                                rows_inserted = self._bulk_insert(db_cursor, batch_to_insert)
                                db_connection.commit()
                                total_rows_inserted += rows_inserted
                                log.info(f"⚡ Final batch {batch_count}: {rows_inserted} rows. Total: {total_rows_inserted:,}")
                        
                        workbook.close()
                    
                    elif file_path.endswith('.csv'):
                        # ============================================================
                        # OPTIMIZED CSV PROCESSING - Using csv.reader (NO iterrows!)
                        # ============================================================
                        log.info("📄 Processing CSV with streaming reader (optimized)")
                        
                        with open(file_path, 'r', encoding='utf-8', errors='replace') as csvfile:
                            reader = csv.reader(csvfile)
                            header = next(reader)
                            
                            # Build column mapping
                            col_map = {i: col for i, col in enumerate(header) if col}
                            header_normalized = {col: self._normalize_column_name(str(col)) for col in col_map.values()}
                            
                            # Pre-compute UID column mapping
                            uid_column_map = {}
                            for uid_col in uid_columns:
                                uid_normalized = self._normalize_column_name(uid_col)
                                for actual_col in col_map.values():
                                    if self._normalize_column_name(str(actual_col)) == uid_normalized:
                                        uid_column_map[uid_col] = actual_col
                                        break
                                if uid_col not in uid_column_map:
                                    uid_column_map[uid_col] = None
                            
                            log.info(f"📊 UID column mapping: {uid_column_map}")
                            
                            with db_manager.get_mysql_connector() as db_connection:
                                db_cursor = db_connection.cursor()
                                
                                batch_to_insert = []
                                processed_count = 0
                                batch_count = 0
                                
                                for row in reader:
                                    # Build row dict from CSV row
                                    row_dict = {}
                                    for i, val in enumerate(row):
                                        if i in col_map:
                                            # Convert empty strings to None
                                            row_dict[col_map[i]] = val if val != '' else None
                                    
                                    data = self._process_row(
                                        row_dict, fieldsMap, normalized_mapping,
                                        header_normalized, uid_column_map
                                    )
                                    
                                    if data:
                                        batch_to_insert.append(data)
                                        processed_count += 1
                                        
                                        # Insert in batches
                                        if len(batch_to_insert) >= self.CHUNK_SIZE:
                                            batch_count += 1
                                            rows_inserted = self._bulk_insert(db_cursor, batch_to_insert)
                                            db_connection.commit()
                                            total_rows_inserted += rows_inserted
                                            log.info(f"⚡ Batch {batch_count}: {rows_inserted} rows. Total: {total_rows_inserted:,}")
                                            batch_to_insert = []
                                
                                # Insert remaining records
                                if batch_to_insert:
                                    batch_count += 1
                                    rows_inserted = self._bulk_insert(db_cursor, batch_to_insert)
                                    db_connection.commit()
                                    total_rows_inserted += rows_inserted
                                    log.info(f"⚡ Final batch {batch_count}: {rows_inserted} rows. Total: {total_rows_inserted:,}")
                        
                        log.info(f"📊 Processed {processed_count:,} rows")
                    
                    else:
                        raise HTTPException(status_code=400, detail=f"Unsupported file type: {file_path}")
                
                except Exception as e:
                    log.error(f"Error processing file: {e}")
                    traceback.print_exc()
                    raise HTTPException(status_code=400, detail=f"Error reading file {file_path}: {str(e)}")
            
            log.info(f"🎉 POS Orders upload completed. Total rows: {total_rows_inserted:,}")
            return total_rows_inserted
            
        except HTTPException as he:
            log.error(f"HTTP Error during data upload: {he.detail}")
            raise he
        except Exception as e:
            log.error(f"Error during data upload: {e}")
            traceback.print_exc()
            db.rollback()
            raise HTTPException(
                status_code=500,
                detail=f"Internal server error during data upload: {str(e)}"
            )
        finally:
            db.close()
