"""
Centralized Data Uploader
Handles all data uploads with validation and deduplication
"""

import pandas as pd
import mysql.connector
from datetime import datetime
import logging
import sys
import os
from typing import Tuple, Dict
import hashlib

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))
from app import config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class DataUploader:
    """Centralized data uploader with history tracking and deduplication"""
    
    def __init__(self):
        from app.core.database import db_manager
        from app.core.config import config
        self.db_manager = db_manager
        self.db_config = db_manager.get_connection_dict()
        self.batch_size = 1000
        logger.info(f"Initialized DataUploader for database: {self.db_config['database']}")
    
    def get_file_hash(self, file_path: str) -> str:
        """Generate unique hash for file to detect duplicates"""
        with open(file_path, 'rb') as f:
            return hashlib.md5(f.read()).hexdigest()
    
    def check_upload_history(self, table_name: str, file_name: str) -> bool:
        """Check if this exact file was already uploaded successfully"""
        try:
            with self.db_manager.get_mysql_connector() as conn:
            cursor = conn.cursor()
            
                query = """
                    SELECT id, upload_date, records_uploaded 
                    FROM upload_history 
                    WHERE table_name = %s 
                      AND file_name = %s 
                      AND upload_status = 'SUCCESS'
                    ORDER BY upload_date DESC LIMIT 1
                """
                cursor.execute(query, (table_name, file_name))
                result = cursor.fetchone()
                
                if result:
                logger.info(f"⚠️  File '{file_name}' already uploaded to {table_name} on {result[1]} ({result[2]:,} records)")
                return True
            return False
            
        except Exception as e:
            logger.warning(f"Could not check upload history: {e}")
            return False
    
    def record_upload(self, table_name: str, file_path: str, file_name: str, 
                     records_before: int, records_uploaded: int, records_after: int,
                     status: str, error_msg: str = None):
        """Record upload attempt in history"""
        try:
            with self.db_manager.get_mysql_connector() as conn:
                cursor = conn.cursor()
                
                file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
                
                query = """
                    INSERT INTO upload_history 
                    (table_name, file_name, file_path, file_size, records_before, 
                     records_uploaded, records_after, upload_status, error_message)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(query, (table_name, file_name, file_path, file_size,
                                      records_before, records_uploaded, records_after,
                                      status, error_msg))
                conn.commit()
            
        except Exception as e:
            logger.error(f"Failed to record upload history: {e}")
    
    def clean_column_name(self, col: str) -> str:
        """Convert Excel column name to MySQL-friendly format"""
        import re
        col = re.sub(r'[^\w\s]', '', col)
        col = re.sub(r'\s+', '_', col.strip())
        return col.lower()
    
    def clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean data - fix dates, times, remove invalid values"""
        for col in df.columns:
            if df[col].dtype == 'object':
                # Convert empty strings and whitespace to None
                df[col] = df[col].apply(lambda x: None if isinstance(x, str) and x.strip() == '' else x)
                
                # Fix invalid time values (e.g., '20:13:42:32', '::', etc.)
                sample = df[col].dropna().head(100)
                if len(sample) > 0:
                    sample_str = str(sample.iloc[0])
                    # If it looks like time-related data
                    if ':' in sample_str:
                        logger.info(f"  🔧 Fixing time format in column '{col}'")
                        def fix_time(x):
                            if pd.isna(x):
                                return None
                            s = str(x).strip()
                            if not s or s == '::' or s.count(':') < 1:
                                return None
                            # Take only first 3 segments (HH:MM:SS)
                            parts = s.split(':')[:3]
                            # Filter out empty parts
                            parts = [p for p in parts if p]
                            if len(parts) == 0:
                                return None
                            return ':'.join(parts) if len(parts) >= 2 else None
                        
                        df[col] = df[col].apply(fix_time)
        
        return df
    
    def _map_bank_statements_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Map Excel columns to fin_bank_statements table columns"""
        # Excel columns: date, mode, particulars, deposits, withdrawals, balance
        # Target columns: statement_date, value_date, bank_ref, narration, memo, 
        #                 amount, debit_credit, balance_after
        
        df_mapped = pd.DataFrame(index=df.index)
        
        # Find date column (could be 'date', 'statement_date', or similar)
        date_col = None
        for col in df.columns:
            if 'date' in col.lower():
                date_col = col
                break
        
        # Map DATE to statement_date and value_date
        if date_col:
            # Try to parse date
            try:
                parsed_dates = pd.to_datetime(df[date_col], errors='coerce')
                df_mapped['statement_date'] = parsed_dates.dt.date
                df_mapped['value_date'] = df_mapped['statement_date']  # Same as statement_date
                # Fill any NaT with today's date as fallback
                if df_mapped['statement_date'].isna().any():
                    today = datetime.now().date()
                    df_mapped['statement_date'] = df_mapped['statement_date'].fillna(today)
                    df_mapped['value_date'] = df_mapped['value_date'].fillna(today)
            except Exception as e:
                logger.warning(f"  ⚠️  Date parsing failed: {e}, using today's date")
                today = datetime.now().date()
                df_mapped['statement_date'] = today
                df_mapped['value_date'] = today
        else:
            # No date column found, use today's date
            logger.warning(f"  ⚠️  No date column found, using today's date")
            today = datetime.now().date()
            df_mapped['statement_date'] = today
            df_mapped['value_date'] = today
        
        # Map MODE to bank_ref
        if 'mode' in df.columns:
            df_mapped['bank_ref'] = df['mode']
        else:
            df_mapped['bank_ref'] = None
        
        # Map PARTICULARS to narration and memo
        if 'particulars' in df.columns:
            df_mapped['narration'] = df['particulars']
            df_mapped['memo'] = df['particulars']
        else:
            df_mapped['narration'] = None
            df_mapped['memo'] = None
        
        # Map DEPOSITS and WITHDRAWALS to amount and debit_credit
        # Find deposits and withdrawals columns (case-insensitive)
        deposits_col = None
        withdrawals_col = None
        for col in df.columns:
            if 'deposit' in col.lower():
                deposits_col = col
            elif 'withdrawal' in col.lower():
                withdrawals_col = col
        
        # Amount: use deposits if > 0, else withdrawals, else 0
        # Debit_credit: CREDIT if deposits > 0, DEBIT if withdrawals > 0, else CREDIT
        if deposits_col:
            deposits = pd.to_numeric(df[deposits_col], errors='coerce').fillna(0)
        else:
            deposits = pd.Series([0] * len(df))
        
        if withdrawals_col:
            withdrawals = pd.to_numeric(df[withdrawals_col], errors='coerce').fillna(0)
        else:
            withdrawals = pd.Series([0] * len(df))
        
        # Calculate amount and debit_credit
        df_mapped['amount'] = deposits.where(deposits > 0, withdrawals).fillna(0)
        df_mapped['debit_credit'] = pd.Series(['CREDIT' if d > 0 else ('DEBIT' if w > 0 else 'CREDIT') 
                                               for d, w in zip(deposits, withdrawals)])
        
        # Ensure amount is never null (set to 0 if all null)
        if df_mapped['amount'].isna().any():
            df_mapped['amount'] = df_mapped['amount'].fillna(0)
        
        # Map BALANCE to balance_after
        if 'balance' in df.columns:
            df_mapped['balance_after'] = pd.to_numeric(df['balance'], errors='coerce')
        else:
            df_mapped['balance_after'] = None
        
        # Add other columns that might be in the table but not in Excel
        df_mapped['counterparty'] = None
        
        logger.info(f"  🔄 Mapped bank statement columns: {len(df_mapped.columns)} columns")
        
        return df_mapped
    
    def _map_orders_columns(self, df: pd.DataFrame, table_columns: list = None) -> pd.DataFrame:
        """Map POS CSV columns to orders table columns"""
        df_mapped = pd.DataFrame(index=df.index)
        
        # Generate id from timestamp or transaction number
        if 'timestamp' in df.columns:
            df_mapped['id'] = df['timestamp'].astype(str)
        elif 'transaction_no_' in df.columns:
            df_mapped['id'] = df['transaction_no_'].astype(str)
        else:
            # Generate ID from index
            df_mapped['id'] = [f"ORD_{datetime.now().strftime('%Y%m%d')}_{i+1}" for i in range(len(df))]
        
        # Map date columns
        if 'date' in df.columns:
            try:
                parsed_dates = pd.to_datetime(df['date'], errors='coerce')
                df_mapped['date'] = parsed_dates
                df_mapped['original_date'] = parsed_dates
                df_mapped['business_date'] = parsed_dates
            except:
                df_mapped['date'] = None
                df_mapped['original_date'] = None
                df_mapped['business_date'] = None
        else:
            df_mapped['date'] = None
            df_mapped['original_date'] = None
            df_mapped['business_date'] = None
        
        # Map store name
        if 'store_no_' in df.columns:
            df_mapped['store_name'] = df['store_no_'].astype(str)
        else:
            df_mapped['store_name'] = None
        
        # Map transaction number
        if 'transaction_no_' in df.columns:
            df_mapped['transaction_number'] = df['transaction_no_'].astype(str)
        else:
            df_mapped['transaction_number'] = None
        
        # Map receipt/bill number
        if 'receipt_no_' in df.columns:
            df_mapped['bill_number'] = df['receipt_no_'].astype(str)
        else:
            df_mapped['bill_number'] = None
        
        # Map time
        if 'time' in df.columns:
            try:
                df_mapped['bill_time'] = pd.to_datetime(df['time'], errors='coerce')
            except:
                df_mapped['bill_time'] = None
        else:
            df_mapped['bill_time'] = None
        
        # Map staff/user
        if 'staff_id' in df.columns:
            df_mapped['bill_user'] = df['staff_id'].astype(str)
        elif 'online_order_taker' in df.columns:
            df_mapped['online_order_taker'] = df['online_order_taker']
            df_mapped['bill_user'] = df['online_order_taker']
        else:
            df_mapped['bill_user'] = None
            df_mapped['online_order_taker'] = None
        
        # Map amounts - try to find gross_amount, subtotal, discount, net_sale
        amount_cols = {
            'gross_amount': ['gross_amount', 'gross', 'total'],
            'subtotal': ['subtotal', 'sub_total'],
            'discount': ['discount', 'total_discount'],
            'net_sale': ['net_sale', 'net_amount', 'net']
        }
        
        for target_col, possible_names in amount_cols.items():
            found = False
            for name in possible_names:
                for col in df.columns:
                    if name.lower() in col.lower():
                        df_mapped[target_col] = pd.to_numeric(df[col], errors='coerce')
                        found = True
                        logger.info(f"  🔧 Mapped '{col}' → '{target_col}'")
                        break
                if found:
                    break
            if not found:
                df_mapped[target_col] = None
        
        # Map other columns with flexible matching
        # source_type (CSV) → source (DB)
        if 'source_type' in df.columns:
            df_mapped['source'] = df['source_type']
            logger.info(f"  🔧 Mapped 'source_type' → 'source'")
        elif 'source' in df.columns:
            df_mapped['source'] = df['source']
        else:
            df_mapped['source'] = None
        
        # instance_id - should exist in CSV
        if 'instance_id' in df.columns:
            df_mapped['instance_id'] = df['instance_id']
        else:
            df_mapped['instance_id'] = None
        
        # online_order_taker - already mapped above, but ensure it's set
        if 'online_order_taker' in df.columns:
            df_mapped['online_order_taker'] = df['online_order_taker']
        else:
            df_mapped['online_order_taker'] = None
        
        # Calculate subtotal if not directly available
        # subtotal = gross_amount - discount (or use net_amount if available)
        if 'subtotal' not in df_mapped.columns or df_mapped['subtotal'].isna().all():
            if 'gross_amount' in df_mapped.columns and 'discount' in df_mapped.columns:
                df_mapped['subtotal'] = pd.to_numeric(df_mapped['gross_amount'], errors='coerce') - pd.to_numeric(df_mapped['discount'], errors='coerce')
                logger.info(f"  🔧 Calculated 'subtotal' = gross_amount - discount")
            elif 'net_amount' in df.columns:
                df_mapped['subtotal'] = pd.to_numeric(df['net_amount'], errors='coerce')
                logger.info(f"  🔧 Calculated 'subtotal' from net_amount")
            else:
                df_mapped['subtotal'] = None
        
        # Try to derive GST at 5% from service_tax_amount if available
        if 'gst_at_5_percent' not in df_mapped.columns or df_mapped['gst_at_5_percent'].isna().all():
            if 'service_tax_amount' in df.columns:
                # If service_tax_amount exists, it might be the GST (check if it's 5% of subtotal)
                service_tax = pd.to_numeric(df['service_tax_amount'], errors='coerce')
                if 'subtotal' in df_mapped.columns:
                    subtotal = pd.to_numeric(df_mapped['subtotal'], errors='coerce')
                    # If service_tax is approximately 5% of subtotal, use it
                    calculated_gst = subtotal * 0.05
                    # Use service_tax if it's close to 5% of subtotal (within 10% tolerance)
                    df_mapped['gst_at_5_percent'] = service_tax.where(
                        (service_tax - calculated_gst).abs() / calculated_gst.abs() < 0.1, 
                        calculated_gst
                    )
                    logger.info(f"  🔧 Derived 'gst_at_5_percent' from service_tax_amount")
                else:
                    df_mapped['gst_at_5_percent'] = service_tax
            else:
                df_mapped['gst_at_5_percent'] = None
        
        # Columns that don't exist in CSV - set to None
        # These will remain NULL as they're not in the source data
        missing_cols = ['channel', 'settlement_mode', 'gst_ecom_at_5_percent', 
                       'packaging_charge_cart_swiggy', 
                       'packaging_charge', 'mode_name', 
                       'restaurant_packaging_charges']
        for col in missing_cols:
            df_mapped[col] = None
        
        # IMPORTANT: Don't set created_at and updated_at to None
        # Let MySQL use the DEFAULT (now()) values
        # We'll exclude them from the INSERT statement if they're not in the dataframe
        
        # payment column
        if 'payment' in df.columns:
            df_mapped['payment'] = pd.to_numeric(df['payment'], errors='coerce')
        else:
            df_mapped['payment'] = None
        
        # CRITICAL FIX: After special mappings, map ALL remaining CSV columns that match database column names
        # This ensures we don't lose data from CSV columns that match DB columns by name
        if table_columns:
            mapped_so_far = set(df_mapped.columns)
            csv_cols = set(df.columns)
            
            # Find CSV columns that match database columns exactly (that we haven't mapped yet)
            for db_col in table_columns:
                if db_col not in mapped_so_far and db_col in csv_cols:
                    # Direct match - map it
                    df_mapped[db_col] = df[db_col]
                    logger.info(f"  🔧 Auto-mapped CSV column '{db_col}' → database column '{db_col}'")
            
            # Add any missing database columns as None
            for db_col in table_columns:
                if db_col not in df_mapped.columns:
                    df_mapped[db_col] = None
        
        logger.info(f"  🔄 Mapped POS orders columns: {len(df_mapped.columns)} columns (from {len(df.columns)} CSV columns)")
        
        return df_mapped
    
    def _map_mpr_upi_columns(self, df: pd.DataFrame, table_columns: list) -> pd.DataFrame:
        """Map MPR UPI Excel columns to database columns with special handling"""
        df_mapped = pd.DataFrame(index=df.index)
        
        # Column mapping: Excel column -> DB column
        column_mapping = {
            'customer_ref_no_rrn': 'customer_ref_no',  # Fix: Excel has _rrn suffix, DB doesn't
        }
        
        # First, apply direct mappings
        for excel_col, db_col in column_mapping.items():
            if excel_col in df.columns:
                df_mapped[db_col] = df[excel_col]
                logger.info(f"  🔧 Mapped '{excel_col}' → '{db_col}'")
        
        # Then, map all other columns that match exactly
        for col in table_columns:
            if col not in df_mapped.columns:
                if col in df.columns:
                    df_mapped[col] = df[col]
                else:
                    # Check if it's in the reverse mapping (already handled)
                    if col not in column_mapping.values():
                        df_mapped[col] = None
        
        logger.info(f"  🔄 Mapped MPR UPI columns: {len(df_mapped.columns)} columns")
        return df_mapped
    
    def _map_mpr_card_columns(self, df: pd.DataFrame, table_columns: list) -> pd.DataFrame:
        """Map MPR Card Excel columns to database columns with special handling"""
        df_mapped = pd.DataFrame(index=df.index)
        
        # Column mapping: Excel column (after cleaning) -> DB column
        # Note: Excel has '%' which becomes empty string '' after cleaning
        # Excel has 'NVL(BH_MEICAT,ME_TRCODE)' which becomes 'nvlbh_meicatme_trcode'
        
        # Find the '%' column (it becomes empty string after cleaning)
        percent_col = None
        nvl_col = None
        
        for col in df.columns:
            col_str = str(col).strip()
            # The '%' column is renamed to '_percent_column_' during cleaning
            if col_str == '_percent_column_' or col_str == '' or col_str == 'nan':
                percent_col = col
            # Find the NVL column - it becomes 'nvlbh_meicatme_trcode'
            elif 'nvlbh_meicat' in col_str.lower() or ('nvl' in col_str.lower() and 'meicat' in col_str.lower()):
                nvl_col = col
        
        # Map special columns
        if percent_col is not None:
            try:
                df_mapped['pymt_percent'] = pd.to_numeric(df[percent_col], errors='coerce')
                logger.info(f"  🔧 Mapped '%' column (empty string) → 'pymt_percent'")
            except Exception as e:
                logger.warning(f"  ⚠️  Could not map '%' column: {e}")
                df_mapped['pymt_percent'] = None
        
        if nvl_col is not None:
            df_mapped['bh_meicat_me_trcode'] = df[nvl_col]
            logger.info(f"  🔧 Mapped '{nvl_col}' → 'bh_meicat_me_trcode'")
        
        # Map all other columns that match exactly
        for col in table_columns:
            if col not in df_mapped.columns:
                if col in df.columns:
                    df_mapped[col] = df[col]
                else:
                    # Check if it's a special column we already handled
                    if col not in ['pymt_percent', 'bh_meicat_me_trcode']:
                        df_mapped[col] = None
        
        logger.info(f"  🔄 Mapped MPR Card columns: {len(df_mapped.columns)} columns")
        return df_mapped
    
    def _map_zomato_columns(self, df: pd.DataFrame, table_columns: list) -> pd.DataFrame:
        """
        Map Zomato Excel columns to zomato table columns
        Handles both direct column matches and SAP accounting format mappings
        Also handles "Store wise details" sheet format with direct column matches
        """
        import re
        df_mapped = pd.DataFrame(index=df.index)
        
        # Excel columns after cleaning: documentno, docdate, credit, debit, local_crcy_amt, 
        #                               text, assignment, pstng_date, bline_date, etc.
        # OR direct matches: order_id, amount, order_date, etc. (from "Store wise details" sheet)
        
        # Detect if this is "Store wise details" format (has direct column matches)
        has_direct_cols = 'order_id' in df.columns and 'amount' in df.columns and 'res_id' in df.columns
        is_store_wise_format = has_direct_cols
        
        # First, try direct column matches (case-insensitive, after cleaning)
        # This handles cases where Excel columns match database column names exactly
        direct_mappings = {}
        for db_col in table_columns:
            if db_col in ['uid', 'mapping_zomato_orders', 'created_at', 'updated_at']:
                continue  # Skip generated columns
            
            # Check if column exists in Excel (exact match after cleaning)
            if db_col in df.columns:
                direct_mappings[db_col] = db_col
                logger.info(f"  🔧 Direct match: '{db_col}' → '{db_col}'")
            else:
                # Check for case-insensitive match
                for excel_col in df.columns:
                    if excel_col.lower() == db_col.lower():
                        direct_mappings[db_col] = excel_col
                        logger.info(f"  🔧 Case-insensitive match: '{excel_col}' → '{db_col}'")
                        break
        
        # Apply direct mappings
        for db_col, excel_col in direct_mappings.items():
            if db_col.endswith('_date') or 'date' in db_col.lower():
                # Date columns: parse and convert to date type
                try:
                    # Handle integer date format (e.g., 20241204 for payout_date)
                    if df[excel_col].dtype in ['int64', 'int32', 'float64']:
                        # Check if it's a date integer (8 digits: YYYYMMDD)
                        sample_val = df[excel_col].dropna().iloc[0] if len(df[excel_col].dropna()) > 0 else None
                        if sample_val and 10000000 <= sample_val <= 99999999:
                            # Convert integer date (YYYYMMDD) to date
                            df_mapped[db_col] = pd.to_datetime(df[excel_col].astype(str), format='%Y%m%d', errors='coerce').dt.date
                            logger.info(f"  🔧 Converted integer date format for '{excel_col}' → '{db_col}'")
                        else:
                            # Try regular datetime parsing
                            df_mapped[db_col] = pd.to_datetime(df[excel_col], errors='coerce').dt.date
                    else:
                        # Already datetime or string - parse normally
                        df_mapped[db_col] = pd.to_datetime(df[excel_col], errors='coerce').dt.date
                except Exception as e:
                    logger.warning(f"  ⚠️  Date parsing error for '{excel_col}': {e}")
                    df_mapped[db_col] = None
            elif db_col in ['amount', 'net_amount', 'final_amount', 'total_amount', 'bill_subtotal',
                           'customer_compensation', 'pro_discount', 'commission_rate', 'zvd', 'tcs_amount',
                           'rejection_penalty_charge', 'user_credits_charge', 'promo_recovery_adj',
                           'icecream_handling', 'icecream_deductions', 'order_support_cost',
                           'pro_discount_passthrough', 'mvd', 'delivery_charge', 'commission_value',
                           'tax_rate', 'taxes_zomato_fee', 'credit_note_amount', 'tds_amount',
                           'pgcharge', 'logistics_charge', 'mdiscount', 'customer_discount',
                           'cancellation_refund', 'total_voucher', 'source_tax', 'tax_paid_by_customer',
                           'charged_by_zomato_agt_rejected_orders', 'percent', 'pg_chgs_percent',
                           'lg', 'merchant_delivery_charge', 'merchant_pack_charge']:
                # Numeric columns - convert to numeric, keep NaN as NaN (will become NULL in DB)
                # Don't convert 0 to NULL - 0 is a valid value from Excel
                df_mapped[db_col] = pd.to_numeric(df[excel_col], errors='coerce')
            elif db_col in ['order_id', 'res_id']:
                # Convert to string (may be int in Excel)
                df_mapped[db_col] = df[excel_col].astype(str)
            else:
                # String columns
                df_mapped[db_col] = df[excel_col].astype(str) if df[excel_col].dtype != 'object' else df[excel_col]
        
        # Handle special column name mappings (Excel column names that don't match DB names exactly)
        # Based on user's mapping table: Excel column names → Database column names
        special_mappings = {
            # Special characters (after cleaning: '%' becomes empty string, need to handle specially)
            '_percent_column_': 'percent',  # '%' column (handled specially in MPR Card, but check for zomato too)
            'percent_column': 'percent',
            # Column names with spaces (after cleaning: "Store Code" → "store_code")
            'store_code': 'store_code',  # "Store Code" → "store_code" after cleaning
            'store_name': 'store_name',  # "Store Name" → "store_name" after cleaning  
            'sap_code': 'sap_code',      # "SAP Code" → "sap_code" after cleaning
            'ls': 'ls',                  # "LS" → "ls" after cleaning
            'pg_chgs_percent': 'pg_chgs_percent',  # "PG Chgs %" → "pg_chgs_percent" after cleaning
            'charged_by_zomato_agt_rejected_orders': 'charged_by_zomato_agt_rejected_orders',  # Long name
        }
        
        # Also check original column names before cleaning for special cases
        # This handles cases where Excel has exact column names like "Store Code", "SAP Code", etc.
        original_to_db_mappings = {
            'Store Code': 'store_code',
            'Store Name': 'store_name',
            'SAP Code': 'sap_code',
            'LS': 'ls',
            'PG Chgs %': 'pg_chgs_percent',
            '%': 'percent',
            'Charged by Zomato agt rejected orders': 'charged_by_zomato_agt_rejected_orders',
        }
        
        # Check for special mappings in cleaned column names
        for excel_col in df.columns:
            excel_col_lower = excel_col.lower().strip()
            excel_col_cleaned = self.clean_column_name(excel_col)
            
            # First check original column name mappings
            if excel_col in original_to_db_mappings:
                db_col = original_to_db_mappings[excel_col]
                if db_col in table_columns and db_col not in df_mapped.columns:
                    if db_col in ['percent', 'pg_chgs_percent']:
                        df_mapped[db_col] = pd.to_numeric(df[excel_col], errors='coerce')
                    else:
                        df_mapped[db_col] = df[excel_col]
                    logger.info(f"  🔧 Special mapping (original): '{excel_col}' → '{db_col}'")
                    continue
            
            # Then check cleaned column name mappings
            for special_excel, db_col in special_mappings.items():
                if excel_col_lower == special_excel.lower() or excel_col_cleaned == special_excel:
                    if db_col in table_columns and db_col not in df_mapped.columns:
                        if db_col in ['percent', 'pg_chgs_percent']:
                            df_mapped[db_col] = pd.to_numeric(df[excel_col], errors='coerce')
                        else:
                            df_mapped[db_col] = df[excel_col]
                        logger.info(f"  🔧 Special mapping (cleaned): '{excel_col}' → '{db_col}'")
                        break
        
        # Now handle SAP accounting format mappings (for columns not already mapped)
        # These are fallback mappings for the old SAP format Excel files
        
        # 1. Map DocumentNo → order_id and res_id (if not already mapped)
        # Check for 'documentno' column (case-insensitive)
        docno_col = None
        for col in df.columns:
            if col.lower().strip() == 'documentno' or col.lower().strip() == 'document_no':
                docno_col = col
                break
        
        if docno_col:
            if 'order_id' in table_columns and 'order_id' not in df_mapped.columns:
                df_mapped['order_id'] = df[docno_col].astype(str)
                logger.info(f"  🔧 Mapped '{docno_col}' → 'order_id' ({df_mapped['order_id'].notna().sum()} non-null values)")
            if 'res_id' in table_columns and 'res_id' not in df_mapped.columns:
                df_mapped['res_id'] = df[docno_col].astype(str)
                logger.info(f"  🔧 Mapped '{docno_col}' → 'res_id'")
        else:
            logger.warning(f"  ⚠️  No 'documentno' column found. Available columns: {list(df.columns)}")
        
        # 2. Map Doc..Date → order_date and payout_date (if not already mapped)
        # Check for doc date column (case-insensitive, handles "Doc..Date" → "docdate" after cleaning)
        doc_date_col = None
        for col in df.columns:
            col_lower = col.lower().strip()
            if col_lower == 'docdate' or col_lower == 'doc__date' or col_lower == 'doc_date':
                doc_date_col = col
                break
        
        if doc_date_col:
            try:
                parsed_dates = pd.to_datetime(df[doc_date_col], errors='coerce', format='%d.%m.%Y')
                # Convert to date type (not datetime)
                if 'order_date' in table_columns and 'order_date' not in df_mapped.columns:
                    df_mapped['order_date'] = parsed_dates.dt.date
                    logger.info(f"  🔧 Mapped '{doc_date_col}' → 'order_date'")
                if 'payout_date' in table_columns and 'payout_date' not in df_mapped.columns:
                    df_mapped['payout_date'] = parsed_dates.dt.date
                    logger.info(f"  🔧 Mapped '{doc_date_col}' → 'payout_date'")
            except Exception as e:
                logger.warning(f"  ⚠️  Date parsing error: {e}, trying alternative format")
                parsed_dates = pd.to_datetime(df[doc_date_col], errors='coerce')
                if 'order_date' in table_columns and 'order_date' not in df_mapped.columns:
                    df_mapped['order_date'] = parsed_dates.dt.date
                if 'payout_date' in table_columns and 'payout_date' not in df_mapped.columns:
                    df_mapped['payout_date'] = parsed_dates.dt.date
        
        # 3. Map Pstng Date → payout_date and utr_date (if not already mapped)
        # Check for pstng date column (case-insensitive)
        pstng_date_col = None
        for col in df.columns:
            col_lower = col.lower().strip()
            if col_lower == 'pstng_date' or col_lower == 'pstngdate' or 'pstng' in col_lower and 'date' in col_lower:
                pstng_date_col = col
                break
        
        if pstng_date_col:
            try:
                parsed_dates = pd.to_datetime(df[pstng_date_col], errors='coerce', format='%d.%m.%Y')
                if 'payout_date' in table_columns and 'payout_date' not in df_mapped.columns:
                    df_mapped['payout_date'] = parsed_dates.dt.date
                if 'utr_date' in table_columns and 'utr_date' not in df_mapped.columns:
                    df_mapped['utr_date'] = parsed_dates.dt.date
                    logger.info(f"  🔧 Mapped '{pstng_date_col}' → 'utr_date' ({df_mapped['utr_date'].notna().sum()} non-null values)")
            except Exception as e:
                logger.warning(f"  ⚠️  Date parsing error for {pstng_date_col}: {e}")
                parsed_dates = pd.to_datetime(df[pstng_date_col], errors='coerce')
                if 'utr_date' in table_columns and 'utr_date' not in df_mapped.columns:
                    df_mapped['utr_date'] = parsed_dates.dt.date
        
        # 4. Map amounts: Use Credit if available, else use abs(Local Crcy Amt) for credits (if not already mapped)
        # Check for 'credit' column (case-insensitive, after cleaning it should be 'credit')
        credit_col = None
        for col in df.columns:
            if col.lower().strip() == 'credit':
                credit_col = col
                break
        
        local_crcy_col = None
        for col in df.columns:
            if col.lower().strip() == 'local_crcy_amt' or col.lower().strip() == 'localcrcyamt':
                local_crcy_col = col
                break
        
        if 'amount' not in df_mapped.columns:
            if credit_col:
                credit_amt = pd.to_numeric(df[credit_col], errors='coerce').fillna(0)
                if 'amount' in table_columns:
                    df_mapped['amount'] = credit_amt
                    logger.info(f"  🔧 Mapped '{credit_col}' → 'amount' (value range: {credit_amt.min():.2f} to {credit_amt.max():.2f})")
                if 'net_amount' in table_columns and 'net_amount' not in df_mapped.columns:
                    df_mapped['net_amount'] = credit_amt
                if 'final_amount' in table_columns and 'final_amount' not in df_mapped.columns:
                    df_mapped['final_amount'] = credit_amt
                if 'total_amount' in table_columns and 'total_amount' not in df_mapped.columns:
                    df_mapped['total_amount'] = credit_amt
                    logger.info(f"  🔧 Also mapped '{credit_col}' → 'total_amount'")
            elif local_crcy_col:
                local_amt = pd.to_numeric(df[local_crcy_col], errors='coerce')
                # If negative, it's a credit (use absolute value), if positive it's a debit
                amount = local_amt.abs()
                if 'amount' in table_columns:
                    df_mapped['amount'] = amount
                    logger.info(f"  🔧 Mapped '{local_crcy_col}' → 'amount' (absolute value, range: {amount.min():.2f} to {amount.max():.2f})")
                if 'net_amount' in table_columns and 'net_amount' not in df_mapped.columns:
                    df_mapped['net_amount'] = amount
                if 'final_amount' in table_columns and 'final_amount' not in df_mapped.columns:
                    df_mapped['final_amount'] = amount
                if 'total_amount' in table_columns and 'total_amount' not in df_mapped.columns:
                    df_mapped['total_amount'] = amount
            else:
                logger.warning(f"  ⚠️  No 'credit' or 'local_crcy_amt' column found for amount mapping. Available columns: {list(df.columns)}")
        
        # 5. Extract UTR number from Text field (if not already mapped)
        # Check for 'text' column (case-insensitive)
        text_col = None
        for col in df.columns:
            if col.lower().strip() == 'text':
                text_col = col
                break
        
        if 'utr_number' not in df_mapped.columns:
            if text_col and 'utr_number' in table_columns:
                def extract_utr(text_val):
                    if pd.isna(text_val):
                        return None
                    text_str = str(text_val)
                    # Pattern: CITIN followed by digits (e.g., CITIN22330463125)
                    utr_match = re.search(r'CITI[N]?(\d+)', text_str, re.IGNORECASE)
                    if utr_match:
                        return utr_match.group(1)
                    # Alternative pattern: UTR followed by alphanumeric
                    utr_match2 = re.search(r'UTR[:\s]*([A-Z0-9]+)', text_str, re.IGNORECASE)
                    if utr_match2:
                        return utr_match2.group(1)
                    return None
                
                df_mapped['utr_number'] = df[text_col].apply(extract_utr)
                utr_count = df_mapped['utr_number'].notna().sum()
                logger.info(f"  🔧 Extracted UTR numbers from '{text_col}' → 'utr_number' ({utr_count} UTRs found)")
        
        # 6. Map Assignment → payment_method (if not already mapped)
        # Check for 'assignment' column (case-insensitive)
        assignment_col = None
        for col in df.columns:
            if col.lower().strip() == 'assignment':
                assignment_col = col
                break
        
        if 'payment_method' not in df_mapped.columns:
            if assignment_col and 'payment_method' in table_columns:
                df_mapped['payment_method'] = df[assignment_col].astype(str)
                logger.info(f"  🔧 Mapped '{assignment_col}' → 'payment_method' ({df_mapped['payment_method'].notna().sum()} non-null values)")
        
        # 7. Set status (if not already mapped)
        if 'status' not in df_mapped.columns:
            if 'status' in table_columns:
                # If credit > 0, status is completed, else pending
                if credit_col:
                    credit_vals = pd.to_numeric(df[credit_col], errors='coerce').fillna(0)
                    df_mapped['status'] = credit_vals.apply(lambda x: 'completed' if x > 0 else 'pending')
                    completed_count = (df_mapped['status'] == 'completed').sum()
                    logger.info(f"  🔧 Set 'status' based on credit amount ({completed_count} completed, {len(df_mapped) - completed_count} pending)")
                else:
                    df_mapped['status'] = 'completed'
                    logger.info(f"  🔧 Set 'status' to 'completed' (no credit column found)")
        
        # 8. Set default values ONLY for columns that are truly not in Excel
        # For "Store wise details" format, columns that exist in Excel should be mapped directly
        # Only set defaults for columns that don't exist in Excel at all
        numeric_defaults = [
            'lg'  # Only 'lg' is not in Excel - all others should be mapped from Excel
        ]
        
        # For Store wise details format, most columns are already mapped from Excel
        # Only set defaults for columns that truly don't exist in Excel
        if is_store_wise_format:
            for col in numeric_defaults:
                if col in table_columns and col not in df_mapped.columns:
                    df_mapped[col] = 0.0  # Set to 0 for the one missing column
            # Don't set defaults for other columns - they should be mapped from Excel
            # If they're not mapped, it means they don't exist in Excel, so leave as None
        else:
            # For SAP format, use 0 as default for unmapped numeric columns
            numeric_defaults_sap = [
                'customer_compensation', 'pro_discount', 'commission_rate', 'zvd', 'tcs_amount',
                'rejection_penalty_charge', 'user_credits_charge', 'promo_recovery_adj',
                'icecream_handling', 'icecream_deductions', 'order_support_cost',
                'pro_discount_passthrough', 'mvd', 'delivery_charge', 'bill_subtotal',
                'commission_value', 'tax_rate', 'taxes_zomato_fee', 'credit_note_amount',
                'tds_amount', 'pgcharge', 'logistics_charge', 'mdiscount', 'customer_discount',
                'cancellation_refund', 'total_voucher', 'source_tax',
                'tax_paid_by_customer', 'charged_by_zomato_agt_rejected_orders',
                'percent', 'pg_chgs_percent', 'lg', 'merchant_delivery_charge',
                'merchant_pack_charge'
            ]
            for col in numeric_defaults_sap:
                if col in table_columns and col not in df_mapped.columns:
                    df_mapped[col] = 0.0
        
        # 9. Set string column defaults ONLY for columns that don't exist in Excel
        # For Store wise details format, most string columns are already mapped
        if not is_store_wise_format:
            # For SAP format, set defaults for unmapped string columns
            string_defaults = [
                'store_code', 'sap_code', 'store_name', 'city_id', 'city_name',
                'res_name', 'service_id', 'action', 'promo_code', 'ls', 'pg_applied_on'
            ]
            for col in string_defaults:
                if col in table_columns and col not in df_mapped.columns:
                    df_mapped[col] = None
        
        # 10. Map all other columns that match exactly (only if they exist in table)
        # IMPORTANT: Only map columns that are in table_columns to avoid inserting into non-existent columns
        # For zomato table, we should NOT map Excel columns like bline_date, debit, credit, etc.
        # because they don't exist in the actual zomato table structure
        excel_columns_to_ignore = ['bline_date', 'pstng_date', 'entry_date', 'debit', 'credit', 
                                   'local_crcy_amt', 'reference', 'assignment', 'text', 'custom', 
                                   'profi', 'curr']  # These are Excel columns, not table columns
        
        # Also ignore any columns that are unnamed/empty (should have been filtered, but double-check)
        for col in list(df.columns):
            if 'unnamed' in str(col).lower() or str(col).strip() == '':
                excel_columns_to_ignore.append(col)
        
        for col in table_columns:
            if col not in df_mapped.columns:
                # Skip if this is an Excel-only column that doesn't exist in the table
                if col in excel_columns_to_ignore:
                    continue
                    
                # Only map if column exists in Excel AND is in table_columns
                if col in df.columns:
                    # Additional check: make sure it's not a date column with header text as data
                    if col.endswith('_date') or 'date' in col.lower():
                        # For date columns, ensure we're not getting header text as data
                        # Check multiple rows to be sure
                        sample_vals = df[col].head(10) if len(df) > 10 else df[col]
                        header_like_count = 0
                        for val in sample_vals:
                            if pd.notna(val) and isinstance(val, str):
                                val_str = str(val).strip()
                                # Check if it looks like a header
                                if ('date' in val_str.lower() and val_str[0].isupper() and 
                                    val_str in ['Bline Date', 'Pstng Date', 'Entry Date', 'Doc..Date']):
                                    header_like_count += 1
                        
                        # If most samples look like headers, skip this column
                        if header_like_count > len(sample_vals) * 0.5:
                            logger.warning(f"  ⚠️  Skipping '{col}' - appears to contain header text, not data")
                            df_mapped[col] = None
                        else:
                            # Try to parse as date
                            try:
                                df_mapped[col] = pd.to_datetime(df[col], errors='coerce').dt.date
                            except:
                                df_mapped[col] = None
                    else:
                        # For non-date columns, check if values look like headers
                        sample_vals = df[col].head(10) if len(df) > 10 else df[col]
                        header_like_count = 0
                        for val in sample_vals:
                            if pd.notna(val) and isinstance(val, str):
                                val_str = str(val).strip()
                                # Check if it matches common header patterns
                                if val_str in ['Debit', 'Credit', 'DocumentNo', 'Custom', 'Profi', 
                                              'Reference', 'Assignment', 'Text', 'Curr.']:
                                    header_like_count += 1
                        
                        if header_like_count > len(sample_vals) * 0.5:
                            logger.warning(f"  ⚠️  Skipping '{col}' - appears to contain header text, not data")
                            df_mapped[col] = None
                        else:
                            df_mapped[col] = df[col]
                elif col not in ['uid', 'mapping_zomato_orders', 'created_at', 'updated_at']:  # Exclude generated/system columns
                    df_mapped[col] = None
        
        # Log summary of mapped columns
        mapped_summary = []
        for col in df_mapped.columns:
            if col in ['uid', 'mapping_zomato_orders', 'created_at', 'updated_at']:
                continue
            non_null = df_mapped[col].notna().sum()
            if df_mapped[col].dtype in ['float64', 'int64']:
                non_zero = (df_mapped[col] != 0).sum()
                if non_zero > 0:
                    mapped_summary.append(f"{col}: {non_null}/{len(df_mapped)} non-null, {non_zero} non-zero")
            else:
                if non_null > 0:
                    mapped_summary.append(f"{col}: {non_null}/{len(df_mapped)} non-null")
        
        logger.info(f"  🔄 Mapped Zomato columns: {len(df_mapped.columns)} columns")
        if mapped_summary:
            logger.info(f"  📊 Mapping summary: {', '.join(mapped_summary[:10])}{'...' if len(mapped_summary) > 10 else ''}")
        
        return df_mapped
    
    def upload_to_table(self, file_path: str, table_name: str, uid_prefix: str, 
                       skip_duplicates: bool = True) -> Tuple[bool, str]:
        """
        Upload Excel file to MySQL table with validation and deduplication
        
        Args:
            file_path: Path to Excel file
            table_name: Target MySQL table
            uid_prefix: Prefix for generated UIDs
            skip_duplicates: Skip if file already uploaded successfully
        """
        try:
            file_name = os.path.basename(file_path)
            
            # Check upload history
            if skip_duplicates and self.check_upload_history(table_name, file_name):
                return True, f"File already uploaded - skipped duplicate"
            
            # Detect file type and read accordingly
            file_ext = os.path.splitext(file_path)[1].lower()
            if file_ext == '.csv':
                logger.info(f"📥 Reading CSV file: {file_path}")
                df = pd.read_csv(file_path, low_memory=False)
            else:
                logger.info(f"📥 Reading Excel file: {file_path}")
                # Special handling for bank statements - skip header rows
                if table_name == 'fin_bank_statements':
                    logger.info(f"  🔧 Bank statements file: skipping header rows (starting from row 16)")
                    df = pd.read_excel(file_path, header=16)  # Row 16 (0-indexed) contains the actual headers
                elif table_name == 'zomato':
                    # For Zomato, ONLY use "Store wise details" sheet (target data)
                    try:
                        xl_file = pd.ExcelFile(file_path)
                        sheet_names = xl_file.sheet_names
                        logger.info(f"  📋 Found {len(sheet_names)} sheet(s): {', '.join(sheet_names[:5])}{'...' if len(sheet_names) > 5 else ''}")
                        
                        # Look for "Store wise details" sheet (exact match or closest match without "-2")
                        target_sheet = None
                        for sheet in sheet_names:
                            sheet_lower = sheet.lower()
                            # Prefer exact match "Store wise details" (without "-2")
                            if 'store' in sheet_lower and 'detail' in sheet_lower and '2' not in sheet_lower:
                                target_sheet = sheet
                                break
                        
                        if target_sheet:
                            logger.info(f"  ✅ Using TARGET sheet: \"{target_sheet}\" (Store wise details)")
                            # Read in chunks for large files to avoid memory issues
                            try:
                                # Try to read all at once first
                                df = pd.read_excel(file_path, sheet_name=target_sheet)
                                logger.info(f"  📊 Loaded {len(df):,} rows from \"{target_sheet}\" sheet")
                            except MemoryError:
                                logger.warning(f"  ⚠️  File too large, reading in chunks...")
                                # Read in chunks if file is too large
                                chunk_list = []
                                chunk_size = 100000
                                for chunk_start in range(0, xl_file.book[target_sheet].max_row, chunk_size):
                                    chunk = pd.read_excel(file_path, sheet_name=target_sheet, 
                                                         skiprows=chunk_start, nrows=chunk_size)
                                    if len(chunk) > 0:
                                        chunk_list.append(chunk)
                                        logger.info(f"  📊 Read chunk: rows {chunk_start:,} to {chunk_start + len(chunk):,}")
                                df = pd.concat(chunk_list, ignore_index=True)
                                logger.info(f"  ✅ Combined {len(df):,} total rows from chunks")
                        else:
                            # ERROR: "Store wise details" sheet not found - this is required for zomato
                            error_msg = f"'Store wise details' sheet not found in Excel file. Available sheets: {', '.join(sheet_names)}"
                            logger.error(f"  ❌ {error_msg}")
                            raise ValueError(error_msg)
                    except ValueError:
                        # Re-raise ValueError (sheet not found)
                        raise
                    except Exception as e:
                        error_msg = f"Error reading 'Store wise details' sheet: {e}"
                        logger.error(f"  ❌ {error_msg}")
                        raise ValueError(error_msg)
                else:
                    df = pd.read_excel(file_path)
            
            total_rows = len(df)
            logger.info(f"📊 Loaded {total_rows:,} rows")
            
            # Store original column names for special mapping (before cleaning)
            original_columns = list(df.columns)
            
            # Filter out unnamed/empty columns BEFORE cleaning (for all tables)
            original_col_count = len(df.columns)
            # Remove columns that are unnamed or empty
            valid_cols = []
            removed_cols = []
            for col in df.columns:
                col_str = str(col).strip()
                # Check if column is unnamed/empty
                is_unnamed = (
                    'Unnamed' in col_str or 
                    col_str == '' or 
                    col_str.lower() == 'nan' or
                    col_str.startswith('Unnamed:')
                )
                
                if is_unnamed:
                    removed_cols.append(col)
                    logger.debug(f"  🔧 Filtering out unnamed/empty column: '{col}'")
                else:
                    valid_cols.append(col)
            
            if len(valid_cols) < original_col_count:
                df = df[valid_cols]
                removed = original_col_count - len(valid_cols)
                logger.info(f"  🔧 Filtered out {removed} unnamed/empty column(s): {removed_cols[:5]}{'...' if len(removed_cols) > 5 else ''}")
                # Update original_columns to match
                original_columns = [col for col in original_columns if col in valid_cols]
            
            # Clean column names
            df.columns = [self.clean_column_name(col) for col in df.columns]
            logger.info(f"📋 Columns: {len(df.columns)} columns")
            
            # Filter out header rows (rows where all values match column names)
            # This is common in Excel files with repeated headers
            # For "Store wise details" format, skip header detection (it's clean data)
            if table_name == 'zomato':
                # Check if this is "Store wise details" format (has direct column matches)
                has_direct_cols = 'order_id' in df.columns or 'res_id' in df.columns
                
                if has_direct_cols:
                    # "Store wise details" format - skip header detection (too slow for 788k rows)
                    # Just check first few rows quickly for any obvious header rows
                    logger.info(f"  🔧 Store wise details format detected - skipping slow header detection")
                    if len(df) > 0:
                        # Quick check: if first row looks like headers, remove it
                        first_row = df.iloc[0]
                        header_match_count = 0
                        for col in df.columns:
                            val = first_row[col]
                            if pd.notna(val):
                                val_str = str(val).strip()
                                # Check if value matches any column name
                                if any(val_str.lower() == str(c).lower() for c in original_columns):
                                    header_match_count += 1
                        
                        # If first row has many matches with column names, it's likely a header
                        if header_match_count > len(df.columns) * 0.5:
                            df = df.iloc[1:].reset_index(drop=True)
                            logger.info(f"  🔧 Removed first row (appeared to be header)")
                else:
                    # SAP format - do header detection but optimize for large files
                    logger.info(f"  🔧 SAP format detected - checking for header rows (sampling method)")
                    original_len = len(df)
                    header_rows = []
                    
                    # For large files, only check first 1000 rows and last 100 rows
                    # This is much faster than checking all 788k rows
                    sample_size = min(1000, len(df))
                    check_indices = list(df.index[:sample_size]) + list(df.index[-100:])
                    
                    original_col_names = [str(c).strip().lower() for c in original_columns]
                    cleaned_col_names = [str(c).strip().lower() for c in df.columns]
                    
                    for idx in check_indices:
                        row = df.loc[idx]
                        matches = 0
                        total_vals = 0
                        
                        for col in df.columns:
                            val = row[col]
                            if pd.notna(val):
                                total_vals += 1
                                val_str = str(val).strip().lower()
                                
                                if val_str in original_col_names or val_str in cleaned_col_names:
                                    matches += 1
                        
                        if total_vals > 0 and matches / total_vals > 0.6:
                            header_rows.append(idx)
                    
                    if header_rows:
                        df = df.drop(index=header_rows)
                        logger.info(f"  🔧 Removed {len(header_rows)} header row(s) from sample check")
                        if len(df) == 0:
                            logger.warning(f"  ⚠️  All rows were removed as header rows!")
                        else:
                            logger.info(f"  📊 After removing header rows: {len(df):,} data rows remaining")
            
            # For MPR Card, handle the '%' column that becomes empty string
            if table_name == 'mpr_hdfc_card':
                # Find the '%' column by checking original column names
                for idx, orig_col in enumerate(original_columns):
                    if str(orig_col).strip() == '%':
                        # Rename the empty string column to a temporary name
                        if df.columns[idx] == '' or str(df.columns[idx]).strip() == '':
                            df.columns.values[idx] = '_percent_column_'
                            logger.info(f"  🔧 Identified '%' column at position {idx}, renamed to '_percent_column_'")
                        break
            
            # For zomato table, filter out rows with null/invalid order_id or res_id BEFORE mapping
            # This applies to both SAP format (DocumentNo) and Store wise details format (order_id/res_id)
            if table_name == 'zomato':
                original_len = len(df)
                before_filter = len(df)
                
                # Check if this is "Store wise details" format (has order_id/res_id directly)
                if 'order_id' in df.columns:
                    # Store wise details format - filter by order_id
                    df = df[df['order_id'].notna()]
                    df = df[df['order_id'].astype(str).str.strip() != '']
                    df = df[df['order_id'].astype(str).str.lower() != 'nan']
                    logger.info(f"  🔧 Filtered rows with null/invalid order_id (Store wise details format)")
                elif 'res_id' in df.columns:
                    # Store wise details format - filter by res_id if order_id not available
                    df = df[df['res_id'].notna()]
                    df = df[df['res_id'].astype(str).str.strip() != '']
                    df = df[df['res_id'].astype(str).str.lower() != 'nan']
                    logger.info(f"  🔧 Filtered rows with null/invalid res_id (Store wise details format)")
                else:
                    # SAP format - filter by DocumentNo
                    docno_col = None
                    for col in df.columns:
                        if col.lower().strip() == 'documentno' or col.lower().strip() == 'document_no':
                            docno_col = col
                            break
                    
                    if docno_col:
                        df = df[df[docno_col].notna()]
                        df = df[df[docno_col].astype(str).str.strip() != '']
                        df = df[df[docno_col].astype(str).str.lower() != 'nan']
                        logger.info(f"  🔧 Filtered rows with null/invalid DocumentNo (SAP format)")
                    else:
                        logger.warning(f"  ⚠️  Could not find 'order_id', 'res_id', or 'documentno' column. Available columns: {list(df.columns)[:10]}...")
                
                after_filter = len(df)
                removed = before_filter - after_filter
                if removed > 0:
                    logger.info(f"  🔧 Filtered out {removed:,} invalid row(s) (kept {after_filter:,} valid rows)")
                    total_rows = len(df)
            
            # Clean data
            df = self.clean_dataframe(df)
            
            # Connect to database
            with self.db_manager.get_mysql_connector() as conn:
            cursor = conn.cursor()
            
            # Get current record count
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            records_before = cursor.fetchone()[0]
            
            # Get table structure
            cursor.execute(f"DESCRIBE {table_name}")
            table_structure = cursor.fetchall()
            table_columns = [row[0] for row in table_structure]
            
            # Identify generated columns using INFORMATION_SCHEMA (more reliable)
            generated_columns = set()
            try:
                cursor.execute("""
                    SELECT COLUMN_NAME 
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_SCHEMA = %s 
                      AND TABLE_NAME = %s 
                      AND GENERATION_EXPRESSION IS NOT NULL 
                      AND GENERATION_EXPRESSION != ''
                """, (self.db_config['database'], table_name))
                generated_columns = {row[0] for row in cursor.fetchall()}
            except Exception as e:
                # Fallback: check Extra field from DESCRIBE
                logger.warning(f"  ⚠️  Could not query INFORMATION_SCHEMA, using fallback method: {e}")
                for row in table_structure:
                    if len(row) > 5:
                        extra = str(row[5] or '')
                        if 'GENERATED' in extra.upper():
                            generated_columns.add(row[0])
            
            # Exclude uid and generated columns from insert
            table_columns_no_uid = [col for col in table_columns 
                                   if col != 'uid' and col not in generated_columns]
            
            if generated_columns:
                logger.info(f"  ⚠️  Excluding {len(generated_columns)} generated column(s): {', '.join(generated_columns)}")
            
            # Special mapping for specific tables
            if table_name == 'fin_bank_statements':
                df_mapped = self._map_bank_statements_columns(df.copy())
            elif table_name == 'orders':
                df_mapped = self._map_orders_columns(df.copy(), table_columns_no_uid)
            elif table_name == 'mpr_hdfc_upi':
                df_mapped = self._map_mpr_upi_columns(df.copy(), table_columns_no_uid)
            elif table_name == 'mpr_hdfc_card':
                df_mapped = self._map_mpr_card_columns(df.copy(), table_columns_no_uid)
            elif table_name == 'zomato':
                df_mapped = self._map_zomato_columns(df.copy(), table_columns_no_uid)
            else:
                # Map columns (exact match)
                available_cols = [col for col in table_columns_no_uid if col in df.columns]
                df_mapped = df[available_cols].copy()
            
            # Remove any generated columns that might have been added by mapping
            if generated_columns:
                for gen_col in generated_columns:
                    if gen_col in df_mapped.columns:
                        df_mapped = df_mapped.drop(columns=[gen_col])
                        logger.info(f"  🔧 Removed generated column '{gen_col}' from dataframe")
            
            # Add missing columns as None (only non-generated columns)
            # BUT exclude created_at and updated_at for orders table - let MySQL use DEFAULT
            # Also exclude mapping_zomato_orders for zomato table - it's a generated column
            columns_to_exclude_from_insert = []
            if table_name == 'orders':
                columns_to_exclude_from_insert = ['created_at', 'updated_at']
                logger.info(f"  🔧 Excluding 'created_at' and 'updated_at' from INSERT - MySQL will use DEFAULT (now())")
            elif table_name == 'zomato':
                columns_to_exclude_from_insert = ['created_at', 'updated_at', 'mapping_zomato_orders']
                logger.info(f"  🔧 Excluding 'created_at', 'updated_at', and 'mapping_zomato_orders' from INSERT - MySQL will use DEFAULT/generated values")
            
            missing_count = 0
            for col in table_columns_no_uid:
                if col not in df_mapped.columns:
                    if col not in columns_to_exclude_from_insert:
                        df_mapped[col] = None
                        missing_count += 1
                    # For excluded columns, don't add them - they'll use DB defaults
            
            if missing_count > 0:
                logger.info(f"  ⚠️  Added {missing_count} missing columns with NULL values")
            
            # Generate required fields for fin_bank_statements table
            if table_name == 'fin_bank_statements':
                # Generate stmt_line_id if missing
                if 'stmt_line_id' not in df_mapped.columns or df_mapped['stmt_line_id'].isna().all():
                    date_str = datetime.now().strftime('%Y%m%d')
                    df_mapped['stmt_line_id'] = [f"BR_{date_str}_{i+1}" for i in range(len(df_mapped))]
                    logger.info(f"  🔧 Generated stmt_line_id for {len(df_mapped)} records")
                
                # Generate account_id if missing
                if 'account_id' not in df_mapped.columns or df_mapped['account_id'].isna().all():
                    df_mapped['account_id'] = 'BANK_RECEIPT_DEFAULT'
                    logger.info(f"  🔧 Set default account_id: BANK_RECEIPT_DEFAULT")
                
                # Generate ingest_run_id if missing
                if 'ingest_run_id' not in df_mapped.columns or df_mapped['ingest_run_id'].isna().all():
                    run_id = f"INGEST_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                    df_mapped['ingest_run_id'] = run_id
                    logger.info(f"  🔧 Generated ingest_run_id: {run_id}")
            
            # For orders and zomato tables, exclude created_at and updated_at from INSERT (let MySQL use DEFAULT)
            if table_name == 'orders':
                columns_to_insert = [col for col in table_columns_no_uid if col not in ['created_at', 'updated_at']]
                df_mapped = df_mapped[columns_to_insert]
                logger.info(f"  🔧 Excluded 'created_at' and 'updated_at' from INSERT - MySQL will use DEFAULT")
            elif table_name == 'zomato':
                columns_to_insert = [col for col in table_columns_no_uid if col not in columns_to_exclude_from_insert]
                df_mapped = df_mapped[columns_to_insert]
                logger.info(f"  🔧 Excluded 'created_at', 'updated_at', and 'mapping_zomato_orders' from INSERT - MySQL will use DEFAULT/generated")
            else:
                # Reorder to match table structure
                df_mapped = df_mapped[table_columns_no_uid]
            
            # Remove duplicate rows based on order_id (if present) before generating UIDs
            if 'order_id' in df_mapped.columns and table_name == 'zomato':
                original_count = len(df_mapped)
                # Remove rows where order_id is null/empty first
                df_mapped = df_mapped[df_mapped['order_id'].notna()]
                df_mapped = df_mapped[df_mapped['order_id'].astype(str).str.strip() != '']
                # Then remove duplicates, keeping the first occurrence
                df_mapped = df_mapped.drop_duplicates(subset=['order_id'], keep='first')
                duplicates_removed = original_count - len(df_mapped)
                if duplicates_removed > 0:
                    logger.warning(f"  ⚠️  Removed {duplicates_removed} duplicate/invalid row(s) based on order_id (kept {len(df_mapped):,} unique rows)")
                    total_rows = len(df_mapped)
            
            # Add uid column only if table has uid column (orders table doesn't have uid)
            # Generate unique UIDs using timestamp + index to avoid duplicates
            if 'uid' in table_columns:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                # Use order_id if available to make UID more unique, otherwise use index
                if 'order_id' in df_mapped.columns:
                    df_mapped.insert(0, 'uid', [f"{uid_prefix}_{timestamp}_{str(row['order_id']).replace(' ', '_').replace('/', '_')}_{i+1}" 
                                                for i, (_, row) in enumerate(df_mapped.iterrows())])
                else:
                    df_mapped.insert(0, 'uid', [f"{uid_prefix}_{timestamp}_{i+1}" for i in range(len(df_mapped))])
                logger.info(f"  🔧 Generated {len(df_mapped)} unique UIDs with prefix '{uid_prefix}'")
            
            # Clear existing data
            logger.info(f"🗑️  Clearing table {table_name}...")
            cursor.execute(f"TRUNCATE TABLE {table_name}")
            
            # Insert in batches
            logger.info(f"💾 Inserting {total_rows:,} records in batches of {self.batch_size}...")
            inserted = 0
            
            for start_idx in range(0, total_rows, self.batch_size):
                end_idx = min(start_idx + self.batch_size, total_rows)
                batch = df_mapped.iloc[start_idx:end_idx]
                
                values = []
                for _, row in batch.iterrows():
                    row_values = tuple(None if pd.isna(val) else val for val in row)
                    values.append(row_values)
                
                columns = ', '.join([f'`{col}`' for col in df_mapped.columns])
                placeholders = ', '.join(['%s'] * len(df_mapped.columns))
                query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                
                cursor.executemany(query, values)
                conn.commit()
                
                inserted += len(batch)
                if inserted % 10000 == 0 or inserted == total_rows:
                    logger.info(f"  ✅ Inserted {inserted:,}/{total_rows:,} records ({inserted/total_rows*100:.1f}%)")
            
            # Get final count
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            records_after = cursor.fetchone()[0]
            
            cursor.close()
            conn.close()
            
            # Record success
            self.record_upload(table_name, file_path, file_name, records_before, 
                             inserted, records_after, 'SUCCESS')
            
            logger.info(f"✅ Upload complete! {inserted:,} records inserted into {table_name}")
            return True, f"Successfully uploaded {inserted:,} records"
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"❌ Upload failed: {error_msg}")
            
            # Record failure
            self.record_upload(table_name, file_path, file_name, 
                             records_before if 'records_before' in locals() else 0,
                             inserted if 'inserted' in locals() else 0,
                             records_before if 'records_before' in locals() else 0,
                             'FAILED', error_msg)
            
            return False, error_msg


def main():
    """CLI entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Upload data files to MySQL')
    parser.add_argument('--file', required=True, help='Path to Excel file')
    parser.add_argument('--table', required=True, help='Target table name')
    parser.add_argument('--prefix', required=True, help='UID prefix (e.g., TRM, MPR_UPI)')
    parser.add_argument('--force', action='store_true', help='Force upload even if duplicate')
    
    args = parser.parse_args()
    
    uploader = DataUploader()
    success, message = uploader.upload_to_table(
        args.file, 
        args.table, 
        args.prefix,
        skip_duplicates=not args.force
    )
    
    if success:
        logger.info(f"✅ {message}")
        sys.exit(0)
    else:
        logger.error(f"❌ {message}")
        sys.exit(1)


if __name__ == '__main__':
    main()
