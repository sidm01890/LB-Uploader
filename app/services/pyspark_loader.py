#!/usr/bin/env python3
"""
PySpark-Enhanced Data Loader for Financial Reconciliation Platform
Combines file management with high-performance PySpark processing
Author: Reconcii Automation
Updated: 2025-11-07
"""

import os
import sys
import re
import logging
import traceback
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime
import time

import pandas as pd
import mysql.connector
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from pyspark.sql.functions import (
    col, trim, lower, lit, coalesce, when,
    date_format, regexp_replace, udf, to_timestamp, to_date, try_to_timestamp
)
from pyspark.sql.types import StringType, TimestampType
from pyspark.sql.utils import AnalysisException

from app.services.file_manager import FileManager, FileSource
from app.config import MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB

logger = logging.getLogger(__name__)

class PySparkDataLoader:
    """
    High-performance data loader using PySpark
    Handles large files efficiently with distributed processing
    """
    
    def __init__(self, app_name: str = "FinancialReconciliation_ETL"):
        self.file_manager = FileManager()
        self.spark = self._init_spark(app_name)
        # Use devyani database explicitly
        db_name = 'devyani'
        self.mysql_url = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{db_name}?allowPublicKeyRetrieval=true&useSSL=false"
        logger.info(f"🗄️ Using database: {db_name}")
    
    def _init_spark(self, app_name: str) -> SparkSession:
        """Initialize Spark session with MySQL connector"""
        try:
            spark = (
                SparkSession.builder
                .appName(app_name)
                .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.33")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .config("spark.driver.memory", "4g")  # Increase driver memory
                .config("spark.executor.memory", "4g")  # Increase executor memory
                .config("spark.driver.maxResultSize", "2g")  # Increase max result size
                .getOrCreate()
            )
            spark.sparkContext.setLogLevel("ERROR")
            logger.info("✅ Spark session initialized")
            return spark
        except Exception as e:
            logger.error(f"❌ Failed to initialize Spark: {e}")
            raise
    
    def clean_column_name(self, name: str) -> str:
        """
        Clean and normalize column names
        Example: "Order Date/Time" → "order_date_time"
        """
        return (
            name.strip()
            .lower()
            .replace(" ", "_")
            .replace("/", "_")
            .replace("-", "_")
            .replace("(", "")
            .replace(")", "")
            .replace(".", "_")
        )
    
    def parse_date_safe(self, date_str: str) -> Optional[str]:
        """
        Fallback date parser for complex formats
        Handles: "Monday, 15 December 2024", "15-Dec-24", etc.
        Returns: String in format "yyyy-MM-dd HH:mm:ss" or None
        """
        if not date_str or not isinstance(date_str, str):
            return None
        
        date_str = date_str.strip()
        
        # Remove weekday prefixes
        date_str = re.sub(
            r"^(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s*",
            "", date_str, flags=re.IGNORECASE
        )
        
        # Known formats to try (order matters - most specific first)
        known_formats = [
            "%Y-%m-%d %H:%M:%S",      # 2024-12-01 14:30:00
            "%Y-%m-%d",                # 2024-12-01
            "%d-%m-%Y %H:%M:%S",       # 01-12-2024 14:30:00
            "%d-%m-%Y",                # 01-12-2024
            "%d/%m/%Y %H:%M:%S",       # 01/12/2024 14:30:00
            "%d/%m/%Y",                # 01/12/2024
            "%m/%d/%Y %H:%M:%S",       # 12/01/2024 14:30:00
            "%m/%d/%Y",                # 12/01/2024
            "%d-%b-%Y %H:%M:%S",       # 01-Dec-2024 14:30:00
            "%d-%b-%Y",                # 01-Dec-2024
            "%d-%B-%Y",                # 01-December-2024
            "%B %d, %Y %H:%M:%S",      # December 01, 2024 14:30:00
            "%B %d, %Y",               # December 01, 2024
            "%b %d, %Y",               # Dec 01, 2024
            "%d %B %Y",                # 01 December 2024
            "%d %b %Y",                # 01 Dec 2024
            "%Y%m%d",                  # 20241201
            "%d.%m.%Y",                # 01.12.2024
            "%d-%b-%y",                # 01-Dec-24
            "%d/%b/%Y",                # 01/Dec/2024
        ]
        
        for fmt in known_formats:
            try:
                parsed_date = datetime.strptime(date_str, fmt)
                # Always return with time component
                return parsed_date.strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                continue
        
        logger.warning(f"Could not parse date: {date_str}")
        return None
    
    def detect_header_row(self, file_path: Path) -> int:
        """
        Detect which row contains the actual data headers in Excel files
        
        Args:
            file_path: Path to Excel file
        
        Returns:
            Row number where headers start (0-indexed)
        """
        try:
            # Read first 30 rows to find header row
            pdf_sample = pd.read_excel(file_path, nrows=30, header=None)
            
            best_row = 0
            best_score = 0
            
            for idx, row in pdf_sample.iterrows():
                # Look for common header keywords
                row_str = ' '.join([str(x).upper() for x in row if pd.notna(x)])
                
                # Scoring system - more specific keywords get higher scores
                score = 0
                if 'DATE' in row_str:
                    score += 3
                if 'AMOUNT' in row_str or 'DEPOSITS' in row_str or 'WITHDRAWALS' in row_str:
                    score += 3
                if 'PARTICULARS' in row_str or 'NARRATION' in row_str or 'DESCRIPTION' in row_str:
                    score += 2
                if 'BALANCE' in row_str:
                    score += 2
                if 'MODE' in row_str or 'TYPE' in row_str:
                    score += 1
                
                # Check if next row looks like data (has numbers/dates)
                if idx + 1 < len(pdf_sample):
                    next_row_str = ' '.join([str(x) for x in pdf_sample.iloc[idx + 1] if pd.notna(x)])
                    # If next row has numbers, add bonus score
                    if any(char.isdigit() for char in next_row_str):
                        score += 2
                
                if score > best_score:
                    best_score = score
                    best_row = idx
            
            if best_score >= 5:  # Require minimum confidence
                logger.info(f"📍 Detected header row at index {best_row} (confidence score: {best_score})")
                return best_row
            else:
                logger.warning(f"⚠️ Low confidence ({best_score}), using default (row 0)")
                return 0
            
        except Exception as e:
            logger.warning(f"⚠️ Header detection failed: {e}, using default")
            return 0
    
    def load_file_to_spark(self, file_path: Path) -> SparkDataFrame:
        """
        Load file (CSV/Excel) into Spark DataFrame
        
        Args:
            file_path: Path to file
        
        Returns:
            Spark DataFrame
        """
        file_ext = file_path.suffix.lower()
        
        try:
            if file_ext in ['.xlsx', '.xls']:
                logger.info(f"📥 Reading Excel file: {file_path.name}")
                
                # Detect header row
                header_row = self.detect_header_row(file_path)
                
                # Use pandas for Excel, read all as strings to avoid type conflicts
                # Let Spark handle type inference consistently
                pdf = pd.read_excel(file_path, header=header_row, dtype=str)
                
                # Remove empty rows
                pdf = pdf.dropna(how='all')
                
                # Create Spark DataFrame - Spark will infer all as StringType initially
                df = self.spark.createDataFrame(pdf)
            elif file_ext == '.csv':
                logger.info(f"📥 Reading CSV file: {file_path.name}")
                df = self.spark.read.csv(
                    str(file_path),
                    header=True,
                    inferSchema=True,
                    multiLine=True,
                    escape='"'
                )
            else:
                raise ValueError(f"Unsupported file format: {file_ext}")
            
            logger.info(f"✅ Loaded {df.count()} rows with {len(df.columns)} columns")
            return df
            
        except Exception as e:
            logger.error(f"❌ Failed to load file: {e}")
            raise
    
    def clean_dataframe(self, df: SparkDataFrame) -> SparkDataFrame:
        """
        Clean DataFrame: normalize columns, handle dates, trim strings
        
        Args:
            df: Input Spark DataFrame
        
        Returns:
            Cleaned Spark DataFrame
        """
        # Clean column names
        df = df.toDF(*[self.clean_column_name(c) for c in df.columns])
        logger.info("🧹 Cleaned column names")
        
        # Identify date/time columns (exclude pure time columns like bh_time which are just strings)
        date_cols = [
            c for c in df.columns 
            if any(keyword in c for keyword in ["date", "timestamp", "created", "updated"])
            or (c.endswith("_time") and c != "bh_time")  # Include _time suffixes but exclude bh_time
        ]
        
        if date_cols:
            logger.info(f"🕒 Normalizing {len(date_cols)} date/time columns: {date_cols}")
            
            for c in date_cols:
                try:
                    # First, ensure the column is string type
                    df = df.withColumn(c, trim(col(c).cast(StringType())))
                    
                    # Remove weekday names (Monday, Tuesday, etc.)
                    df = df.withColumn(
                        c,
                        regexp_replace(
                            col(c),
                            r"^(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s*",
                            ""
                        )
                    )
                    
                    # Try multiple date formats using try_to_timestamp (PySpark 4.0)
                    # Each format that fails returns null, coalesce picks first non-null
                    df = df.withColumn(
                        c,
                        coalesce(
                            # ISO formats with time
                            try_to_timestamp(col(c), lit("yyyy-MM-dd HH:mm:ss")),
                            try_to_timestamp(col(c), lit("yyyy-MM-dd'T'HH:mm:ss")),
                            # ISO format date only
                            try_to_timestamp(col(c), lit("yyyy-MM-dd")),
                            # European formats (DD-MM-YYYY)
                            try_to_timestamp(col(c), lit("dd-MM-yyyy HH:mm:ss")),
                            try_to_timestamp(col(c), lit("dd-MM-yyyy")),
                            try_to_timestamp(col(c), lit("dd/MM/yyyy HH:mm:ss")),
                            try_to_timestamp(col(c), lit("dd/MM/yyyy")),
                            # US formats (MM/DD/YYYY)
                            try_to_timestamp(col(c), lit("MM/dd/yyyy HH:mm:ss")),
                            try_to_timestamp(col(c), lit("MM/dd/yyyy")),
                            # Month name formats
                            try_to_timestamp(col(c), lit("d MMMM yyyy HH:mm:ss")),
                            try_to_timestamp(col(c), lit("d MMMM yyyy")),
                            try_to_timestamp(col(c), lit("d MMM yyyy")),
                            try_to_timestamp(col(c), lit("MMMM d, yyyy")),
                            try_to_timestamp(col(c), lit("MMM d, yyyy")),
                            try_to_timestamp(col(c), lit("dd-MMM-yyyy")),
                            try_to_timestamp(col(c), lit("dd-MMM-yy")),
                            try_to_timestamp(col(c), lit("dd/MMM/yyyy")),
                            # Compact formats
                            try_to_timestamp(col(c), lit("yyyyMMdd")),
                            try_to_timestamp(col(c), lit("dd.MM.yyyy")),
                            # Return NULL if all formats fail (don't keep unparseable string)
                            lit(None).cast(TimestampType())
                        )
                    )
                    
                    # Log success
                    logger.info(f"✅ Normalized date column: {c}")
                    
                except Exception as e:
                    logger.warning(f"⚠️ Date normalization failed for {c}: {e}")
        
        # Trim all string columns
        for c in df.columns:
            if c not in date_cols:
                df = df.withColumn(c, trim(col(c).cast(StringType())))
        
        logger.info("✅ DataFrame cleaned and normalized")
        return df
    
    def transform_bank_receipt(self, df: SparkDataFrame) -> SparkDataFrame:
        """
        Transform Bank Receipt data to match fin_bank_statements schema
        
        Excel columns: DATE, MODE, PARTICULARS, DEPOSITS, WITHDRAWALS, BALANCE
        Target schema: stmt_line_id, account_id, statement_date, value_date, narration,
                      amount, debit_credit, balance_after, bank_ref, counterparty, memo, ingest_run_id
        """
        from pyspark.sql.functions import (
            when, coalesce, lit, concat, date_format, monotonically_increasing_id,
            regexp_extract, current_timestamp, col
        )
        from uuid import uuid4
        
        logger.info("🔄 Transforming Bank Receipt data to fin_bank_statements schema")
        
        # Generate unique ingest_run_id for this batch
        ingest_run_id = f"BR_{uuid4().hex[:12]}"
        
        # First, cast numeric columns to proper types
        df = df.withColumn("deposits", col("deposits").cast("decimal(15,4)")) \
               .withColumn("withdrawals", col("withdrawals").cast("decimal(15,4)")) \
               .withColumn("balance", col("balance").cast("decimal(15,4)"))
        
        # Transform DataFrame
        df_transformed = df.select(
            # stmt_line_id: unique identifier for each row
            concat(
                lit("BR_"),
                date_format(col("date"), "yyyyMMdd"),
                lit("_"),
                monotonically_increasing_id().cast("string")
            ).alias("stmt_line_id"),
            
            # account_id: hardcoded for Bank Receipt (can be configured later)
            lit("BANK_RECEIPT_DEFAULT").alias("account_id"),
            
            # statement_date: from DATE column
            col("date").cast("date").alias("statement_date"),
            
            # value_date: same as statement_date
            col("date").cast("date").alias("value_date"),
            
            # narration: from PARTICULARS
            col("particulars").alias("narration"),
            
            # amount: DEPOSITS if > 0, else WITHDRAWALS, else 0
            coalesce(
                when((col("deposits").isNotNull()) & (col("deposits") > 0), col("deposits")),
                when((col("withdrawals").isNotNull()) & (col("withdrawals") > 0), col("withdrawals")),
                lit(0)
            ).cast("decimal(15,4)").alias("amount"),
            
            # debit_credit: CREDIT for deposits, DEBIT for withdrawals
            when((col("deposits").isNotNull()) & (col("deposits") > 0), lit("CREDIT"))
            .when((col("withdrawals").isNotNull()) & (col("withdrawals") > 0), lit("DEBIT"))
            .otherwise(lit("CREDIT")).alias("debit_credit"),
            
            # balance_after: from BALANCE column
            col("balance").cast("decimal(15,4)").alias("balance_after"),
            
            # bank_ref: from MODE column (or extract from particulars)
            coalesce(col("mode"), lit("")).alias("bank_ref"),
            
            # counterparty: extract from PARTICULARS (NEFT/RTGS patterns)
            regexp_extract(
                col("particulars"),
                r"(?:NEFT|RTGS|IMPS|UPI)-[^-]+-([^-]+)", 
                1
            ).alias("counterparty"),
            
            # memo: copy of PARTICULARS for reference
            col("particulars").alias("memo"),
            
            # ingest_run_id: unique batch identifier
            lit(ingest_run_id).alias("ingest_run_id")
        )
        
        logger.info(f"✅ Transformed to fin_bank_statements schema ({df_transformed.count()} rows)")
        return df_transformed
    
    def transform_trm(self, df: SparkDataFrame) -> SparkDataFrame:
        """
        Transform TRM (Pine Labs) data to match trm table schema
        
        Source: Pine Labs transaction reports (39 columns)
        Target: trm table (28 columns)
        """
        from pyspark.sql.functions import md5, concat_ws, col, lower, trim
        
        logger.info("🔄 Transforming TRM data to trm schema")
        
        # Clean column names (convert to lowercase with underscores)
        for c in df.columns:
            new_name = c.lower().replace(' ', '_')
            df = df.withColumnRenamed(c, new_name)
        
        # Generate UID from transaction_id + date + store_name
        df = df.withColumn("uid", 
            md5(concat_ws("_", 
                col("transaction_id"), 
                col("date"), 
                col("store_name")
            ))
        )
        
        # Cast numeric columns
        df = df.withColumn("amount", col("amount").cast("double")) \
               .withColumn("tip_amount", col("tip_amount").cast("double"))
        
        # Select and map columns to target schema
        df_transformed = df.select(
            col("uid"),
            col("zone"),
            col("store_name"),
            col("city"),
            col("pos"),
            col("hardware_model"),
            col("hardware_id"),
            col("acquirer"),
            col("tid"),
            col("mid"),
            col("batch_no"),
            col("payment_mode"),
            col("customer_payment_mode_id"),
            col("name"),
            col("card_issuer"),
            col("card_type"),
            col("card_network"),
            col("card_colour"),
            col("transaction_id"),
            col("invoice"),
            col("approval_code"),
            col("type"),
            col("amount"),
            col("tip_amount"),
            col("currency"),
            col("date"),
            col("batch_status"),
            col("txn_status")
        )
        
        logger.info(f"✅ Transformed to trm schema ({df_transformed.count()} rows)")
        return df_transformed
    
    def transform_mpr_upi(self, df: SparkDataFrame) -> SparkDataFrame:
        """
        Transform MPR HDFC UPI data to match mpr_hdfc_upi table schema
        
        Source: HDFC UPI transaction reports (28 columns)
        Target: mpr_hdfc_upi table (24 columns)
        """
        from pyspark.sql.functions import md5, concat_ws, col, to_timestamp, lit, when
        
        logger.info("🔄 Transforming MPR UPI data to mpr_hdfc_upi schema")
        
        # Clean column names
        column_mapping = {
            'External MID': 'external_mid',
            'External TID': 'external_tid',
            'UPI Merchant ID': 'upi_merchant_id',
            'Merchant Name': 'merchant_name',
            'Merchant VPA': 'merchant_vpa',
            'Payer VPA': 'payer_vpa',
            'UPI Trxn ID': 'upi_trxn_id',
            'Order ID': 'order_id',
            'Customer Ref No. (RRN)': 'customer_ref_no',
            'Transaction Req Date': 'transaction_req_date',
            'Settlement Date': 'settlement_date',
            'Currency': 'currency',
            'Transaction Amount': 'transaction_amount',
            'MSF Amount': 'msf_amount',
            'CGST AMT': 'cgst_amt',
            'SGST AMT': 'sgst_amt',
            'IGST AMT': 'igst_amt',
            'UTGST AMT': 'utgst_amt',
            'Net Amount': 'net_amount',
            'GST Invoice No': 'gst_invoice_no',
            'Trans Type': 'trans_type',
            'Pay Type': 'pay_type',
            'CR / DR': 'cr_dr'
        }
        
        for old_name, new_name in column_mapping.items():
            if old_name in df.columns:
                df = df.withColumnRenamed(old_name, new_name)
        
        # Parse datetime columns
        df = df.withColumn("transaction_req_date", 
            to_timestamp(col("transaction_req_date"), "dd-MM-yyyy HH:mm:ss")
        ).withColumn("settlement_date",
            to_timestamp(col("settlement_date"), "dd-MM-yyyy HH:mm:ss")
        )
        
        # Cast numeric columns to double (handle both Long and String types)
        numeric_fields = ['transaction_amount', 'msf_amount', 'cgst_amt', 'sgst_amt', 
                         'igst_amt', 'utgst_amt', 'net_amount']
        
        for col_name in numeric_fields:
            if col_name in df.columns:
                df = df.withColumn(col_name,
                    when(col(col_name).isNotNull(), col(col_name).cast("double"))
                    .otherwise(lit(None).cast("double"))
                )
        
        # Generate UID from upi_trxn_id + transaction_req_date
        df = df.withColumn("uid",
            md5(concat_ws("_", 
                col("upi_trxn_id"), 
                col("transaction_req_date").cast("string")
            ))
        )
        
        # Select columns in target schema order
        df_transformed = df.select(
            col("uid"),
            col("external_mid"),
            col("external_tid"),
            col("upi_merchant_id"),
            col("merchant_name"),
            col("merchant_vpa"),
            col("payer_vpa"),
            col("upi_trxn_id"),
            col("order_id"),
            col("customer_ref_no__rrn").alias("customer_ref_no"),
            col("transaction_req_date"),
            col("settlement_date"),
            col("currency"),
            col("transaction_amount"),
            col("msf_amount"),
            col("cgst_amt"),
            col("sgst_amt"),
            col("igst_amt"),
            col("utgst_amt"),
            col("net_amount"),
            col("gst_invoice_no"),
            col("trans_type"),
            col("pay_type"),
            col("cr___dr").alias("cr_dr")
        )
        
        logger.info(f"✅ Transformed to mpr_hdfc_upi schema ({df_transformed.count()} rows)")
        return df_transformed
    
    def transform_mpr_card(self, df: SparkDataFrame) -> SparkDataFrame:
        """
        Transform MPR HDFC Card data to match mpr_hdfc_card table schema
        
        Source: HDFC Card transaction reports (54+ columns)
        Target: mpr_hdfc_card table (54 columns)
        """
        from pyspark.sql.functions import md5, concat_ws, col, when, lit, try_to_timestamp, regexp_extract
        
        logger.info("🔄 Transforming MPR Card data to mpr_hdfc_card schema")
        
        # Handle special column renames
        if '%' in df.columns:
            df = df.withColumnRenamed('%', 'pymt_percent')
        if 'NVL(BH_MEICAT,ME_TRCODE)' in df.columns:
            df = df.withColumnRenamed('NVL(BH_MEICAT,ME_TRCODE)', 'bh_meicat_me_trcode')
        
        # Clean column names (uppercase to lowercase with underscores)
        for c in df.columns:
            if c not in ['pymt_percent', 'bh_meicat_me_trcode']:
                new_name = c.lower()
                if new_name != c:
                    df = df.withColumnRenamed(c, new_name)
        
        # Fix bh_time: extract only HH:MM:SS (first 3 colon-separated parts)
        # Handles malformed values like '17:35:27:26' -> '17:35:27'
        if 'bh_time' in df.columns:
            df = df.withColumn("bh_time",
                regexp_extract(col("bh_time"), r"^(\d{1,2}:\d{2}:\d{2})", 1)
            )
        
        # Parse datetime columns with error handling (returns NULL for invalid timestamps)
        df = df.withColumn("chg_date",
            try_to_timestamp(col("chg_date"), lit("dd-MM-yyyy HH:mm:ss"))
        ).withColumn("process_date",
            try_to_timestamp(col("process_date"), lit("dd-MM-yyyy HH:mm:ss"))
        )
        
        # Cast all numeric columns to double explicitly (handle both Long and String types)
        numeric_cols = [
            'pymt_chgamnt', 'pymt_comm', 'pymt_percent', 'pymt_servtax', 'pymt_sbcess',
            'pymt_kkcess', 'pymt_cgst', 'pymt_sgst', 'pymt_igst', 'pymt_utgst', 'pymt_netamnt',
            'auth_amount', 'auth_comm', 'auth_servtax', 'auth_sbcess', 'auth_kkcess',
            'auth_cgst', 'auth_sgst', 'auth_igst', 'auth_utgst', 'auth_netamnt',
            'bh_cashamnt', 'inr_chgamnt', 'inr_comm', 'inr_servtax', 'inr_sbcess',
            'inr_kkcess', 'inr_cgst', 'inr_sgst', 'inr_igst', 'inr_utgst', 'inr_netamnt'
        ]
        
        from pyspark.sql.functions import isnan
        
        for col_name in numeric_cols:
            if col_name in df.columns:
                # Handle empty strings and malformed numbers (convert to NULL before casting)
                df = df.withColumn(col_name, 
                    when((col(col_name).isNull()) | (col(col_name) == ""), lit(None))
                    .otherwise(col(col_name))
                ).withColumn(col_name, col(col_name).cast("double"))
                # Convert NaN to NULL
                df = df.withColumn(col_name,
                    when(isnan(col(col_name)), lit(None)).otherwise(col(col_name))
                )
        
        # Generate UID with row_number to ensure absolute uniqueness
        # (same card at same terminal/time with same amount can occur multiple times)
        from pyspark.sql import Window
        from pyspark.sql.functions import row_number
        
        # Add row number partitioned by all fields to create unique IDs
        window_spec = Window.partitionBy(lit(1)).orderBy(lit(1))
        df = df.withColumn("row_num", row_number().over(window_spec))
        
        df = df.withColumn("uid",
            md5(concat_ws("_",
                col("cardnbr"),
                col("chg_date").cast("string"),
                col("terminal_no"),
                col("row_num").cast("string")
            ))
        ).drop("row_num")
        
        # Select columns (will adapt based on what exists in DataFrame)
        available_cols = ['uid', 'mecode', 'me_name', 'cardnbr', 'legal_name',
                         'chg_date', 'process_date', 'terminal_no', 'stall_no',
                         'grp_desc', 'app_code', 'pymt_cur_code']
        
        # Add all payment columns
        for c in numeric_cols:
            if c in df.columns:
                available_cols.append(c)
        
        # Add remaining text columns
        remaining_cols = ['txncode', 'envn', 'bh_posmode', 'me_currac', 
                         'bh_meicat_me_trcode', 'ubs_acc', 'bh_time']
        for c in remaining_cols:
            if c in df.columns:
                available_cols.append(c)
        
        # Select only columns that exist
        df_transformed = df.select([col(c) for c in available_cols if c in df.columns])
        
        logger.info(f"✅ Transformed to mpr_hdfc_card schema ({df_transformed.count()} rows)")
        return df_transformed
    
    def transform_zomato(self, df):
        """
        Transform Zomato data to match zomato table schema
        Source: Zomato transaction reports (14 columns)
        Target: zomato table (60 columns)
        """
        from pyspark.sql.functions import md5, concat_ws, col, lit, try_to_timestamp
        
        logger.info("🔄 Transforming Zomato data to zomato schema")
        
        # Clean column names (lowercase, replace spaces/special chars)
        for c in df.columns:
            new_name = c.lower().replace(' ', '_').replace('.', '_').replace('/', '_')
            if new_name != c:
                df = df.withColumnRenamed(c, new_name)
        
        # Parse date columns
        date_cols = ['bline_date', 'doc__date', 'pstng_date', 'entry_date']
        for dc in date_cols:
            if dc in df.columns:
                df = df.withColumn(dc, try_to_timestamp(col(dc), lit("dd.MM.yyyy")))
        
        # Cast numeric columns
        numeric_cols = ['debit', 'credit', 'local_crcy_amt']
        for nc in numeric_cols:
            if nc in df.columns:
                df = df.withColumn(nc,
                    when((col(nc).isNull()) | (col(nc) == ""), lit(None))
                    .otherwise(col(nc))
                ).withColumn(nc, col(nc).cast("double"))
                # Handle NaN
                from pyspark.sql.functions import isnan
                df = df.withColumn(nc,
                    when(isnan(col(nc)), lit(None)).otherwise(col(nc))
                )
        
        # Generate UID from documentno + bline_date + reference
        df = df.withColumn("uid",
            md5(concat_ws("_",
                col("documentno"),
                col("bline_date").cast("string"),
                col("reference")
            ))
        )
        
        # Map columns to zomato table schema
        # Basic columns from file
        df = df.withColumn("order_id", col("documentno")) \
               .withColumn("order_date", col("doc__date")) \
               .withColumn("amount", col("local_crcy_amt")) \
               .withColumn("status", lit("completed"))
        
        # Select available columns
        available_cols = ['uid', 'order_id', 'order_date', 'amount', 'status']
        
        # Add optional columns if they exist
        optional_cols = ['reference', 'assignment', 'text', 'debit', 'credit']
        for c in optional_cols:
            if c in df.columns:
                available_cols.append(c)
        
        df_transformed = df.select([col(c) for c in available_cols if c in df.columns])
        
        logger.info(f"✅ Transformed to zomato schema ({df_transformed.count()} rows)")
        return df_transformed
    
    def transform_pos_orders(self, df):
        """
        Transform POS data to match pizzahut_orders table schema
        Source: POS CSV files (143 columns)
        Target: pizzahut_orders table (144 columns)
        
        Note: Date parsing is already done by clean_dataframe() before this function
        """
        from pyspark.sql.functions import md5, concat_ws, col, lit, row_number, when, isnan
        from pyspark.sql import Window
        
        logger.info("🔄 Transforming POS data to pizzahut_orders schema")
        
        # Clean column names (lowercase, replace spaces/special chars)
        # This is needed because clean_dataframe uses clean_column_name which is different
        for c in df.columns:
            new_name = c.lower().replace(' ', '_').replace('.', '_').replace('/', '_').replace('-', '_')
            if new_name != c:
                df = df.withColumnRenamed(c, new_name)
        
        # Note: Dates are already parsed by clean_dataframe()
        # We skip date parsing here to avoid overwriting with less robust logic
        
        # Parse time columns (these are NOT handled by clean_dataframe)
        # Time columns contain pure time values like "12:34:56"
        from pyspark.sql.functions import try_to_timestamp
        time_cols = ['time', 'time_when_total_pressed', 'time_when_trans__closed',
                     'last_printing_time', 'online_void_time', 'order_received_time',
                     'food_ready', 'load_time']
        for tc in time_cols:
            if tc in df.columns:
                df = df.withColumn(tc,
                    try_to_timestamp(col(tc), lit("HH:mm:ss"))
                )
        
        # Cast numeric columns
        numeric_cols = ['net_amount', 'gross_amount', 'payment', 'discount_amount',
                       'customer_discount', 'total_discount', 'no__of_items',
                       'amount_to_account', 'rounded', 'income_exp__amount',
                       'starting_point_balance', 'online_payment_amount',
                       'service_tax_amount', 'service_tax_ecess_amount',
                       'service_tax_she_cess_amount', 'zomato_online_discount_amt']
        
        from pyspark.sql.functions import isnan
        for nc in numeric_cols:
            if nc in df.columns:
                # Handle empty strings
                df = df.withColumn(nc,
                    when((col(nc).isNull()) | (col(nc) == ""), lit(None))
                    .otherwise(col(nc))
                ).withColumn(nc, col(nc).cast("double"))
                # Handle NaN
                df = df.withColumn(nc,
                    when(isnan(col(nc)), lit(None)).otherwise(col(nc))
                )
        
        # Generate UID with row_number for uniqueness
        window_spec = Window.partitionBy(lit(1)).orderBy(lit(1))
        df = df.withColumn("row_num", row_number().over(window_spec))
        
        df = df.withColumn("uid",
            md5(concat_ws("_",
                col("store_no_"),
                col("transaction_no_"),
                col("date").cast("string"),
                col("row_num").cast("string")
            ))
        ).drop("row_num")
        
        # Rename columns to match MySQL schema (remove trailing underscores)
        rename_map = {
            'store_no_': 'store_no',
            'pos_terminal_no_': 'pos_terminal_no',
            'transaction_no_': 'transaction_no',
            'receipt_no_': 'receipt_no',
            'shift_no_': 'shift_no',
            'customer_no_': 'customer_no',
            'refund_receipt_no_': 'refund_receipt_no',
            'table_no_': 'table_no',
            'no__of_covers_': 'no_of_covers',
            'split_number': 'split_number',
            'member_card_no_': 'member_card_no',
            'receipt_no__1': 'receipt_no_1',
            'token_no_': 'token_no',
            'pos_order_no_': 'pos_order_no',
            'merge_table_no_': 'merge_table_no',
            'coupon_no_': 'coupon_no',
            'web_order_no_': 'web_order_no',
            'kot_no_': 'kot_no',
            'table_token_no_': 'table_token_no'
        }
        
        for old, new in rename_map.items():
            if old in df.columns:
                df = df.withColumnRenamed(old, new)
        
        logger.info(f"✅ Transformed to pizzahut_orders schema")
        return df
    
    def transform_pos_to_orders(self, df):
        """
        Transform POS data (from CSV) to match orders table schema
        Source: POS CSV files (143 columns from pizzahut_orders structure)
        Target: orders table (25 columns)
        
        Key transformations:
        - Use existing timestamp columns (already parsed by transform_pos_orders)
        - Map payment DECIMAL to NULL (type mismatch - source is amount, target is payment method)
        - Set missing GST/packaging columns to 0.0
        """
        from pyspark.sql.functions import md5, concat_ws, col, lit, when, concat, isnan
        
        logger.info("🔄 Transforming POS data to orders schema")
        
        # Clean column names
        for c in df.columns:
            new_name = c.lower().replace(' ', '_').replace('.', '_').replace('/', '_').replace('-', '_')
            if new_name != c:
                df = df.withColumnRenamed(c, new_name)
        
        # Note: Date columns are already TIMESTAMP type from transform_pos_orders
        # We'll use them directly instead of combining strings
        
        # Calculate subtotal = gross_amount - total_discount
        # Cast to DECIMAL before subtraction
        if 'gross_amount' in df.columns and 'total_discount' in df.columns:
            df = df.withColumn("subtotal_calc",
                when(col("gross_amount").isNotNull() & col("total_discount").isNotNull(),
                    col("gross_amount").cast("decimal(15,2)") - col("total_discount").cast("decimal(15,2)")
                ).otherwise(col("gross_amount").cast("decimal(15,2)"))
            )
        
        # Select and map columns to orders schema
        select_cols = []
        
        # Map columns from pizzahut_orders to orders
        # Target (orders table) : Source (pizzahut_orders DataFrame after transform_pos_orders)
        # Note: Source column names are normalized (lowercase, underscores, no trailing underscores)
        column_mapping = {
            # Use 'date' as primary, fallback to 'created_on_pos_terminal' if date is NULL
            # timestamp column has hex values (0x...), so we skip it
            'date': 'date',  # datetime - use Date column (column #5), NOT timestamp
            'original_date': 'original_date',  # datetime (already parsed)
            'business_date': 'shift_date',  # datetime (already parsed, shift_date is business_date)
            'store_name': 'store_no',  # varchar - normalized without trailing underscore
            'online_order_taker': 'online_order_taker',  # varchar (direct)
            'payment': None,  # varchar - TYPE MISMATCH (source is DECIMAL amount, target is VARCHAR payment method name)
            'bill_number': 'pos_receipt_no',  # varchar - normalized without trailing underscore  
            'bill_time': 'created_on_pos_terminal',  # datetime - use Created on POS Terminal as bill time
            'bill_user': 'staff_name',  # varchar
            'channel': 'source_type',  # varchar - use Source Type column (column #62)
            'settlement_mode': 'transaction_type',  # varchar
            'subtotal': 'subtotal_calc',  # decimal (calculated)
            'discount': 'total_discount',  # decimal
            'net_sale': 'net_amount',  # decimal
            'gst_at_5_percent': None,  # decimal - NOT IN SOURCE, set to 0.0
            'gst_ecom_at_5_percent': None,  # decimal - NOT IN SOURCE, set to 0.0
            'packaging_charge_cart_swiggy': None,  # decimal - NOT IN SOURCE, set to 0.0
            'packaging_charge': None,  # decimal - NOT IN SOURCE, set to 0.0
            'gross_amount': 'gross_amount',  # decimal (direct)
            'mode_name': 'transaction_type',  # varchar
            'transaction_number': 'transaction_no',  # varchar - normalized without trailing underscore
            'source': 'source',  # varchar - use Source column (column #97)
            'instance_id': 'instance_id',  # varchar (direct)
            'restaurant_packaging_charges': None  # decimal - NOT IN SOURCE, set to 0.0
        }
        
        for target_col, source_col in column_mapping.items():
            if source_col is None:
                # Set missing columns to NULL or 0.0 for decimals
                if target_col in ['gst_at_5_percent', 'gst_ecom_at_5_percent', 
                                  'packaging_charge_cart_swiggy', 'packaging_charge',
                                  'restaurant_packaging_charges']:
                    select_cols.append(lit(0.0).cast("double").alias(target_col))
                else:
                    select_cols.append(lit(None).cast("string").alias(target_col))
            elif source_col in df.columns:
                select_cols.append(col(source_col).alias(target_col))
            else:
                # Column not found in source, set to NULL with string type
                select_cols.append(lit(None).cast("string").alias(target_col))
        
        df_transformed = df.select(*select_cols)
        
        logger.info(f"✅ Transformed to orders schema ({df_transformed.count()} rows)")
        return df_transformed
    
    def ensure_table_exists(self, table_name: str, df: SparkDataFrame):
        """
        Create MySQL table if it doesn't exist based on DataFrame schema
        
        Args:
            table_name: Target table name
            df: Spark DataFrame with schema
        """
        try:
            from app.core.database import db_manager
            with db_manager.get_mysql_connector() as conn:
                cursor = conn.cursor()
                
                # Check if table exists
                cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
                exists = cursor.fetchone()
                
                if not exists:
                    logger.info(f"🆕 Creating MySQL table `{table_name}`")
                    
                    # Create column definitions
                    cols = ", ".join([f"`{c}` TEXT" for c in df.columns])
                    create_sql = f"CREATE TABLE `{table_name}` ({cols}, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);"
                    
                    cursor.execute(create_sql)
                    conn.commit()
                    logger.info(f"✅ Table `{table_name}` created successfully")
                else:
                    logger.info(f"✅ Table `{table_name}` already exists")
            
        except Exception as e:
            logger.error(f"❌ Table creation failed: {e}")
            raise
    
    def align_schema_with_table(self, df: SparkDataFrame, table_name: str) -> SparkDataFrame:
        """
        Align Spark DataFrame schema with existing MySQL table
        
        Args:
            df: Spark DataFrame
            table_name: Target MySQL table
        
        Returns:
            Schema-aligned DataFrame
        """
        try:
            from app.core.database import db_manager
            with db_manager.get_mysql_connector() as conn:
                cursor = conn.cursor()
                
                # Get MySQL table columns
                cursor.execute(f"SHOW COLUMNS FROM {table_name}")
                mysql_cols = [r[0].lower() for r in cursor.fetchall()]
            
                # Add missing columns as NULL
                for c in mysql_cols:
                    if c not in df.columns and c != 'created_at':
                        df = df.withColumn(c, lit(None))
                
                # Select only columns that exist in MySQL table (excluding created_at)
                valid_cols = [c for c in mysql_cols if c in df.columns and c != 'created_at']
                df = df.select([col(c).cast(StringType()) for c in valid_cols])
                
                logger.info(f"✅ Schema aligned with MySQL ({len(df.columns)} columns)")
            return df
            
        except Exception as e:
            logger.error(f"❌ Schema alignment failed: {e}")
            raise
    
    def upload_to_mysql(self, 
                       df: SparkDataFrame, 
                       table_name: str,
                       mode: str = "append") -> int:
        """
        Upload Spark DataFrame to MySQL
        
        Args:
            df: Spark DataFrame
            table_name: Target table name
            mode: Write mode (append/overwrite)
        
        Returns:
            Number of rows uploaded
        """
        try:
            row_count = df.count()
            # Extract database name from JDBC URL
            db_name = self.mysql_url.split('/')[-1].split('?')[0]
            logger.info(f"🗄️ Uploading {row_count} rows to MySQL `{db_name}.{table_name}`")
            
            df.write \
                .format("jdbc") \
                .option("url", self.mysql_url) \
                .option("dbtable", table_name) \
                .option("user", MYSQL_USER) \
                .option("password", MYSQL_PASSWORD) \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .option("batchsize", 1000) \
                .mode(mode) \
                .save()
            
            logger.info(f"✅ Successfully uploaded {row_count} rows")
            return row_count
            
        except AnalysisException as ae:
            logger.error(f"❌ Spark SQL error: {ae}")
            raise
        except Exception as e:
            logger.error(f"❌ Upload failed: {e}")
            logger.error(traceback.format_exc())
            raise
    
    def process_file(self,
                    file_path: Path,
                    table_name: str,
                    source: FileSource,
                    vendor_folder: str,
                    mode: str = "append") -> Dict[str, Any]:
        """
        Complete ETL workflow for a single file
        
        Args:
            file_path: Path to source file
            table_name: Target MySQL table
            source: FileSource (SFTP or EMAIL)
            vendor_folder: Vendor folder name
            mode: Write mode (append/overwrite)
        
        Returns:
            Processing results
        """
        temp_path = None
        
        try:
            # Copy to temp for safe processing
            temp_path = self.file_manager.copy_to_temp(file_path)
            
            # Load file
            df = self.load_file_to_spark(temp_path)
            
            # Clean and normalize
            df = self.clean_dataframe(df)
            
            # Apply vendor-specific transformations
            if vendor_folder == "Bank_Receipt" and table_name == "fin_bank_statements":
                logger.info("📋 Applying Bank Receipt transformation mapping")
                df = self.transform_bank_receipt(df)
                # For Bank Receipt, we've already created the target schema
                # No need to ensure table exists or align schema - use existing table
            elif vendor_folder == "TRM_Data" and table_name == "trm":
                logger.info("📋 Applying TRM transformation mapping")
                df = self.transform_trm(df)
                # TRM table already exists, transformation handles schema
            elif vendor_folder == "MPR_Data" and table_name == "mpr_hdfc_upi":
                logger.info("📋 Applying MPR UPI transformation mapping")
                df = self.transform_mpr_upi(df)
                # MPR UPI table already exists
            elif vendor_folder == "MPR_Data" and table_name == "mpr_hdfc_card":
                logger.info("📋 Applying MPR Card transformation mapping")
                df = self.transform_mpr_card(df)
                # MPR Card table already exists
            elif vendor_folder == "Zomato_data" and table_name == "zomato":
                logger.info("📋 Applying Zomato transformation mapping")
                df = self.transform_zomato(df)
                # Zomato table already exists
            elif vendor_folder == "POS_Data" and table_name == "pizzahut_orders":
                logger.info("📋 Applying POS transformation mapping (pizzahut_orders)")
                df = self.transform_pos_orders(df)
                # POS table already exists
            elif vendor_folder == "POS_Data" and table_name == "orders":
                logger.info("📋 Applying POS transformation mapping (CSV → pizzahut_orders → orders)")
                # Two-stage transformation: CSV → pizzahut_orders (parse dates) → orders (schema mapping)
                df = self.transform_pos_orders(df)  # Stage 1: Parse datetimes
                df = self.transform_pos_to_orders(df)  # Stage 2: Map to orders schema
                # Orders table already exists
            else:
                # Standard flow for other vendors
                # Ensure table exists
                self.ensure_table_exists(table_name, df)
                
                # Align schema
                df = self.align_schema_with_table(df, table_name)
            
            # Upload to MySQL
            rows_uploaded = self.upload_to_mysql(df, table_name, mode)
            
            # Move to processed
            processed_path = self.file_manager.move_to_processed(
                file_path,
                source=source,
                vendor_folder=vendor_folder
            )
            
            logger.info(f"✅ File processed and archived: {processed_path}")
            
            return {
                "success": True,
                "rows_uploaded": rows_uploaded,
                "processed_path": str(processed_path),
                "table_name": table_name
            }
            
        except Exception as e:
            logger.error(f"❌ Processing failed for {file_path.name}: {e}")
            
            # Move to failed
            error_msg = f"{type(e).__name__}: {str(e)}"
            failed_path = self.file_manager.move_to_failed(
                file_path,
                failure_type="upload",
                error_message=error_msg
            )
            
            return {
                "success": False,
                "error": error_msg,
                "failed_path": str(failed_path)
            }
            
        finally:
            # Cleanup temp file
            if temp_path and temp_path.exists():
                temp_path.unlink()
    
    def process_vendor_folder(self,
                             vendor_folder: str,
                             source: FileSource = FileSource.SFTP,
                             table_name: Optional[str] = None,
                             file_pattern: str = "*.*") -> Dict[str, Any]:
        """
        Process all files from a vendor folder using PySpark
        
        Args:
            vendor_folder: Vendor folder name
            source: FileSource (SFTP or EMAIL)
            table_name: Target table (auto-derived if None)
            file_pattern: File pattern to match
        
        Returns:
            Processing summary
        """
        logger.info(f"🔄 PySpark batch processing: {vendor_folder} from {source.value}")
        
        # Scan for files
        files = self.file_manager.scan_incoming_files(
            source=source,
            vendor_folder=vendor_folder,
            file_pattern=file_pattern
        )
        
        if not files:
            logger.info(f"No files found in {vendor_folder}")
            return {
                "vendor": vendor_folder,
                "source": source.value,
                "files_found": 0,
                "files_processed": 0,
                "success": [],
                "failed": []
            }
        
        # Auto-derive table name if not provided
        if table_name is None:
            table_name = self._derive_table_name(vendor_folder)
        
        # Filter files based on table name for MPR data (UPI vs Card)
        if vendor_folder == "MPR_Data":
            if "upi" in table_name.lower():
                files = [f for f in files if "UPI" in f.name.upper()]
                logger.info(f"Filtered to {len(files)} UPI files")
            elif "card" in table_name.lower():
                files = [f for f in files if "S3" in f.name.upper() or "CARD" in f.name.upper()]
                logger.info(f"Filtered to {len(files)} Card files")
        
        results = {
            "vendor": vendor_folder,
            "source": source.value,
            "table_name": table_name,
            "files_found": len(files),
            "files_processed": 0,
            "total_rows": 0,
            "success": [],
            "failed": []
        }
        
        # Process each file
        for file_path in files:
            logger.info(f"📄 Processing: {file_path.name}")
            
            process_result = self.process_file(
                file_path=file_path,
                table_name=table_name,
                source=source,
                vendor_folder=vendor_folder
            )
            
            if process_result["success"]:
                results["success"].append({
                    "file": file_path.name,
                    "rows": process_result.get("rows_uploaded", 0)
                })
                results["total_rows"] += process_result.get("rows_uploaded", 0)
            else:
                results["failed"].append({
                    "file": file_path.name,
                    "error": process_result.get("error", "Unknown error")
                })
            
            results["files_processed"] += 1
        
        # Summary
        logger.info(f"""
        📊 {vendor_folder} Processing Summary:
        - Files found: {results['files_found']}
        - Successfully processed: {len(results['success'])}
        - Total rows uploaded: {results['total_rows']}
        - Failed: {len(results['failed'])}
        """)
        
        return results
    
    def collect_summary_statistics(self, table_name: str) -> Dict[str, Any]:
        """
        Collect comprehensive statistics from the database table for email summary.
        
        Args:
            table_name: Name of the table to analyze
        
        Returns:
            Dictionary with statistics (date_range, amounts, row_stats, etc.)
        """
        try:
            from app.core.database import db_manager
            from app.core.config import config
            with db_manager.get_mysql_connector() as conn:
                cursor = conn.cursor(dictionary=True)
                
                stats = {}
                
                # Get basic row count
                cursor.execute(f"SELECT COUNT(*) as total_rows FROM {table_name}")
                result = cursor.fetchone()
                stats['total_rows'] = result['total_rows'] if result else 0
            
            # Try to get date range (look for common date columns)
            date_columns = ['date', 'transaction_date', 'business_date', 'created_at', 'value_date']
            for date_col in date_columns:
                try:
                    cursor.execute(f"""
                        SELECT 
                            MIN({date_col}) as min_date,
                            MAX({date_col}) as max_date,
                            DATEDIFF(MAX({date_col}), MIN({date_col})) + 1 as days
                        FROM {table_name}
                        WHERE {date_col} IS NOT NULL
                    """)
                    result = cursor.fetchone()
                    if result and result['min_date']:
                        stats['date_range'] = {
                            'min_date': str(result['min_date']),
                            'max_date': str(result['max_date']),
                            'days': result['days']
                        }
                        break
                except:
                    continue
            
            # Try to get amount statistics (look for common amount columns)
            amount_columns = {
                'gross_amount': 'Gross Amount',
                'net_sale': 'Net Sale',
                'amount': 'Amount',
                'credit_amount': 'Credit Amount',
                'debit_amount': 'Debit Amount',
                'discount': 'Discount',
                'transaction_amount': 'Transaction Amount'
            }
            
            amount_stats = {}
            for col, label in amount_columns.items():
                try:
                    cursor.execute(f"SELECT SUM({col}) as total FROM {table_name} WHERE {col} IS NOT NULL")
                    result = cursor.fetchone()
                    if result and result['total'] is not None:
                        amount_stats[label] = float(result['total'])
                except:
                    continue
            
                if amount_stats:
                    stats['amount_stats'] = amount_stats
                
                return stats
            
        except Exception as e:
            logger.error(f"❌ Error collecting summary statistics: {e}")
            return {}
    
    def send_email_notification(
        self,
        vendor_folder: str,
        table_name: str,
        processing_results: Dict[str, Any],
        processing_time: float
    ) -> bool:
        """
        Send email notification with processing summary.
        
        Args:
            vendor_folder: Vendor folder name
            table_name: Target table name
            processing_results: Results from process_vendor_folder
            processing_time: Total processing time in seconds
        
        Returns:
            True if email sent successfully, False otherwise
        """
        try:
            from app.services.email_service import EmailService
            
            email_service = EmailService()
            
            if not email_service.enabled:
                logger.info("📧 Email notifications disabled (EMAIL_ENABLED=false)")
                return False
            
            # Collect detailed statistics from database
            db_stats = self.collect_summary_statistics(table_name)
            
            # Combine processing results with database statistics
            summary_data = {
                'files_processed': processing_results.get('files_processed', 0),
                'total_rows': processing_results.get('total_rows', 0),
                'success_count': len(processing_results.get('success', [])),
                'failed_count': len(processing_results.get('failed', [])),
                'date_range': db_stats.get('date_range', {}),
                'amount_stats': db_stats.get('amount_stats', {}),
                'row_stats': db_stats.get('row_stats', {})
            }
            
            # Send email
            success = email_service.send_processing_summary(
                vendor_name=vendor_folder,
                table_name=table_name,
                summary_data=summary_data,
                processing_time=processing_time
            )
            
            if success:
                logger.info(f"✅ Email notification sent successfully for {vendor_folder}")
            else:
                logger.warning(f"⚠️ Failed to send email notification for {vendor_folder}")
            
            return success
            
        except ImportError:
            logger.warning("⚠️ EmailService not available - skipping email notification")
            return False
        except Exception as e:
            logger.error(f"❌ Error sending email notification: {e}")
            return False
    
    def _derive_table_name(self, vendor_folder: str) -> str:
        """Derive table name from vendor folder"""
        folder_lower = vendor_folder.lower()
        
        table_mapping = {
            "bank_receipt": "fin_bank_statements",
            "mpr_data": "mpr_hdfc_upi",  # Default for MPR
            "pos_data": "orders",  # Changed from pizzahut_orders to orders
            "trm_data": "trm",
            "zomato_data": "zomato",
            "zomato": "zomato",
            "swiggy_data": "swiggy",
            "swiggy": "swiggy",
        }
        
        for key, table in table_mapping.items():
            if key in folder_lower:
                return table
        
        return "fin_transactions"
    
    def stop(self):
        """Stop Spark session"""
        logger.info("🧹 Stopping Spark session")
        self.spark.stop()


# CLI Interface
if __name__ == "__main__":
    import argparse
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    
    parser = argparse.ArgumentParser(description="PySpark-powered batch file processor")
    parser.add_argument("--vendor", type=str, help="Vendor folder to process")
    parser.add_argument("--source", choices=["sftp", "email"], default="sftp")
    parser.add_argument("--table", type=str, help="Target table name")
    parser.add_argument("--pattern", type=str, default="*.*", help="File pattern")
    
    args = parser.parse_args()
    
    loader = PySparkDataLoader()
    
    try:
        source = FileSource.SFTP if args.source == "sftp" else FileSource.EMAIL
        
        if args.vendor:
            # Track processing time
            start_time = time.time()
            
            result = loader.process_vendor_folder(
                vendor_folder=args.vendor,
                source=source,
                table_name=args.table,
                file_pattern=args.pattern
            )
            
            processing_time = time.time() - start_time
            
            print(f"\n✅ Processing complete!")
            print(f"Files processed: {result['files_processed']}")
            print(f"Total rows: {result['total_rows']}")
            print(f"Success: {len(result['success'])}")
            print(f"Failed: {len(result['failed'])}")
            print(f"Processing time: {processing_time:.2f}s")
            
            # Send email notification if enabled
            if args.table and len(result['success']) > 0:
                logger.info("📧 Preparing email notification...")
                loader.send_email_notification(
                    vendor_folder=args.vendor,
                    table_name=args.table,
                    processing_results=result,
                    processing_time=processing_time
                )
        else:
            print("Please specify --vendor")
    
    finally:
        loader.stop()
