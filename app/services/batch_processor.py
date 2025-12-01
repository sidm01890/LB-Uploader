#!/usr/bin/env python3
"""
Batch File Processor for SFTP and Email Sources
Processes files from organized vendor folders and archives them by month/year
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime
import asyncio
import pandas as pd

from app.services.file_manager import FileManager, FileSource
from app.services.file_utils import extract_headers, get_file_info
from app.services.mapping_service import suggest_mapping
from app.services.upload_service import uploader
from app.db import get_mysql_connection

logger = logging.getLogger(__name__)

class BatchFileProcessor:
    """
    Batch processor for files from SFTP and email sources
    Automatically processes files and archives them by month/year
    """
    
    def __init__(self):
        self.file_manager = FileManager()
        self.engine = get_mysql_connection()
    
    async def process_vendor_folder(self, 
                                    vendor_folder: str,
                                    source: FileSource = FileSource.SFTP,
                                    table_name: Optional[str] = None,
                                    file_pattern: str = "*.csv",
                                    auto_map: bool = True) -> Dict[str, Any]:
        """
        Process all files from a specific vendor folder
        
        Args:
            vendor_folder: Vendor folder name (e.g., "Bank_Receipt", "Zomato_data")
            source: FileSource.SFTP or FileSource.EMAIL
            table_name: Target database table (if None, derive from vendor name)
            file_pattern: File pattern to match
            auto_map: Use AI for automatic column mapping
        
        Returns:
            Processing results summary
        """
        logger.info(f"🔄 Processing {vendor_folder} from {source.value}")
        
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
        
        # Determine target table
        if table_name is None:
            table_name = self._derive_table_name(vendor_folder)
        
        results = {
            "vendor": vendor_folder,
            "source": source.value,
            "table_name": table_name,
            "files_found": len(files),
            "files_processed": 0,
            "success": [],
            "failed": []
        }
        
        # Process each file
        for file_path in files:
            try:
                logger.info(f"📄 Processing: {file_path.name}")
                
                # Copy to temp for processing
                temp_path = self.file_manager.copy_to_temp(file_path)
                
                try:
                    # Process the file
                    process_result = await self._process_single_file(
                        file_path=temp_path,
                        original_path=file_path,
                        table_name=table_name,
                        vendor_folder=vendor_folder,
                        source=source,
                        auto_map=auto_map
                    )
                    
                    if process_result["success"]:
                        results["success"].append({
                            "file": file_path.name,
                            "rows": process_result.get("rows_uploaded", 0)
                        })
                        
                        # Move to processed
                        processed_path = self.file_manager.move_to_processed(
                            file_path,
                            source=source,
                            vendor_folder=vendor_folder
                        )
                        logger.info(f"✅ Successfully processed and archived: {file_path.name}")
                    else:
                        results["failed"].append({
                            "file": file_path.name,
                            "error": process_result.get("error", "Unknown error")
                        })
                        
                        # Move to failed
                        failed_path = self.file_manager.move_to_failed(
                            file_path,
                            failure_type="upload",
                            error_message=process_result.get("error", "Unknown error")
                        )
                        logger.error(f"❌ Processing failed: {file_path.name}")
                    
                    results["files_processed"] += 1
                    
                finally:
                    # Cleanup temp file
                    if temp_path.exists():
                        temp_path.unlink()
                
            except Exception as e:
                logger.error(f"Error processing {file_path.name}: {e}")
                results["failed"].append({
                    "file": file_path.name,
                    "error": str(e)
                })
        
        # Log summary
        logger.info(f"""
        📊 {vendor_folder} Processing Summary:
        - Files found: {results['files_found']}
        - Successfully processed: {len(results['success'])}
        - Failed: {len(results['failed'])}
        """)
        
        return results
    
    async def _process_single_file(self,
                                   file_path: Path,
                                   original_path: Path,
                                   table_name: str,
                                   vendor_folder: str,
                                   source: FileSource,
                                   auto_map: bool = True) -> Dict[str, Any]:
        """Process a single file with AI mapping and upload"""
        try:
            # Get file info
            file_info = get_file_info(str(file_path))
            
            # Extract headers
            file_headers = extract_headers(str(file_path))
            if not file_headers:
                return {
                    "success": False,
                    "error": "Could not extract file headers"
                }
            
            # Get AI mapping if enabled
            if auto_map:
                mapping_result = await suggest_mapping(
                    file_headers=file_headers,
                    table_name=table_name
                )
                
                if not mapping_result.get("success"):
                    return {
                        "success": False,
                        "error": "Failed to generate column mapping"
                    }
                
                column_mapping = mapping_result["suggested_mapping"]
            else:
                # Use direct mapping (assumes headers match table columns)
                column_mapping = {h: h for h in file_headers}
            
            # Upload data
            # Note: This is a simplified version. You may need to adapt based on your upload service
            import pandas as pd
            
            if file_path.suffix.lower() in ['.xlsx', '.xls']:
                df = pd.read_excel(file_path)
            else:
                df = pd.read_csv(file_path)
            
            # Rename columns according to mapping
            df = df.rename(columns=column_mapping)
            
            # Upload to database (simplified - adapt to your needs)
            rows_uploaded = await self._upload_dataframe(df, table_name)
            
            return {
                "success": True,
                "rows_uploaded": rows_uploaded,
                "mapping": column_mapping
            }
            
        except Exception as e:
            logger.error(f"Error in _process_single_file: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    async def _upload_dataframe(self, df: pd.DataFrame, table_name: str) -> int:
        """Upload dataframe to database"""
        try:
            # Use pandas to_sql for simple upload
            df.to_sql(
                table_name,
                self.engine,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=1000
            )
            return len(df)
        except Exception as e:
            logger.error(f"Database upload error: {e}")
            raise
    
    def _derive_table_name(self, vendor_folder: str) -> str:
        """
        Derive table name from vendor folder name
        
        Examples:
            Bank_Receipt -> fin_bank_statements
            Zomato_data -> fin_payments
            POS_Data -> fin_transactions
        """
        folder_lower = vendor_folder.lower()
        
        # Mapping of vendor folders to table names
        table_mapping = {
            "bank_receipt": "fin_bank_statements",
            "mpr_data": "fin_transactions",
            "pos_data": "fin_transactions",
            "trm_data": "fin_transactions",
            "zomato_data": "fin_payments",
            "swiggy_data": "fin_payments",
            "zomato": "fin_payments",
            "swiggy": "fin_payments",
        }
        
        for key, table in table_mapping.items():
            if key in folder_lower:
                return table
        
        # Default to fin_transactions
        return "fin_transactions"
    
    async def process_all_sftp_vendors(self, 
                                      file_pattern: str = "*.csv",
                                      auto_map: bool = True) -> Dict[str, Any]:
        """Process files from all SFTP vendor folders"""
        logger.info("🚀 Starting SFTP batch processing")
        
        vendors = self.file_manager.get_vendor_folders(FileSource.SFTP)
        all_results = {
            "total_vendors": len(vendors),
            "vendors": {}
        }
        
        for vendor in vendors:
            result = await self.process_vendor_folder(
                vendor_folder=vendor,
                source=FileSource.SFTP,
                file_pattern=file_pattern,
                auto_map=auto_map
            )
            all_results["vendors"][vendor] = result
        
        return all_results
    
    async def process_all_email_vendors(self,
                                       file_pattern: str = "*.*",
                                       auto_map: bool = True) -> Dict[str, Any]:
        """Process files from all email vendor folders"""
        logger.info("📧 Starting email batch processing")
        
        vendors = self.file_manager.get_vendor_folders(FileSource.EMAIL)
        all_results = {
            "total_vendors": len(vendors),
            "vendors": {}
        }
        
        for vendor in vendors:
            result = await self.process_vendor_folder(
                vendor_folder=vendor,
                source=FileSource.EMAIL,
                file_pattern=file_pattern,
                auto_map=auto_map
            )
            all_results["vendors"][vendor] = result
        
        return all_results
    
    def cleanup_old_files(self, days: int = 30):
        """Cleanup old temp and failed files"""
        logger.info(f"🧹 Cleaning up files older than {days} days")
        
        # Cleanup temp files (older than 1 day)
        self.file_manager.cleanup_temp(older_than_hours=24)
        
        # Note: You might want to add cleanup for old failed files
        # based on your retention policy


# CLI Interface
async def main():
    """Command-line interface for batch processing"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Batch process files from SFTP/Email folders")
    parser.add_argument("--source", choices=["sftp", "email", "all"], default="sftp",
                       help="Source to process")
    parser.add_argument("--vendor", type=str, help="Specific vendor folder to process")
    parser.add_argument("--pattern", type=str, default="*.csv", help="File pattern to match")
    parser.add_argument("--no-auto-map", action="store_true", 
                       help="Disable automatic AI column mapping")
    parser.add_argument("--cleanup", action="store_true", help="Run cleanup of old files")
    
    args = parser.parse_args()
    
    processor = BatchFileProcessor()
    
    if args.cleanup:
        processor.cleanup_old_files()
        return
    
    if args.vendor:
        # Process specific vendor
        source = FileSource.SFTP if args.source == "sftp" else FileSource.EMAIL
        result = await processor.process_vendor_folder(
            vendor_folder=args.vendor,
            source=source,
            file_pattern=args.pattern,
            auto_map=not args.no_auto_map
        )
        print(f"\n✅ Processed {result['files_processed']} files from {args.vendor}")
    else:
        # Process all vendors
        if args.source in ["sftp", "all"]:
            sftp_results = await processor.process_all_sftp_vendors(
                file_pattern=args.pattern,
                auto_map=not args.no_auto_map
            )
            print(f"\n📊 SFTP Processing Complete:")
            print(f"Total vendors: {sftp_results['total_vendors']}")
            for vendor, result in sftp_results['vendors'].items():
                print(f"  {vendor}: {len(result['success'])} success, {len(result['failed'])} failed")
        
        if args.source in ["email", "all"]:
            email_results = await processor.process_all_email_vendors(
                file_pattern=args.pattern,
                auto_map=not args.no_auto_map
            )
            print(f"\n📧 Email Processing Complete:")
            print(f"Total vendors: {email_results['total_vendors']}")
            for vendor, result in email_results['vendors'].items():
                print(f"  {vendor}: {len(result['success'])} success, {len(result['failed'])} failed")


if __name__ == "__main__":
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    asyncio.run(main())
