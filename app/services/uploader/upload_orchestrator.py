"""
Automated Upload Orchestrator
Created: November 12, 2025
Purpose: Discover and upload all data files (POS, TRM, MPR, Bank) automatically
"""

import os
import sys
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple
import json

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from app.logging_config import setup_logging
from app.services.uploader.data_uploader import DataUploader

# Setup logging
setup_logging('INFO')
logger = logging.getLogger('upload_orchestrator')


class UploadOrchestrator:
    """Orchestrates automated upload of all data types"""
    
    def __init__(self, base_path: str = None):
        """
        Initialize the upload orchestrator
        
        Args:
            base_path: Base directory path for data files (default: current directory)
        """
        self.base_path = Path(base_path) if base_path else Path.cwd()
        self.uploader = DataUploader()
        self.upload_results = []
        self.summary = {
            'start_time': None,
            'end_time': None,
            'total_files': 0,
            'successful_uploads': 0,
            'failed_uploads': 0,
            'skipped_uploads': 0,
            'total_records_uploaded': 0,
            'uploads': []
        }
        
        # Data source configurations
        self.data_sources = [
            {
                'name': 'TRM Terminal Data',
                'directory': 'data/sftp/TRM_Data',
                'pattern': '*.xlsx',
                'table': 'trm',
                'prefix': 'TRM',
                'priority': 1
            },
            {
                'name': 'MPR UPI Data',
                'directory': 'data/sftp/MPR_Data',
                'pattern': '*UPI*.xlsx',
                'table': 'mpr_hdfc_upi',
                'prefix': 'MPR_UPI',
                'priority': 2
            },
            {
                'name': 'MPR Card Data',
                'directory': 'data/sftp/MPR_Data',
                'pattern': '*Card*.xlsx',
                'table': 'mpr_hdfc_card',
                'prefix': 'MPR_CARD',
                'priority': 3
            },
            {
                'name': 'Bank Statements',
                'directory': 'data/sftp/Bank_Data',
                'pattern': '*.xlsx',
                'table': 'fin_bank_statements',
                'prefix': 'BANK',
                'priority': 4
            },
            {
                'name': 'POS Orders',
                'directory': 'data/sftp/POS_Data',
                'pattern': '*.csv',
                'table': 'orders',
                'prefix': 'ORD',
                'priority': 5
            },
            {
                'name': 'Zomato Data',
                'directory': 'data/sftp/3PO_Data',
                'pattern': '*Zomato*.xlsx',
                'table': 'zomato',
                'prefix': 'ZOMATO',
                'priority': 6
            }
        ]
    
    def discover_files(self) -> List[Dict]:
        """
        Discover all data files to upload
        
        Returns:
            List of file metadata dictionaries
        """
        discovered_files = []
        
        for source in self.data_sources:
            source_dir = self.base_path / source['directory']
            
            if not source_dir.exists():
                logger.warning(f"⚠️  Directory not found: {source_dir}")
                continue
            
            # Find matching files
            pattern = source['pattern']
            # For MPR Card Data, check both S3 and Card patterns
            if source['table'] == 'mpr_hdfc_card':
                files_s3 = list(source_dir.glob('*S3*.xlsx'))
                files_card = list(source_dir.glob('*Card*.xlsx'))
                files = list(set(files_s3 + files_card))  # Remove duplicates
            else:
                files = list(source_dir.glob(pattern))
            
            for file_path in files:
                # Skip temporary/hidden files
                if file_path.name.startswith('~') or file_path.name.startswith('.'):
                    continue
                
                file_info = {
                    'source_name': source['name'],
                    'file_path': str(file_path),
                    'file_name': file_path.name,
                    'file_size': file_path.stat().st_size,
                    'table': source['table'],
                    'prefix': source['prefix'],
                    'priority': source['priority']
                }
                discovered_files.append(file_info)
                logger.info(f"📁 Discovered: {file_info['file_name']} ({file_info['file_size']:,} bytes)")
        
        # Sort by priority
        discovered_files.sort(key=lambda x: x['priority'])
        
        logger.info(f"📊 Total files discovered: {len(discovered_files)}")
        return discovered_files
    
    def upload_file(self, file_info: Dict, force: bool = False) -> Tuple[bool, str, int]:
        """
        Upload a single file
        
        Args:
            file_info: File metadata dictionary
            force: Force upload even if already uploaded
        
        Returns:
            Tuple of (success, message, records_uploaded)
        """
        logger.info(f"\n{'='*80}")
        logger.info(f"📤 Uploading: {file_info['source_name']}")
        logger.info(f"   File: {file_info['file_name']}")
        logger.info(f"   Table: {file_info['table']}")
        logger.info(f"{'='*80}")
        
        try:
            success, message = self.uploader.upload_to_table(
                file_path=file_info['file_path'],
                table_name=file_info['table'],
                uid_prefix=file_info['prefix'],
                skip_duplicates=not force
            )
            
            # Extract records uploaded from message
            records_uploaded = 0
            if success and "Successfully uploaded" in message:
                # Parse "Successfully uploaded 140,334 records"
                parts = message.split()
                for i, part in enumerate(parts):
                    if part == "uploaded" and i+1 < len(parts):
                        records_uploaded = int(parts[i+1].replace(',', ''))
                        break
            
            return success, message, records_uploaded
            
        except Exception as e:
            error_msg = f"❌ Upload failed: {str(e)}"
            logger.error(error_msg)
            return False, error_msg, 0
    
    def upload_all(self, force: bool = False) -> Dict:
        """
        Upload all discovered files
        
        Args:
            force: Force upload even if files already uploaded
        
        Returns:
            Summary dictionary with upload results
        """
        self.summary['start_time'] = datetime.now()
        logger.info(f"\n🚀 Starting automated upload process at {self.summary['start_time']}")
        
        # Discover files
        files = self.discover_files()
        self.summary['total_files'] = len(files)
        
        if len(files) == 0:
            logger.warning("⚠️  No files found to upload!")
            self.summary['end_time'] = datetime.now()
            return self.summary
        
        # Upload each file
        for idx, file_info in enumerate(files, 1):
            logger.info(f"\n📋 Processing file {idx}/{len(files)}")
            
            upload_start = datetime.now()
            success, message, records_uploaded = self.upload_file(file_info, force)
            upload_duration = (datetime.now() - upload_start).total_seconds()
            
            # Build upload result
            result = {
                'source_name': file_info['source_name'],
                'file_name': file_info['file_name'],
                'file_size': file_info['file_size'],
                'table': file_info['table'],
                'success': success,
                'message': message,
                'records_uploaded': records_uploaded,
                'duration_seconds': round(upload_duration, 2),
                'timestamp': datetime.now().isoformat()
            }
            
            self.summary['uploads'].append(result)
            
            # Update counters
            if success:
                if "already uploaded" in message.lower() or "skipping" in message.lower():
                    self.summary['skipped_uploads'] += 1
                    logger.info(f"⏭️  Skipped (already uploaded)")
                else:
                    self.summary['successful_uploads'] += 1
                    self.summary['total_records_uploaded'] += records_uploaded
                    logger.info(f"✅ Success: {records_uploaded:,} records uploaded in {upload_duration:.1f}s")
            else:
                self.summary['failed_uploads'] += 1
                logger.error(f"❌ Failed: {message}")
        
        self.summary['end_time'] = datetime.now()
        total_duration = (self.summary['end_time'] - self.summary['start_time']).total_seconds()
        
        # Print final summary
        logger.info(f"\n{'='*80}")
        logger.info(f"📊 UPLOAD SUMMARY")
        logger.info(f"{'='*80}")
        logger.info(f"Total Files: {self.summary['total_files']}")
        logger.info(f"✅ Successful: {self.summary['successful_uploads']}")
        logger.info(f"❌ Failed: {self.summary['failed_uploads']}")
        logger.info(f"⏭️  Skipped: {self.summary['skipped_uploads']}")
        logger.info(f"📈 Total Records: {self.summary['total_records_uploaded']:,}")
        logger.info(f"⏱️  Duration: {total_duration:.1f}s")
        logger.info(f"{'='*80}")
        
        return self.summary
    
    def get_summary_json(self) -> str:
        """
        Get upload summary as JSON string
        
        Returns:
            JSON formatted summary
        """
        return json.dumps(self.summary, indent=2, default=str)
    
    def save_summary(self, output_path: str = None) -> str:
        """
        Save upload summary to file
        
        Args:
            output_path: Path to save summary (default: reports/upload_summary_YYYYMMDD_HHMMSS.json)
        
        Returns:
            Path to saved summary file
        """
        if output_path is None:
            # Create reports directory if not exists
            reports_dir = self.base_path / 'reports'
            reports_dir.mkdir(exist_ok=True)
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_path = reports_dir / f'upload_summary_{timestamp}.json'
        
        with open(output_path, 'w') as f:
            f.write(self.get_summary_json())
        
        logger.info(f"💾 Summary saved to: {output_path}")
        return str(output_path)


def main():
    """Main entry point for CLI usage"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Automated data upload orchestrator')
    parser.add_argument('--base-path', type=str, help='Base directory for data files')
    parser.add_argument('--force', action='store_true', help='Force upload even if already uploaded')
    parser.add_argument('--save-summary', action='store_true', help='Save summary to JSON file')
    
    args = parser.parse_args()
    
    # Create orchestrator
    orchestrator = UploadOrchestrator(base_path=args.base_path)
    
    # Run uploads
    summary = orchestrator.upload_all(force=args.force)
    
    # Save summary if requested
    if args.save_summary:
        orchestrator.save_summary()
    
    # Exit with appropriate code
    if summary['failed_uploads'] > 0:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == '__main__':
    main()
