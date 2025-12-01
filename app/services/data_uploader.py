"""
Simplified Data Uploader with Business Rules
- Uses existing pyspark_loader for file processing
- Adds validation and duplicate prevention
- Process-driven with clear rules
"""

import mysql.connector
import logging
from datetime import datetime
from typing import Dict, List, Tuple
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from app import config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class DataUploader:
    """Simple, rule-based data uploader using existing infrastructure"""
    
    def __init__(self):
        from app.core.database import db_manager
        from app.core.config import config
        self.db_manager = db_manager
        self.db_config = db_manager.get_connection_dict()
        logger.info("Initialized DataUploader for database: devyani")
    
    def get_record_count(self, table_name: str) -> int:
        """Get current record count from table"""
        try:
            with self.db_manager.get_mysql_connector() as conn:
                cursor = conn.cursor()
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = cursor.fetchone()[0]
                return count
        except Exception as e:
            logger.error(f"Error getting count from {table_name}: {e}")
            return 0
    
    def check_duplicates(self, table_name: str, file_path: str) -> Dict:
        """
        Check if data from this file was already uploaded
        Returns: dict with duplicate info
        """
        try:
            # Get file metadata
            file_name = os.path.basename(file_path)
            file_size = os.path.getsize(file_path)
            
            with self.db_manager.get_mysql_connector() as conn:
                cursor = conn.cursor(dictionary=True)
                
                # Check upload history (we'll create this table)
                cursor.execute("""
                    SELECT * FROM upload_history 
                    WHERE table_name = %s 
                    AND file_name = %s 
                    AND file_size = %s
                    ORDER BY upload_date DESC
                    LIMIT 1
                """, (table_name, file_name, file_size))
                
                result = cursor.fetchone()
                
                if result:
                return {
                    'is_duplicate': True,
                    'last_upload': result['upload_date'],
                    'records_uploaded': result['records_uploaded']
                }
            else:
                return {'is_duplicate': False}
                
        except mysql.connector.Error as e:
            if e.errno == 1146:  # Table doesn't exist
                # Create upload_history table
                self.create_upload_history_table()
                return {'is_duplicate': False}
            else:
                logger.error(f"Error checking duplicates: {e}")
                return {'is_duplicate': False}
    
    def create_upload_history_table(self):
        """Create table to track upload history"""
        try:
            with self.db_manager.get_mysql_connector() as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS upload_history (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    table_name VARCHAR(100) NOT NULL,
                    file_name VARCHAR(255) NOT NULL,
                    file_path VARCHAR(500),
                    file_size BIGINT,
                    records_before INT,
                    records_uploaded INT,
                    records_after INT,
                    upload_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                    upload_status VARCHAR(50),
                    error_message TEXT,
                    INDEX idx_table_file (table_name, file_name),
                    INDEX idx_upload_date (upload_date)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            
            conn.commit()
            cursor.close()
            conn.close()
            logger.info("✅ Created upload_history table")
            
        except Exception as e:
            logger.error(f"Error creating upload_history table: {e}")
    
    def log_upload(self, table_name: str, file_path: str, records_before: int, 
                   records_after: int, status: str, error_msg: str = None):
        """Log upload to history table"""
        try:
            file_name = os.path.basename(file_path)
            file_size = os.path.getsize(file_path)
            records_uploaded = records_after - records_before
            
            with self.db_manager.get_mysql_connector() as conn:
                cursor = conn.cursor()
                
                cursor.execute("""
                    INSERT INTO upload_history 
                    (table_name, file_name, file_path, file_size, records_before, 
                     records_uploaded, records_after, upload_status, error_message)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (table_name, file_name, file_path, file_size, records_before,
                      records_uploaded, records_after, status, error_msg))
                
                conn.commit()
            
        except Exception as e:
            logger.error(f"Error logging upload: {e}")
    
    def upload_file(self, file_path: str, table_name: str, vendor: str, 
                    force: bool = False) -> Tuple[bool, str]:
        """
        Upload file using existing pyspark_loader infrastructure
        
        Args:
            file_path: Path to file
            table_name: Target table
            vendor: Vendor identifier for pyspark_loader
            force: Skip duplicate check if True
        
        Returns:
            (success, message)
        """
        try:
            logger.info(f"\n{'='*60}")
            logger.info(f"📦 Uploading: {os.path.basename(file_path)}")
            logger.info(f"📊 Target Table: {table_name}")
            logger.info(f"{'='*60}")
            
            # Check if file exists
            if not os.path.exists(file_path):
                return False, f"File not found: {file_path}"
            
            # Get record count before
            records_before = self.get_record_count(table_name)
            logger.info(f"📈 Records before upload: {records_before:,}")
            
            # Check for duplicates unless forced
            if not force:
                dup_check = self.check_duplicates(table_name, file_path)
                if dup_check['is_duplicate']:
                    logger.warning(f"⚠️  File already uploaded on {dup_check['last_upload']}")
                    logger.warning(f"⚠️  {dup_check['records_uploaded']:,} records were uploaded")
                    logger.warning(f"⚠️  Use --force to re-upload")
                    return False, "Duplicate file (use --force to override)"
            
            # Use existing pyspark_loader
            logger.info(f"🚀 Starting upload via pyspark_loader...")
            
            import subprocess
            cmd = [
                'python3', '-m', 'app.services.pyspark_loader',
                '--vendor', vendor,
                '--source', 'sftp',
                '--table', table_name
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
            
            if result.returncode == 0:
                # Get record count after
                records_after = self.get_record_count(table_name)
                records_uploaded = records_after - records_before
                
                logger.info(f"✅ Upload successful!")
                logger.info(f"📈 Records after upload: {records_after:,}")
                logger.info(f"📊 New records added: {records_uploaded:,}")
                
                # Log to history
                self.log_upload(table_name, file_path, records_before, 
                               records_after, 'SUCCESS')
                
                return True, f"Uploaded {records_uploaded:,} records"
            else:
                error_msg = result.stderr or result.stdout
                logger.error(f"❌ Upload failed: {error_msg}")
                
                # Log failure
                records_after = self.get_record_count(table_name)
                self.log_upload(table_name, file_path, records_before, 
                               records_after, 'FAILED', error_msg[:500])
                
                return False, error_msg
                
        except subprocess.TimeoutExpired:
            error_msg = "Upload timeout (>10 minutes)"
            logger.error(f"❌ {error_msg}")
            return False, error_msg
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"❌ Upload error: {error_msg}")
            return False, error_msg
    
    def get_upload_history(self, table_name: str = None, limit: int = 10) -> List[Dict]:
        """Get upload history"""
        try:
            with self.db_manager.get_mysql_connector() as conn:
                cursor = conn.cursor(dictionary=True)
                
                if table_name:
                    cursor.execute("""
                        SELECT * FROM upload_history 
                        WHERE table_name = %s 
                        ORDER BY upload_date DESC 
                        LIMIT %s
                    """, (table_name, limit))
                else:
                    cursor.execute("""
                        SELECT * FROM upload_history 
                        ORDER BY upload_date DESC 
                        LIMIT %s
                    """, (limit,))
                
                results = cursor.fetchall()
                
                return results
            
        except Exception as e:
            logger.error(f"Error getting history: {e}")
            return []
    
    def show_status(self):
        """Show current data status"""
        try:
            with self.db_manager.get_mysql_connector() as conn:
                cursor = conn.cursor()
                
                print("\n" + "="*70)
                print("📊 CURRENT DATA STATUS")
                print("="*70)
                
                tables = [
                    ('orders', 'POS Orders'),
                    ('trm', 'TRM Transactions'),
                    ('mpr_hdfc_upi', 'MPR UPI'),
                    ('mpr_hdfc_card', 'MPR Card'),
                    ('fin_bank_statements', 'Bank Statements')
                ]
                
                for table, label in tables:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cursor.fetchone()[0]
                    print(f"  {label:.<40} {count:>15,} records")
                
                print("="*70)
                
                # Show recent uploads
                print("\n📜 RECENT UPLOADS (Last 5):")
                print("="*70)
                
                history = self.get_upload_history(limit=5)
                if history:
                    for h in history:
                        status_icon = "✅" if h['upload_status'] == 'SUCCESS' else "❌"
                        print(f"{status_icon} {h['upload_date']} | {h['table_name']:20} | "
                              f"{h['file_name']:30} | +{h['records_uploaded']:,} records")
                else:
                    print("  No upload history found")
                
                print("="*70 + "\n")
            
        except Exception as e:
            logger.error(f"Error showing status: {e}")


def main():
    """CLI interface"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Upload data files with rules and duplicate prevention')
    parser.add_argument('--action', choices=['upload', 'status', 'history'], 
                       default='status', help='Action to perform')
    parser.add_argument('--file', help='File path to upload')
    parser.add_argument('--table', help='Target table name')
    parser.add_argument('--vendor', help='Vendor identifier')
    parser.add_argument('--force', action='store_true', 
                       help='Force upload even if duplicate')
    
    args = parser.parse_args()
    
    uploader = DataUploader()
    
    if args.action == 'status':
        uploader.show_status()
    
    elif args.action == 'history':
        history = uploader.get_upload_history(args.table, limit=20)
        print("\n📜 Upload History:")
        print("="*100)
        for h in history:
            status_icon = "✅" if h['upload_status'] == 'SUCCESS' else "❌"
            print(f"{status_icon} {h['upload_date']} | {h['table_name']:20} | "
                  f"{h['file_name']:40} | +{h['records_uploaded']:>8,} records")
        print("="*100 + "\n")
    
    elif args.action == 'upload':
        if not all([args.file, args.table, args.vendor]):
            print("❌ Error: --file, --table, and --vendor are required for upload")
            sys.exit(1)
        
        success, message = uploader.upload_file(
            args.file, args.table, args.vendor, args.force
        )
        
        if success:
            print(f"\n✅ {message}\n")
            uploader.show_status()
            sys.exit(0)
        else:
            print(f"\n❌ {message}\n")
            sys.exit(1)


if __name__ == '__main__':
    main()
