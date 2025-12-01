#!/usr/bin/env python3
"""
File Management Service for Financial Reconciliation Platform
Handles file organization with SFTP/Email sources and month/year archiving
"""

import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List, Tuple
import logging
from enum import Enum

logger = logging.getLogger(__name__)

class FileSource(Enum):
    """File source types"""
    SFTP = "sftp"
    EMAIL = "email"
    UPLOAD = "uploads"
    TEMP = "temp"

class ProcessStatus(Enum):
    """File processing status"""
    SUCCESS = "success"
    PARTIAL = "partial"
    FAILED = "failed"

class FileManager:
    """
    Manages file organization with the following structure:
    
    data/
    ├── sftp/                    # SFTP downloads (primary source)
    │   ├── Bank_Receipt/
    │   ├── MPR_Data/
    │   ├── POS_Data/
    │   ├── TRM_Data/
    │   └── Zomato_data/
    ├── email/                   # Email attachments (secondary source)
    │   └── [vendor_name]/
    ├── processed/               # Archived by month/year after processing
    │   ├── 2025-11/            # Format: YYYY-MM
    │   │   ├── sftp/
    │   │   │   └── [vendor_folders]/
    │   │   └── email/
    │   │       └── [vendor_folders]/
    │   └── 2025-10/
    ├── failed/                  # Failed processing
    │   ├── validation/
    │   └── upload/
    ├── temp/                    # Temporary processing
    └── logs/                    # Processing logs
        ├── automation/
        └── jobs/
    """
    
    def __init__(self, base_path: Optional[str] = None):
        if base_path is None:
            # Use relative path from project root
            project_root = Path(__file__).parent.parent.parent
            base_path = str(project_root / "data")
        self.base_path = Path(base_path)
        self._ensure_directory_structure()
    
    def _ensure_directory_structure(self):
        """Ensure all required directories exist"""
        required_dirs = [
            self.base_path / "sftp",
            self.base_path / "email",
            self.base_path / "processed",
            self.base_path / "failed" / "validation",
            self.base_path / "failed" / "upload",
            self.base_path / "temp",
            self.base_path / "logs" / "automation",
            self.base_path / "logs" / "jobs",
        ]
        
        for dir_path in required_dirs:
            dir_path.mkdir(parents=True, exist_ok=True)
            logger.debug(f"Ensured directory exists: {dir_path}")
    
    def get_sftp_path(self, vendor_folder: Optional[str] = None) -> Path:
        """Get SFTP source path, optionally for specific vendor"""
        sftp_path = self.base_path / "sftp"
        if vendor_folder:
            sftp_path = sftp_path / vendor_folder
            sftp_path.mkdir(parents=True, exist_ok=True)
        return sftp_path
    
    def get_email_path(self, vendor_folder: Optional[str] = None) -> Path:
        """Get email source path, optionally for specific vendor"""
        email_path = self.base_path / "email"
        if vendor_folder:
            email_path = email_path / vendor_folder
            email_path.mkdir(parents=True, exist_ok=True)
        return email_path
    
    def get_processed_path(self, year_month: Optional[str] = None, 
                          source: Optional[FileSource] = None,
                          vendor_folder: Optional[str] = None) -> Path:
        """
        Get processed archive path with year-month structure
        
        Args:
            year_month: Format YYYY-MM (default: current month)
            source: FileSource enum (sftp or email)
            vendor_folder: Vendor subfolder name
        """
        if year_month is None:
            year_month = datetime.now().strftime("%Y-%m")
        
        processed_path = self.base_path / "processed" / year_month
        
        if source:
            processed_path = processed_path / source.value
        
        if vendor_folder:
            processed_path = processed_path / vendor_folder
        
        processed_path.mkdir(parents=True, exist_ok=True)
        return processed_path
    
    def get_failed_path(self, failure_type: str = "validation") -> Path:
        """Get failed processing path"""
        failed_path = self.base_path / "failed" / failure_type
        failed_path.mkdir(parents=True, exist_ok=True)
        return failed_path
    
    def get_temp_path(self) -> Path:
        """Get temporary processing path"""
        return self.base_path / "temp"
    
    def scan_incoming_files(self, source: FileSource = FileSource.SFTP,
                           vendor_folder: Optional[str] = None,
                           file_pattern: str = "*") -> List[Path]:
        """
        Scan for new incoming files from SFTP or email
        
        Args:
            source: FileSource.SFTP or FileSource.EMAIL
            vendor_folder: Specific vendor folder to scan
            file_pattern: File pattern to match (e.g., "*.csv", "*.xlsx")
        
        Returns:
            List of file paths
        """
        if source == FileSource.SFTP:
            base_path = self.get_sftp_path(vendor_folder)
        elif source == FileSource.EMAIL:
            base_path = self.get_email_path(vendor_folder)
        else:
            raise ValueError(f"Invalid source: {source}")
        
        files = []
        
        if vendor_folder:
            # Scan specific vendor folder
            files.extend(list(base_path.glob(file_pattern)))
        else:
            # Scan all vendor subfolders
            for vendor_path in base_path.iterdir():
                if vendor_path.is_dir() and not vendor_path.name.startswith('.'):
                    files.extend(list(vendor_path.glob(file_pattern)))
        
        # Filter out directories and hidden files
        files = [f for f in files if f.is_file() and not f.name.startswith('.')]
        
        logger.info(f"Found {len(files)} files in {source.value}/{vendor_folder or 'all vendors'}")
        return files
    
    def move_to_processed(self, file_path: Path, 
                         source: FileSource,
                         vendor_folder: str,
                         year_month: Optional[str] = None,
                         preserve_structure: bool = True) -> Path:
        """
        Move file to processed archive organized by month/year
        
        Args:
            file_path: Source file path
            source: FileSource enum (SFTP or EMAIL)
            vendor_folder: Vendor subfolder name
            year_month: Archive month (default: current month)
            preserve_structure: Keep vendor folder structure
        
        Returns:
            New file path in processed folder
        """
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        # Get processed path with year-month structure
        if preserve_structure:
            dest_dir = self.get_processed_path(year_month, source, vendor_folder)
        else:
            dest_dir = self.get_processed_path(year_month, source)
        
        # Generate unique filename if exists
        dest_path = dest_dir / file_path.name
        if dest_path.exists():
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            stem = file_path.stem
            suffix = file_path.suffix
            dest_path = dest_dir / f"{stem}_{timestamp}{suffix}"
        
        # Move file
        shutil.move(str(file_path), str(dest_path))
        logger.info(f"Moved to processed: {file_path.name} -> {dest_path}")
        
        return dest_path
    
    def move_to_failed(self, file_path: Path, 
                      failure_type: str = "validation",
                      error_message: Optional[str] = None) -> Path:
        """
        Move file to failed folder with error log
        
        Args:
            file_path: Source file path
            failure_type: Type of failure (validation, upload)
            error_message: Error description
        
        Returns:
            New file path in failed folder
        """
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        dest_dir = self.get_failed_path(failure_type)
        
        # Generate unique filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        stem = file_path.stem
        suffix = file_path.suffix
        dest_path = dest_dir / f"{stem}_{timestamp}{suffix}"
        
        # Move file
        shutil.move(str(file_path), str(dest_path))
        
        # Write error log
        if error_message:
            error_log_path = dest_path.with_suffix('.error.log')
            with open(error_log_path, 'w') as f:
                f.write(f"File: {file_path.name}\n")
                f.write(f"Failed at: {datetime.now().isoformat()}\n")
                f.write(f"Failure type: {failure_type}\n")
                f.write(f"Error: {error_message}\n")
        
        logger.warning(f"Moved to failed/{failure_type}: {file_path.name}")
        return dest_path
    
    def copy_to_temp(self, file_path: Path) -> Path:
        """
        Copy file to temp folder for processing
        
        Args:
            file_path: Source file path
        
        Returns:
            Temp file path
        """
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        temp_dir = self.get_temp_path()
        
        # Generate unique temp filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        stem = file_path.stem
        suffix = file_path.suffix
        temp_path = temp_dir / f"{stem}_{timestamp}{suffix}"
        
        # Copy file
        shutil.copy2(str(file_path), str(temp_path))
        logger.debug(f"Copied to temp: {file_path.name}")
        
        return temp_path
    
    def cleanup_temp(self, older_than_hours: int = 24):
        """
        Clean up old temp files
        
        Args:
            older_than_hours: Delete files older than this many hours
        """
        temp_dir = self.get_temp_path()
        current_time = datetime.now().timestamp()
        cutoff_time = current_time - (older_than_hours * 3600)
        
        deleted_count = 0
        for file_path in temp_dir.glob("*"):
            if file_path.is_file():
                file_mtime = file_path.stat().st_mtime
                if file_mtime < cutoff_time:
                    file_path.unlink()
                    deleted_count += 1
                    logger.debug(f"Deleted old temp file: {file_path.name}")
        
        logger.info(f"Cleaned up {deleted_count} temp files older than {older_than_hours}h")
        return deleted_count
    
    def get_vendor_folders(self, source: FileSource = FileSource.SFTP) -> List[str]:
        """
        Get list of vendor folders in SFTP or email
        
        Args:
            source: FileSource.SFTP or FileSource.EMAIL
        
        Returns:
            List of vendor folder names
        """
        if source == FileSource.SFTP:
            base_path = self.get_sftp_path()
        elif source == FileSource.EMAIL:
            base_path = self.get_email_path()
        else:
            raise ValueError(f"Invalid source: {source}")
        
        vendors = [
            d.name for d in base_path.iterdir() 
            if d.is_dir() and not d.name.startswith('.')
        ]
        
        return sorted(vendors)
    
    def get_processing_summary(self) -> Dict[str, any]:
        """Get summary of files across all folders"""
        summary = {
            "sftp": {
                "vendors": {},
                "total_files": 0
            },
            "email": {
                "vendors": {},
                "total_files": 0
            },
            "processed": {
                "months": {},
                "total_files": 0
            },
            "failed": {
                "validation": 0,
                "upload": 0
            },
            "temp": 0
        }
        
        # SFTP files
        sftp_vendors = self.get_vendor_folders(FileSource.SFTP)
        for vendor in sftp_vendors:
            files = self.scan_incoming_files(FileSource.SFTP, vendor)
            summary["sftp"]["vendors"][vendor] = len(files)
            summary["sftp"]["total_files"] += len(files)
        
        # Email files
        email_vendors = self.get_vendor_folders(FileSource.EMAIL)
        for vendor in email_vendors:
            files = self.scan_incoming_files(FileSource.EMAIL, vendor)
            summary["email"]["vendors"][vendor] = len(files)
            summary["email"]["total_files"] += len(files)
        
        # Processed files (by month)
        processed_path = self.base_path / "processed"
        if processed_path.exists():
            for month_dir in processed_path.iterdir():
                if month_dir.is_dir() and not month_dir.name.startswith('.'):
                    file_count = sum(1 for _ in month_dir.rglob("*") if _.is_file())
                    summary["processed"]["months"][month_dir.name] = file_count
                    summary["processed"]["total_files"] += file_count
        
        # Failed files
        failed_validation = self.get_failed_path("validation")
        summary["failed"]["validation"] = sum(1 for _ in failed_validation.glob("*") if _.is_file())
        
        failed_upload = self.get_failed_path("upload")
        summary["failed"]["upload"] = sum(1 for _ in failed_upload.glob("*") if _.is_file())
        
        # Temp files
        temp_path = self.get_temp_path()
        summary["temp"] = sum(1 for _ in temp_path.glob("*") if _.is_file())
        
        return summary


# Example usage and utility functions
def create_vendor_folder(vendor_name: str, source: FileSource = FileSource.SFTP):
    """Create a new vendor folder in SFTP or email"""
    fm = FileManager()
    if source == FileSource.SFTP:
        path = fm.get_sftp_path(vendor_name)
    else:
        path = fm.get_email_path(vendor_name)
    logger.info(f"Created vendor folder: {path}")
    return path


def process_file_workflow(file_path: Path, source: FileSource, vendor_folder: str,
                          success: bool = True, error_message: Optional[str] = None):
    """
    Complete workflow for processing a file
    
    Args:
        file_path: File to process
        source: FileSource (SFTP or EMAIL)
        vendor_folder: Vendor name
        success: Whether processing succeeded
        error_message: Error message if failed
    """
    fm = FileManager()
    
    try:
        # Copy to temp for processing
        temp_path = fm.copy_to_temp(file_path)
        
        # Process the file (your processing logic here)
        # ...
        
        # Move based on result
        if success:
            final_path = fm.move_to_processed(file_path, source, vendor_folder)
            logger.info(f"Successfully processed: {file_path.name} -> {final_path}")
        else:
            final_path = fm.move_to_failed(file_path, "upload", error_message)
            logger.error(f"Processing failed: {file_path.name} -> {final_path}")
        
        # Cleanup temp
        if temp_path.exists():
            temp_path.unlink()
        
        return final_path
        
    except Exception as e:
        logger.error(f"Error in file workflow: {e}")
        raise


if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    
    # Initialize file manager
    fm = FileManager()
    
    # Get summary
    summary = fm.get_processing_summary()
    print("\n📊 File Management Summary:")
    print(f"SFTP Files: {summary['sftp']['total_files']}")
    for vendor, count in summary['sftp']['vendors'].items():
        print(f"  - {vendor}: {count} files")
    
    print(f"\nEmail Files: {summary['email']['total_files']}")
    for vendor, count in summary['email']['vendors'].items():
        print(f"  - {vendor}: {count} files")
    
    print(f"\nProcessed (by month): {summary['processed']['total_files']} total")
    for month, count in summary['processed']['months'].items():
        print(f"  - {month}: {count} files")
    
    print(f"\nFailed:")
    print(f"  - Validation: {summary['failed']['validation']} files")
    print(f"  - Upload: {summary['failed']['upload']} files")
    
    print(f"\nTemp: {summary['temp']} files")
