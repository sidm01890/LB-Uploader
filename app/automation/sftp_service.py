"""
SFTP Automation Service for Smart Uploader Platform
Handles automated file downloads, processing, and notifications
"""

import asyncio
import paramiko
import ftplib
import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
from pathlib import Path
import schedule
import time
from dataclasses import dataclass
from enum import Enum
import json
import os
import shutil
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)

class ConnectionType(Enum):
    SFTP = "sftp"
    FTP = "ftp"
    FTPS = "ftps"

@dataclass
class SFTPConfig:
    host: str
    port: int
    username: str
    password: Optional[str] = None
    private_key_path: Optional[str] = None
    connection_type: ConnectionType = ConnectionType.SFTP
    remote_path: str = "/"
    file_patterns: List[str] = None
    archive_processed: bool = True
    max_retries: int = 3
    timeout: int = 30

@dataclass
class DownloadResult:
    success: bool
    file_path: str
    file_size: int
    download_time: float
    error_message: Optional[str] = None

class SFTPAutomationService:
    """
    Enterprise SFTP automation service with scheduling, monitoring, and error handling
    """
    
    def __init__(self, config: SFTPConfig, local_download_path: str):
        self.config = config
        self.local_download_path = Path(local_download_path)
        self.local_download_path.mkdir(parents=True, exist_ok=True)
        
        # Create subdirectories
        self.processed_path = self.local_download_path / "processed"
        self.failed_path = self.local_download_path / "failed"
        self.archive_path = self.local_download_path / "archive"
        
        for path in [self.processed_path, self.failed_path, self.archive_path]:
            path.mkdir(parents=True, exist_ok=True)
        
        self.connection_pool = {}
        self.download_history = []
        self.is_running = False
        
        logger.info(f"SFTP Automation Service initialized for {config.host}")
    
    async def start_scheduled_downloads(self, schedule_time: str = "02:00"):
        """
        Start scheduled daily downloads at specified time
        
        Args:
            schedule_time: Time in HH:MM format (24-hour)
        """
        
        logger.info(f"Starting scheduled downloads at {schedule_time} daily")
        
        # Schedule daily download
        schedule.every().day.at(schedule_time).do(self._execute_scheduled_download)
        
        # Also schedule hourly health checks
        schedule.every().hour.do(self._health_check)
        
        self.is_running = True
        
        # Run scheduler in background
        while self.is_running:
            schedule.run_pending()
            await asyncio.sleep(60)  # Check every minute
    
    def _execute_scheduled_download(self):
        """Execute scheduled download (called by scheduler)"""
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(self.download_all_files())
            loop.close()
            
            logger.info(f"Scheduled download completed: {result['summary']}")
            return result
            
        except Exception as e:
            logger.error(f"Scheduled download failed: {str(e)}", exc_info=True)
            return {"success": False, "error": str(e)}
    
    async def download_all_files(self) -> Dict[str, Any]:
        """
        Download all matching files from SFTP server
        """
        
        start_time = datetime.now()
        download_results = []
        
        try:
            logger.info("Starting automated file download process")
            
            # Establish connection
            connection = await self._create_connection()
            
            # Get list of files to download
            remote_files = await self._list_remote_files(connection)
            
            logger.info(f"Found {len(remote_files)} files to download")
            
            # Download files concurrently
            download_tasks = [
                self._download_single_file(connection, remote_file)
                for remote_file in remote_files
            ]
            
            download_results = await asyncio.gather(*download_tasks, return_exceptions=True)
            
            # Close connection
            await self._close_connection(connection)
            
            # Process results
            successful_downloads = [r for r in download_results if isinstance(r, DownloadResult) and r.success]
            failed_downloads = [r for r in download_results if isinstance(r, DownloadResult) and not r.success]
            exceptions = [r for r in download_results if isinstance(r, Exception)]
            
            # Archive processed files on remote server
            if self.config.archive_processed and successful_downloads:
                await self._archive_remote_files(successful_downloads)
            
            # Update download history
            self.download_history.append({
                "timestamp": start_time.isoformat(),
                "total_files": len(remote_files),
                "successful": len(successful_downloads),
                "failed": len(failed_downloads) + len(exceptions),
                "processing_time": (datetime.now() - start_time).total_seconds(),
                "downloaded_files": [r.file_path for r in successful_downloads]
            })
            
            return {
                "success": True,
                "summary": {
                    "total_files_found": len(remote_files),
                    "successful_downloads": len(successful_downloads),
                    "failed_downloads": len(failed_downloads) + len(exceptions),
                    "processing_time_seconds": (datetime.now() - start_time).total_seconds()
                },
                "downloaded_files": [r.file_path for r in successful_downloads],
                "errors": [r.error_message for r in failed_downloads if r.error_message] + [str(e) for e in exceptions]
            }
            
        except Exception as e:
            logger.error(f"Download process failed: {str(e)}", exc_info=True)
            return {
                "success": False,
                "error": str(e),
                "summary": {
                    "total_files_found": 0,
                    "successful_downloads": 0,
                    "failed_downloads": 1,
                    "processing_time_seconds": (datetime.now() - start_time).total_seconds()
                }
            }
    
    async def _create_connection(self):
        """Create SFTP/FTP connection based on configuration"""
        
        try:
            if self.config.connection_type == ConnectionType.SFTP:
                return await self._create_sftp_connection()
            elif self.config.connection_type == ConnectionType.FTP:
                return await self._create_ftp_connection()
            elif self.config.connection_type == ConnectionType.FTPS:
                return await self._create_ftps_connection()
            else:
                raise ValueError(f"Unsupported connection type: {self.config.connection_type}")
                
        except Exception as e:
            logger.error(f"Failed to create connection: {str(e)}")
            raise
    
    async def _create_sftp_connection(self):
        """Create SFTP connection using paramiko"""
        
        try:
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            # Connect with password or private key
            if self.config.private_key_path:
                private_key = paramiko.RSAKey.from_private_key_file(self.config.private_key_path)
                ssh_client.connect(
                    hostname=self.config.host,
                    port=self.config.port,
                    username=self.config.username,
                    pkey=private_key,
                    timeout=self.config.timeout
                )
            else:
                ssh_client.connect(
                    hostname=self.config.host,
                    port=self.config.port,
                    username=self.config.username,
                    password=self.config.password,
                    timeout=self.config.timeout
                )
            
            sftp_client = ssh_client.open_sftp()
            
            logger.info(f"SFTP connection established to {self.config.host}")
            return {"type": "sftp", "ssh": ssh_client, "sftp": sftp_client}
            
        except Exception as e:
            logger.error(f"SFTP connection failed: {str(e)}")
            raise
    
    async def _create_ftp_connection(self):
        """Create FTP connection"""
        
        try:
            ftp_client = ftplib.FTP()
            ftp_client.connect(self.config.host, self.config.port, timeout=self.config.timeout)
            ftp_client.login(self.config.username, self.config.password)
            
            logger.info(f"FTP connection established to {self.config.host}")
            return {"type": "ftp", "client": ftp_client}
            
        except Exception as e:
            logger.error(f"FTP connection failed: {str(e)}")
            raise
    
    async def _create_ftps_connection(self):
        """Create FTPS (FTP over SSL) connection"""
        
        try:
            ftps_client = ftplib.FTP_TLS()
            ftps_client.connect(self.config.host, self.config.port, timeout=self.config.timeout)
            ftps_client.login(self.config.username, self.config.password)
            ftps_client.prot_p()  # Enable encryption
            
            logger.info(f"FTPS connection established to {self.config.host}")
            return {"type": "ftps", "client": ftps_client}
            
        except Exception as e:
            logger.error(f"FTPS connection failed: {str(e)}")
            raise
    
    async def _list_remote_files(self, connection: Dict) -> List[Dict[str, Any]]:
        """List files on remote server matching patterns"""
        
        try:
            if connection["type"] == "sftp":
                return await self._list_sftp_files(connection["sftp"])
            else:  # FTP/FTPS
                return await self._list_ftp_files(connection["client"])
                
        except Exception as e:
            logger.error(f"Failed to list remote files: {str(e)}")
            raise
    
    async def _list_sftp_files(self, sftp_client) -> List[Dict[str, Any]]:
        """List files using SFTP"""
        
        try:
            files = []
            file_list = sftp_client.listdir_attr(self.config.remote_path)
            
            for file_attr in file_list:
                if self._matches_pattern(file_attr.filename):
                    files.append({
                        "name": file_attr.filename,
                        "size": file_attr.st_size,
                        "modified": datetime.fromtimestamp(file_attr.st_mtime),
                        "remote_path": f"{self.config.remote_path}/{file_attr.filename}".replace("//", "/")
                    })
            
            # Sort by modification time (newest first)
            files.sort(key=lambda x: x["modified"], reverse=True)
            
            logger.info(f"Found {len(files)} matching files via SFTP")
            return files
            
        except Exception as e:
            logger.error(f"SFTP file listing failed: {str(e)}")
            raise
    
    async def _list_ftp_files(self, ftp_client) -> List[Dict[str, Any]]:
        """List files using FTP/FTPS"""
        
        try:
            files = []
            ftp_client.cwd(self.config.remote_path)
            
            file_list = []
            ftp_client.retrlines('LIST', file_list.append)
            
            for line in file_list:
                # Parse FTP LIST response
                parts = line.split()
                if len(parts) >= 9 and not parts[0].startswith('d'):  # Not a directory
                    filename = ' '.join(parts[8:])
                    if self._matches_pattern(filename):
                        try:
                            size = ftp_client.size(filename)
                            modified_time = ftp_client.voidcmd(f'MDTM {filename}')
                            modified = datetime.strptime(modified_time[4:], '%Y%m%d%H%M%S')
                        except:
                            size = 0
                            modified = datetime.now()
                        
                        files.append({
                            "name": filename,
                            "size": size,
                            "modified": modified,
                            "remote_path": f"{self.config.remote_path}/{filename}".replace("//", "/")
                        })
            
            # Sort by modification time (newest first)
            files.sort(key=lambda x: x["modified"], reverse=True)
            
            logger.info(f"Found {len(files)} matching files via FTP")
            return files
            
        except Exception as e:
            logger.error(f"FTP file listing failed: {str(e)}")
            raise
    
    def _matches_pattern(self, filename: str) -> bool:
        """Check if filename matches configured patterns"""
        
        if not self.config.file_patterns:
            return True  # No patterns means match all
        
        import fnmatch
        return any(fnmatch.fnmatch(filename, pattern) for pattern in self.config.file_patterns)
    
    async def _download_single_file(self, connection: Dict, file_info: Dict[str, Any]) -> DownloadResult:
        """Download a single file"""
        
        start_time = time.time()
        local_file_path = self.local_download_path / file_info["name"]
        
        try:
            if connection["type"] == "sftp":
                connection["sftp"].get(file_info["remote_path"], str(local_file_path))
            else:  # FTP/FTPS
                with open(local_file_path, 'wb') as local_file:
                    connection["client"].retrbinary(f'RETR {file_info["name"]}', local_file.write)
            
            download_time = time.time() - start_time
            
            logger.info(f"Downloaded {file_info['name']} ({file_info['size']} bytes) in {download_time:.2f}s")
            
            return DownloadResult(
                success=True,
                file_path=str(local_file_path),
                file_size=file_info["size"],
                download_time=download_time
            )
            
        except Exception as e:
            error_msg = f"Failed to download {file_info['name']}: {str(e)}"
            logger.error(error_msg)
            
            # Move failed file to failed directory if it was partially downloaded
            if local_file_path.exists():
                failed_file_path = self.failed_path / file_info["name"]
                shutil.move(str(local_file_path), str(failed_file_path))
            
            return DownloadResult(
                success=False,
                file_path=str(local_file_path),
                file_size=0,
                download_time=time.time() - start_time,
                error_message=error_msg
            )
    
    async def _archive_remote_files(self, successful_downloads: List[DownloadResult]):
        """Archive successfully downloaded files on remote server"""
        
        try:
            connection = await self._create_connection()
            
            if connection["type"] == "sftp":
                sftp_client = connection["sftp"]
                
                # Create archive directory if it doesn't exist
                archive_dir = f"{self.config.remote_path}/archive"
                try:
                    sftp_client.mkdir(archive_dir)
                except:
                    pass  # Directory might already exist
                
                # Move files to archive
                for download in successful_downloads:
                    filename = Path(download.file_path).name
                    source_path = f"{self.config.remote_path}/{filename}"
                    archive_path = f"{archive_dir}/{filename}"
                    
                    try:
                        sftp_client.rename(source_path, archive_path)
                        logger.info(f"Archived remote file: {filename}")
                    except Exception as e:
                        logger.warning(f"Failed to archive {filename}: {str(e)}")
            
            await self._close_connection(connection)
            
        except Exception as e:
            logger.error(f"Failed to archive remote files: {str(e)}")
    
    async def _close_connection(self, connection: Dict):
        """Close connection safely"""
        
        try:
            if connection["type"] == "sftp":
                connection["sftp"].close()
                connection["ssh"].close()
            else:  # FTP/FTPS
                connection["client"].quit()
                
        except Exception as e:
            logger.warning(f"Error closing connection: {str(e)}")
    
    async def _health_check(self):
        """Perform health check on SFTP service"""
        
        try:
            logger.info("Performing SFTP service health check")
            
            # Test connection
            connection = await self._create_connection()
            await self._close_connection(connection)
            
            # Check local storage
            free_space = shutil.disk_usage(self.local_download_path).free
            if free_space < 1024 * 1024 * 1024:  # Less than 1GB
                logger.warning(f"Low disk space: {free_space / 1024 / 1024 / 1024:.2f}GB remaining")
            
            logger.info("Health check passed")
            return True
            
        except Exception as e:
            logger.error(f"Health check failed: {str(e)}")
            return False
    
    def stop_scheduled_downloads(self):
        """Stop the scheduled download service"""
        
        self.is_running = False
        schedule.clear()
        logger.info("SFTP automation service stopped")
    
    def get_download_statistics(self) -> Dict[str, Any]:
        """Get comprehensive download statistics"""
        
        if not self.download_history:
            return {"total_sessions": 0, "total_files": 0, "success_rate": 0}
        
        total_sessions = len(self.download_history)
        total_files = sum(session["total_files"] for session in self.download_history)
        successful_files = sum(session["successful"] for session in self.download_history)
        
        return {
            "total_sessions": total_sessions,
            "total_files_processed": total_files,
            "successful_downloads": successful_files,
            "success_rate": successful_files / total_files if total_files > 0 else 0,
            "average_processing_time": sum(session["processing_time"] for session in self.download_history) / total_sessions,
            "last_download": self.download_history[-1]["timestamp"],
            "recent_activity": self.download_history[-5:] if len(self.download_history) >= 5 else self.download_history
        }
    
    async def manual_download(self, specific_files: Optional[List[str]] = None) -> Dict[str, Any]:
        """Manually trigger download of specific files or all files"""
        
        logger.info("Manual download triggered")
        
        if specific_files:
            # Download specific files only
            return await self._download_specific_files(specific_files)
        else:
            # Download all matching files
            return await self.download_all_files()
    
    async def _download_specific_files(self, filenames: List[str]) -> Dict[str, Any]:
        """Download specific files by name"""
        
        try:
            connection = await self._create_connection()
            download_results = []
            
            for filename in filenames:
                file_info = {
                    "name": filename,
                    "size": 0,  # Will be updated during download
                    "remote_path": f"{self.config.remote_path}/{filename}".replace("//", "/")
                }
                
                result = await self._download_single_file(connection, file_info)
                download_results.append(result)
            
            await self._close_connection(connection)
            
            successful = [r for r in download_results if r.success]
            failed = [r for r in download_results if not r.success]
            
            return {
                "success": True,
                "requested_files": len(filenames),
                "successful_downloads": len(successful),
                "failed_downloads": len(failed),
                "downloaded_files": [r.file_path for r in successful],
                "errors": [r.error_message for r in failed if r.error_message]
            }
            
        except Exception as e:
            logger.error(f"Manual download failed: {str(e)}")
            return {"success": False, "error": str(e)}


# Configuration factory for different SFTP providers
class SFTPConfigFactory:
    """Factory for creating SFTP configurations for common providers"""
    
    @staticmethod
    def create_aws_sftp_config(
        host: str, 
        username: str, 
        private_key_path: str,
        remote_path: str = "/",
        file_patterns: List[str] = None
    ) -> SFTPConfig:
        """Create configuration for AWS Transfer Family SFTP"""
        
        return SFTPConfig(
            host=host,
            port=22,
            username=username,
            private_key_path=private_key_path,
            connection_type=ConnectionType.SFTP,
            remote_path=remote_path,
            file_patterns=file_patterns or ["*.csv", "*.xlsx", "*.json"],
            archive_processed=True,
            max_retries=3,
            timeout=30
        )
    
    @staticmethod
    def create_azure_sftp_config(
        host: str,
        username: str,
        password: str,
        remote_path: str = "/",
        file_patterns: List[str] = None
    ) -> SFTPConfig:
        """Create configuration for Azure Blob SFTP"""
        
        return SFTPConfig(
            host=host,
            port=22,
            username=username,
            password=password,
            connection_type=ConnectionType.SFTP,
            remote_path=remote_path,
            file_patterns=file_patterns or ["*.csv", "*.xlsx", "*.json"],
            archive_processed=True,
            max_retries=3,
            timeout=30
        )
    
    @staticmethod
    def create_generic_ftp_config(
        host: str,
        username: str,
        password: str,
        port: int = 21,
        use_tls: bool = False,
        remote_path: str = "/",
        file_patterns: List[str] = None
    ) -> SFTPConfig:
        """Create configuration for generic FTP/FTPS server"""
        
        return SFTPConfig(
            host=host,
            port=port,
            username=username,
            password=password,
            connection_type=ConnectionType.FTPS if use_tls else ConnectionType.FTP,
            remote_path=remote_path,
            file_patterns=file_patterns or ["*.csv", "*.xlsx", "*.json"],
            archive_processed=True,
            max_retries=3,
            timeout=30
        )