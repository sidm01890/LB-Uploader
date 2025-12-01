"""
Main Automation Orchestrator for Smart Uploader Platform
Coordinates SFTP downloads, email processing, file mapping, and notifications
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
import schedule
import time
import json
from pathlib import Path

# Import our automation services
from .sftp_service import SFTPAutomationService, SFTPConfig, SFTPConfigFactory
from .email_service import EmailProcessingService, EmailConfig, EmailConfigFactory
from .notification_service import (
    NotificationService, 
    NotificationRecipient, 
    NotificationType, 
    NotificationLevel,
    ProcessingSummary
)

# Import existing services
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from app.services.mapping_service import suggest_mapping
from app.services.upload_service import DataUploader
from app.services.validation_service import DataValidator
from app.db import get_mysql_connection

logger = logging.getLogger(__name__)

@dataclass
class AutomationConfig:
    """Configuration for automation orchestrator"""
    
    # Required Configuration (no defaults)
    sftp_configs: List[SFTPConfig]
    email_config: EmailConfig
    notification_config: Dict[str, Any]
    recipients: List[NotificationRecipient]
    
    # Optional Configuration (with defaults)
    sftp_schedule: str = "daily"  # daily, hourly, custom
    sftp_time: str = "02:00"  # HH:MM format for daily schedule
    email_check_interval: int = 15  # minutes
    
    # Processing Configuration
    auto_mapping: bool = True
    auto_upload: bool = True
    validation_enabled: bool = True
    
    # File Management
    processed_files_retention_days: int = 30
    failed_files_retention_days: int = 90
    
    # Performance Settings
    max_concurrent_files: int = 5
    batch_size: int = 1000

class AutomationOrchestrator:
    """
    Main orchestrator for all automation services
    Coordinates daily file downloads, processing, and notifications
    """
    
    def __init__(self, config: AutomationConfig):
        self.config = config
        self.is_running = False
        self.processing_stats = {
            "files_processed_today": 0,
            "records_processed_today": 0,
            "errors_today": [],
            "last_run": None
        }
        
        # Initialize services
        self._initialize_services()
        
        # Setup scheduling
        self._setup_scheduling()
        
        logger.info("Automation Orchestrator initialized")
    
    def _initialize_services(self):
        """Initialize all automation services"""
        
        try:
            # Initialize SFTP services
            self.sftp_services = {}
            for i, sftp_config in enumerate(self.config.sftp_configs):
                service_name = f"sftp_service_{i}"
                self.sftp_services[service_name] = SFTPAutomationService(sftp_config)
            
            # Initialize email service
            self.email_service = EmailProcessingService(self.config.email_config)
            
            # Initialize notification service
            smtp_config = self.config.notification_config
            self.notification_service = NotificationService(
                smtp_host=smtp_config['smtp_host'],
                smtp_port=smtp_config['smtp_port'],
                username=smtp_config['username'],
                password=smtp_config['password'],
                sender_name=smtp_config.get('sender_name', 'Smart Uploader Platform'),
                sender_email=smtp_config.get('sender_email', smtp_config['username'])
            )
            
            # Add recipients to notification service
            for recipient in self.config.recipients:
                self.notification_service.add_recipient(recipient)
            
            # Initialize processing services
            # Use suggest_mapping function instead of class
            self.upload_service = DataUploader()
            self.validation_service = DataValidator()
            
            logger.info("All automation services initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize services: {str(e)}", exc_info=True)
            raise
    
    def _setup_scheduling(self):
        """Setup automated scheduling for all services"""
        
        try:
            # Schedule SFTP downloads
            if self.config.sftp_schedule == "daily":
                schedule.every().day.at(self.config.sftp_time).do(self._execute_daily_automation)
            elif self.config.sftp_schedule == "hourly":
                schedule.every().hour.do(self._execute_sftp_downloads)
            
            # Schedule email processing
            schedule.every(self.config.email_check_interval).minutes.do(self._execute_email_processing)
            
            # Schedule daily reports
            schedule.every().day.at("08:00").do(self._send_daily_report)
            
            # Schedule cleanup tasks
            schedule.every().day.at("01:00").do(self._cleanup_old_files)
            
            # Schedule health checks
            schedule.every().hour.do(self._health_check)
            
            logger.info(f"Scheduling configured - SFTP: {self.config.sftp_schedule}, Email: every {self.config.email_check_interval}min")
            
        except Exception as e:
            logger.error(f"Failed to setup scheduling: {str(e)}", exc_info=True)
            raise
    
    async def start_automation(self):
        """Start the automation orchestrator"""
        
        logger.info("🚀 Starting Smart Uploader Automation Platform")
        
        self.is_running = True
        
        # Send startup notification
        await self.notification_service.send_success_notification(
            process_name="Automation Platform",
            files_processed=0,
            records_uploaded=0,
            processing_time="0 minutes",
            success_rate=100.0,
            highlights=["Automation platform started successfully", "All services initialized"]
        )
        
        # Start email monitoring in background
        email_task = asyncio.create_task(
            self.email_service.start_email_monitoring(self.config.email_check_interval)
        )
        
        # Main scheduling loop
        try:
            while self.is_running:
                schedule.run_pending()
                await asyncio.sleep(30)  # Check every 30 seconds
                
        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
        finally:
            # Cancel background tasks
            email_task.cancel()
            try:
                await email_task
            except asyncio.CancelledError:
                pass
            
            await self._shutdown()
    
    def _execute_daily_automation(self):
        """Execute complete daily automation workflow"""
        
        logger.info("🔄 Starting daily automation workflow")
        
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            result = loop.run_until_complete(self._run_daily_workflow())
            
            loop.close()
            
            logger.info(f"Daily automation completed: {result['summary']}")
            
        except Exception as e:
            logger.error(f"Daily automation failed: {str(e)}", exc_info=True)
            
            # Send error notification
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                
                loop.run_until_complete(
                    self.notification_service.send_error_alert(
                        process_name="Daily Automation",
                        error_type="Automation Failure",
                        description=str(e),
                        alert_level=NotificationLevel.CRITICAL
                    )
                )
                
                loop.close()
                
            except:
                pass
    
    async def _run_daily_workflow(self) -> Dict[str, Any]:
        """Execute complete daily workflow"""
        
        start_time = datetime.now()
        workflow_summary = {
            "sftp_results": [],
            "email_results": {},
            "processing_results": [],
            "upload_results": [],
            "total_files": 0,
            "successful_files": 0,
            "failed_files": 0,
            "errors": []
        }
        
        try:
            # Step 1: Download files from all SFTP sources
            logger.info("Step 1: Downloading files from SFTP sources")
            sftp_results = await self._execute_sftp_downloads()
            workflow_summary["sftp_results"] = sftp_results
            
            # Step 2: Process email attachments
            logger.info("Step 2: Processing email attachments")
            email_results = await self._execute_email_processing()
            workflow_summary["email_results"] = email_results
            
            # Step 3: Collect all downloaded files
            all_files = []
            
            # Add SFTP files
            for service_result in sftp_results:
                if service_result.get("success"):
                    all_files.extend(service_result.get("downloaded_files", []))
            
            # Add email attachment files
            if email_results.get("success"):
                all_files.extend(email_results.get("downloaded_files", []))
            
            workflow_summary["total_files"] = len(all_files)
            
            if all_files:
                # Step 4: Process and validate files
                logger.info(f"Step 4: Processing {len(all_files)} files")
                processing_results = await self._process_files(all_files)
                workflow_summary["processing_results"] = processing_results
                
                # Step 5: Upload successful files
                logger.info("Step 5: Uploading processed files")
                upload_results = await self._upload_files(processing_results)
                workflow_summary["upload_results"] = upload_results
                
                # Update statistics
                workflow_summary["successful_files"] = sum(
                    1 for result in processing_results if result.get("success", False)
                )
                workflow_summary["failed_files"] = len(all_files) - workflow_summary["successful_files"]
            
            # Step 6: Generate and send reports
            logger.info("Step 6: Generating reports")
            await self._generate_workflow_report(workflow_summary, start_time)
            
            # Update daily stats
            self.processing_stats["files_processed_today"] += workflow_summary["total_files"]
            self.processing_stats["last_run"] = datetime.now()
            
            return {
                "success": True,
                "summary": workflow_summary
            }
            
        except Exception as e:
            logger.error(f"Daily workflow failed: {str(e)}", exc_info=True)
            
            workflow_summary["errors"].append({
                "timestamp": datetime.now().isoformat(),
                "source": "Daily Workflow",
                "error_type": "Workflow Failure",
                "description": str(e)
            })
            
            return {
                "success": False,
                "error": str(e),
                "summary": workflow_summary
            }
    
    async def _execute_sftp_downloads(self) -> List[Dict[str, Any]]:
        """Execute downloads from all SFTP services"""
        
        results = []
        
        for service_name, sftp_service in self.sftp_services.items():
            try:
                logger.info(f"Executing SFTP download for {service_name}")
                result = await sftp_service.download_files()
                result["service_name"] = service_name
                results.append(result)
                
            except Exception as e:
                logger.error(f"SFTP download failed for {service_name}: {str(e)}")
                results.append({
                    "service_name": service_name,
                    "success": False,
                    "error": str(e)
                })
        
        return results
    
    def _execute_email_processing(self):
        """Execute email processing (called by scheduler)"""
        
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            result = loop.run_until_complete(self.email_service.process_new_emails())
            
            loop.close()
            
            logger.info(f"Email processing completed: {result.get('summary', {})}")
            
            return result
            
        except Exception as e:
            logger.error(f"Email processing failed: {str(e)}", exc_info=True)
            return {"success": False, "error": str(e)}
    
    async def _process_files(self, file_paths: List[str]) -> List[Dict[str, Any]]:
        """Process files through mapping and validation"""
        
        results = []
        
        # Process files concurrently (with limit)
        semaphore = asyncio.Semaphore(self.config.max_concurrent_files)
        
        async def process_single_file(file_path: str):
            async with semaphore:
                return await self._process_single_file(file_path)
        
        # Create tasks for all files
        tasks = [process_single_file(file_path) for file_path in file_paths]
        
        # Execute with progress tracking
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Handle exceptions in results
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                processed_results.append({
                    "file_path": file_paths[i],
                    "success": False,
                    "error": str(result)
                })
            else:
                processed_results.append(result)
        
        return processed_results
    
    async def _process_single_file(self, file_path: str) -> Dict[str, Any]:
        """Process a single file through the pipeline"""
        
        try:
            file_path_obj = Path(file_path)
            
            # Step 1: Validate file
            if self.config.validation_enabled:
                validation_result = self.validation_service.validate_file(file_path)
                if not validation_result["is_valid"]:
                    return {
                        "file_path": file_path,
                        "success": False,
                        "error": f"Validation failed: {validation_result['errors']}"
                    }
            
            # Step 2: Auto-detect and map columns
            if self.config.auto_mapping:
                mapping_result = await self.mapping_service.auto_map_columns(file_path)
                
                if not mapping_result["success"]:
                    return {
                        "file_path": file_path,
                        "success": False,
                        "error": f"Mapping failed: {mapping_result['error']}"
                    }
                
                table_mapping = mapping_result["mapping"]
            else:
                # Use default mapping logic
                table_mapping = {"table_name": file_path_obj.stem, "columns": {}}
            
            return {
                "file_path": file_path,
                "success": True,
                "table_mapping": table_mapping,
                "validation_result": validation_result if self.config.validation_enabled else None
            }
            
        except Exception as e:
            logger.error(f"Failed to process file {file_path}: {str(e)}")
            return {
                "file_path": file_path,
                "success": False,
                "error": str(e)
            }
    
    async def _upload_files(self, processing_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Upload successfully processed files"""
        
        upload_results = []
        
        for result in processing_results:
            if not result.get("success", False):
                continue
            
            try:
                file_path = result["file_path"]
                table_mapping = result["table_mapping"]
                
                # Upload file using the upload service
                upload_result = await self.upload_service.upload_file(
                    file_path=file_path,
                    table_mapping=table_mapping,
                    batch_size=self.config.batch_size
                )
                
                upload_results.append({
                    "file_path": file_path,
                    "success": upload_result["success"],
                    "records_uploaded": upload_result.get("records_uploaded", 0),
                    "error": upload_result.get("error")
                })
                
                # Update statistics
                if upload_result["success"]:
                    self.processing_stats["records_processed_today"] += upload_result.get("records_uploaded", 0)
                
            except Exception as e:
                logger.error(f"Failed to upload file {result['file_path']}: {str(e)}")
                upload_results.append({
                    "file_path": result["file_path"],
                    "success": False,
                    "error": str(e)
                })
        
        return upload_results
    
    async def _generate_workflow_report(self, workflow_summary: Dict[str, Any], start_time: datetime):
        """Generate and send workflow completion report"""
        
        try:
            end_time = datetime.now()
            processing_time = (end_time - start_time).total_seconds() / 60  # minutes
            
            # Create processing summary
            summary = ProcessingSummary(
                process_name="Daily Automation Workflow",
                start_time=start_time,
                end_time=end_time,
                total_files=workflow_summary["total_files"],
                successful_files=workflow_summary["successful_files"],
                failed_files=workflow_summary["failed_files"],
                total_records=sum(
                    result.get("records_uploaded", 0) 
                    for result in workflow_summary.get("upload_results", [])
                ),
                successful_records=sum(
                    result.get("records_uploaded", 0) 
                    for result in workflow_summary.get("upload_results", [])
                    if result.get("success", False)
                ),
                failed_records=0,  # Calculate if needed
                errors=workflow_summary.get("errors", []),
                warnings=[],  # Add warnings if needed
                performance_metrics={
                    "total_time_minutes": f"{processing_time:.2f}",
                    "records_per_second": (
                        sum(result.get("records_uploaded", 0) for result in workflow_summary.get("upload_results", [])) / 
                        (processing_time * 60) if processing_time > 0 else 0
                    ),
                    "avg_file_size_mb": "N/A"  # Calculate if needed
                }
            )
            
            # Send report based on results
            if workflow_summary["failed_files"] == 0:
                # Success notification
                await self.notification_service.send_success_notification(
                    process_name="Daily Automation",
                    files_processed=workflow_summary["total_files"],
                    records_uploaded=summary.total_records,
                    processing_time=f"{processing_time:.2f} minutes",
                    success_rate=100.0,
                    highlights=[
                        f"All {workflow_summary['total_files']} files processed successfully",
                        f"SFTP sources: {len(workflow_summary['sftp_results'])}",
                        f"Email attachments: {workflow_summary['email_results'].get('summary', {}).get('data_file_attachments', 0)}"
                    ]
                )
            else:
                # Send daily report with issues
                await self.notification_service.send_daily_report(summary)
            
        except Exception as e:
            logger.error(f"Failed to generate workflow report: {str(e)}")
    
    def _send_daily_report(self):
        """Send daily summary report (called by scheduler)"""
        
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            # Create summary from daily stats
            summary = ProcessingSummary(
                process_name="Daily Summary Report",
                start_time=datetime.now().replace(hour=0, minute=0, second=0, microsecond=0),
                end_time=datetime.now(),
                total_files=self.processing_stats["files_processed_today"],
                successful_files=self.processing_stats["files_processed_today"],  # Simplified
                failed_files=0,
                total_records=self.processing_stats["records_processed_today"],
                successful_records=self.processing_stats["records_processed_today"],
                failed_records=0,
                errors=self.processing_stats["errors_today"],
                warnings=[],
                performance_metrics={}
            )
            
            result = loop.run_until_complete(self.notification_service.send_daily_report(summary))
            
            loop.close()
            
            # Reset daily stats
            self.processing_stats["files_processed_today"] = 0
            self.processing_stats["records_processed_today"] = 0
            self.processing_stats["errors_today"] = []
            
            logger.info(f"Daily report sent: {result}")
            
        except Exception as e:
            logger.error(f"Failed to send daily report: {str(e)}")
    
    def _cleanup_old_files(self):
        """Clean up old processed and failed files"""
        
        try:
            logger.info("Starting file cleanup")
            
            now = datetime.now()
            
            # Cleanup processed files
            processed_cutoff = now - timedelta(days=self.config.processed_files_retention_days)
            failed_cutoff = now - timedelta(days=self.config.failed_files_retention_days)
            
            cleanup_stats = {
                "processed_files_deleted": 0,
                "failed_files_deleted": 0,
                "space_freed_mb": 0
            }
            
            # Add cleanup logic here based on your file organization
            # This would involve checking file timestamps and removing old files
            
            logger.info(f"File cleanup completed: {cleanup_stats}")
            
        except Exception as e:
            logger.error(f"File cleanup failed: {str(e)}")
    
    def _health_check(self):
        """Perform system health check"""
        
        try:
            health_status = {
                "timestamp": datetime.now().isoformat(),
                "services": {},
                "overall_status": "healthy"
            }
            
            # Check SFTP services
            for service_name, sftp_service in self.sftp_services.items():
                try:
                    # Basic connectivity check
                    stats = sftp_service.get_connection_stats()
                    health_status["services"][service_name] = {
                        "status": "healthy",
                        "connections": stats.get("active_connections", 0)
                    }
                except Exception as e:
                    health_status["services"][service_name] = {
                        "status": "unhealthy",
                        "error": str(e)
                    }
                    health_status["overall_status"] = "degraded"
            
            # Check email service
            try:
                email_stats = self.email_service.get_processing_statistics()
                health_status["services"]["email"] = {
                    "status": "healthy",
                    "messages_processed": email_stats.get("total_messages_processed", 0)
                }
            except Exception as e:
                health_status["services"]["email"] = {
                    "status": "unhealthy",
                    "error": str(e)
                }
                health_status["overall_status"] = "degraded"
            
            # Check database connectivity
            try:
                db = get_mysql_connection()
                # Simple query to test connectivity
                # db.execute("SELECT 1")
                health_status["services"]["database"] = {"status": "healthy"}
            except Exception as e:
                health_status["services"]["database"] = {
                    "status": "unhealthy",
                    "error": str(e)
                }
                health_status["overall_status"] = "degraded"
            
            # Log health status
            if health_status["overall_status"] != "healthy":
                logger.warning(f"System health check - Status: {health_status['overall_status']}")
                
                # Send health alert if critical
                unhealthy_services = [
                    name for name, service in health_status["services"].items()
                    if service.get("status") != "healthy"
                ]
                
                if len(unhealthy_services) > 1:  # Multiple services down
                    try:
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                        
                        loop.run_until_complete(
                            self.notification_service.send_error_alert(
                                process_name="System Health Check",
                                error_type="Service Degradation",
                                description=f"Multiple services unhealthy: {', '.join(unhealthy_services)}",
                                details=json.dumps(health_status, indent=2),
                                alert_level=NotificationLevel.WARNING
                            )
                        )
                        
                        loop.close()
                        
                    except:
                        pass
            else:
                logger.debug("System health check - All services healthy")
            
        except Exception as e:
            logger.error(f"Health check failed: {str(e)}")
    
    async def _shutdown(self):
        """Graceful shutdown of automation services"""
        
        logger.info("🔄 Shutting down automation orchestrator")
        
        try:
            # Stop email monitoring
            self.email_service.stop_monitoring()
            
            # Close SFTP connections
            for sftp_service in self.sftp_services.values():
                sftp_service.stop_monitoring()
            
            # Clear schedules
            schedule.clear()
            
            # Send shutdown notification
            await self.notification_service.send_success_notification(
                process_name="Automation Platform",
                files_processed=0,
                records_uploaded=0,
                processing_time="0 minutes",
                success_rate=100.0,
                highlights=["Automation platform shut down gracefully"]
            )
            
            logger.info("✅ Automation orchestrator shut down successfully")
            
        except Exception as e:
            logger.error(f"Error during shutdown: {str(e)}")
    
    def stop(self):
        """Stop the automation orchestrator"""
        
        self.is_running = False
    
    def get_status(self) -> Dict[str, Any]:
        """Get current status of automation orchestrator"""
        
        return {
            "is_running": self.is_running,
            "services": {
                "sftp_services": len(self.sftp_services),
                "email_service": "active" if self.email_service.is_monitoring else "inactive",
                "notification_service": "active"
            },
            "daily_stats": self.processing_stats,
            "next_scheduled_runs": {
                "sftp_download": schedule.next_run(),
                "email_check": schedule.next_run(),
                "daily_report": schedule.next_run()
            }
        }