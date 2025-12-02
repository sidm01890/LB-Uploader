"""
Scheduled Job Manager for Smart Uploader Platform
Manages recurring automation jobs using APScheduler
"""

import logging
from datetime import datetime, time, timedelta
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass
from enum import Enum
import asyncio
import json

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.executors.asyncio import AsyncIOExecutor

# Import automation services
from app.automation.sftp_service import SFTPAutomationService, SFTPConfig
from app.automation.email_service import EmailProcessingService, EmailConfig
from app.automation.notification_service import NotificationService, NotificationLevel, NotificationType
from app.automation.orchestrator import AutomationOrchestrator, AutomationConfig
from app.automation.config_factory import AutomationConfigFactory

logger = logging.getLogger(__name__)

class JobType(str, Enum):
    SFTP_DOWNLOAD = "sftp_download"
    EMAIL_PROCESSING = "email_processing"
    FULL_WORKFLOW = "full_workflow"
    DAILY_REPORT = "daily_report"
    FILE_CLEANUP = "file_cleanup"
    HEALTH_CHECK = "health_check"

class JobFrequency(str, Enum):
    ONCE = "once"
    MINUTELY = "minutely"
    HOURLY = "hourly"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    CUSTOM_CRON = "custom_cron"

@dataclass
class ScheduledJobConfig:
    job_id: str
    job_name: str
    job_type: JobType
    frequency: JobFrequency
    schedule_params: Dict[str, Any]  # Cron parameters or interval settings
    job_config: Dict[str, Any]  # Job-specific configuration
    enabled: bool = True
    max_retries: int = 3
    retry_delay_minutes: int = 5
    timeout_minutes: int = 60
    notification_on_success: bool = False
    notification_on_failure: bool = True

class ScheduledJobManager:
    """
    Manages scheduled automation jobs using APScheduler
    """
    
    def __init__(self):
        # Configure job stores and executors
        jobstores = {
            'default': MemoryJobStore()
        }
        
        executors = {
            'default': AsyncIOExecutor(),
        }
        
        job_defaults = {
            'coalesce': False,
            'max_instances': 1,
            'misfire_grace_time': 300  # 5 minutes
        }
        
        # Initialize scheduler
        self.scheduler = AsyncIOScheduler(
            jobstores=jobstores,
            executors=executors,
            job_defaults=job_defaults
        )
        
        # Job tracking
        self.scheduled_jobs: Dict[str, ScheduledJobConfig] = {}
        self.job_execution_history: List[Dict[str, Any]] = []
        self.notification_service: Optional[NotificationService] = None
        
        # Services cache
        self.service_cache: Dict[str, Any] = {}
        
        logger.info("Scheduled job manager initialized")
    
    def set_notification_service(self, notification_service: NotificationService):
        """Set notification service for job status updates"""
        self.notification_service = notification_service
    
    async def start_scheduler(self):
        """Start the job scheduler"""
        
        try:
            self.scheduler.start()
            logger.info("✅ Job scheduler started successfully")
            
            # Add default system jobs
            await self._add_default_system_jobs()
            
        except Exception as e:
            logger.error(f"Failed to start scheduler: {str(e)}", exc_info=True)
            raise
    
    async def stop_scheduler(self):
        """Stop the job scheduler"""
        
        try:
            self.scheduler.shutdown(wait=True)
            logger.info("Job scheduler stopped")
            
        except Exception as e:
            logger.error(f"Error stopping scheduler: {str(e)}")
    
    async def add_scheduled_job(self, job_config: ScheduledJobConfig) -> str:
        """Add a new scheduled job"""
        
        try:
            # Validate job configuration
            self._validate_job_config(job_config)
            
            # Create trigger based on frequency
            trigger = self._create_trigger(job_config.frequency, job_config.schedule_params)
            
            # Wrap job execution with error handling and logging
            job_executor = self._create_job_executor(job_config)
            
            # Add job to scheduler
            self.scheduler.add_job(
                func=job_executor,
                trigger=trigger,
                id=job_config.job_id,
                name=job_config.job_name,
                replace_existing=True,
                max_instances=1
            )
            
            # Store job configuration
            self.scheduled_jobs[job_config.job_id] = job_config
            
            logger.info(f"✅ Scheduled job added: {job_config.job_name} ({job_config.job_id})")
            
            return job_config.job_id
            
        except Exception as e:
            logger.error(f"Failed to add scheduled job {job_config.job_id}: {str(e)}", exc_info=True)
            raise
    
    def _validate_job_config(self, job_config: ScheduledJobConfig):
        """Validate job configuration"""
        
        if not job_config.job_id:
            raise ValueError("Job ID is required")
        
        if not job_config.job_name:
            raise ValueError("Job name is required")
        
        if job_config.job_type not in JobType:
            raise ValueError(f"Invalid job type: {job_config.job_type}")
        
        if job_config.frequency not in JobFrequency:
            raise ValueError(f"Invalid frequency: {job_config.frequency}")
    
    def _create_trigger(self, frequency: JobFrequency, schedule_params: Dict[str, Any]):
        """Create scheduler trigger based on frequency"""
        
        if frequency == JobFrequency.ONCE:
            # Run once at specified time
            run_date = schedule_params.get("run_date")
            if isinstance(run_date, str):
                run_date = datetime.fromisoformat(run_date)
            
            from apscheduler.triggers.date import DateTrigger
            return DateTrigger(run_date=run_date)
        
        elif frequency == JobFrequency.MINUTELY:
            minutes = schedule_params.get("minutes", 1)
            return IntervalTrigger(minutes=minutes)
        
        elif frequency == JobFrequency.HOURLY:
            hours = schedule_params.get("hours", 1)
            return IntervalTrigger(hours=hours)
        
        elif frequency == JobFrequency.DAILY:
            hour = schedule_params.get("hour", 2)
            minute = schedule_params.get("minute", 0)
            return CronTrigger(hour=hour, minute=minute)
        
        elif frequency == JobFrequency.WEEKLY:
            day_of_week = schedule_params.get("day_of_week", 0)  # Monday
            hour = schedule_params.get("hour", 2)
            minute = schedule_params.get("minute", 0)
            return CronTrigger(day_of_week=day_of_week, hour=hour, minute=minute)
        
        elif frequency == JobFrequency.MONTHLY:
            day = schedule_params.get("day", 1)
            hour = schedule_params.get("hour", 2)
            minute = schedule_params.get("minute", 0)
            return CronTrigger(day=day, hour=hour, minute=minute)
        
        elif frequency == JobFrequency.CUSTOM_CRON:
            cron_expression = schedule_params.get("cron_expression")
            if not cron_expression:
                raise ValueError("Cron expression required for custom_cron frequency")
            
            # Parse cron expression
            parts = cron_expression.split()
            if len(parts) != 5:
                raise ValueError("Invalid cron expression format")
            
            return CronTrigger(
                minute=parts[0],
                hour=parts[1],
                day=parts[2],
                month=parts[3],
                day_of_week=parts[4]
            )
        
        else:
            raise ValueError(f"Unsupported frequency: {frequency}")
    
    def _create_job_executor(self, job_config: ScheduledJobConfig) -> Callable:
        """Create job executor with error handling and logging"""
        
        async def job_executor():
            execution_id = f"{job_config.job_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            start_time = datetime.now()
            
            execution_record = {
                "execution_id": execution_id,
                "job_id": job_config.job_id,
                "job_name": job_config.job_name,
                "job_type": job_config.job_type,
                "started_at": start_time,
                "status": "running",
                "result": None,
                "error": None,
                "retry_count": 0
            }
            
            self.job_execution_history.append(execution_record)
            
            try:
                logger.info(f"🚀 Starting scheduled job: {job_config.job_name} ({execution_id})")
                
                # Execute job based on type
                if job_config.job_type == JobType.SFTP_DOWNLOAD:
                    result = await self._execute_sftp_job(job_config.job_config)
                
                elif job_config.job_type == JobType.EMAIL_PROCESSING:
                    result = await self._execute_email_job(job_config.job_config)
                
                elif job_config.job_type == JobType.FULL_WORKFLOW:
                    result = await self._execute_workflow_job(job_config.job_config)
                
                elif job_config.job_type == JobType.DAILY_REPORT:
                    result = await self._execute_daily_report_job(job_config.job_config)
                
                elif job_config.job_type == JobType.FILE_CLEANUP:
                    result = await self._execute_cleanup_job(job_config.job_config)
                
                elif job_config.job_type == JobType.HEALTH_CHECK:
                    result = await self._execute_health_check_job(job_config.job_config)
                
                else:
                    raise ValueError(f"Unsupported job type: {job_config.job_type}")
                
                # Update execution record
                execution_record["status"] = "completed" if result.get("success") else "failed"
                execution_record["completed_at"] = datetime.now()
                execution_record["execution_time_seconds"] = (
                    execution_record["completed_at"] - start_time
                ).total_seconds()
                execution_record["result"] = result
                
                if not result.get("success"):
                    execution_record["error"] = result.get("error", "Unknown error")
                
                # Send notifications
                await self._send_job_notification(job_config, execution_record)
                
                logger.info(f"✅ Completed scheduled job: {job_config.job_name} ({execution_id})")
                
            except Exception as e:
                error_msg = str(e)
                logger.error(f"❌ Scheduled job failed: {job_config.job_name} ({execution_id}): {error_msg}", exc_info=True)
                
                # Update execution record
                execution_record["status"] = "failed"
                execution_record["completed_at"] = datetime.now()
                execution_record["execution_time_seconds"] = (
                    execution_record["completed_at"] - start_time
                ).total_seconds()
                execution_record["error"] = error_msg
                
                # Send failure notification
                await self._send_job_notification(job_config, execution_record)
                
                # TODO: Implement retry logic here if needed
        
        return job_executor
    
    async def _execute_sftp_job(self, job_config: Dict[str, Any]) -> Dict[str, Any]:
        """Execute SFTP download job"""
        
        try:
            # Create or get cached SFTP service
            service_key = f"sftp_{hash(json.dumps(job_config, sort_keys=True))}"
            
            if service_key not in self.service_cache:
                sftp_config = SFTPConfig(**job_config["sftp_config"])
                self.service_cache[service_key] = SFTPAutomationService(sftp_config)
            
            sftp_service = self.service_cache[service_key]
            
            # Execute download
            result = await sftp_service.download_files()
            
            return {
                "success": True,
                "job_type": "sftp_download",
                "files_downloaded": result.get("files_downloaded", []),
                "download_count": result.get("download_count", 0),
                "total_size_mb": result.get("total_size_mb", 0)
            }
            
        except Exception as e:
            return {
                "success": False,
                "job_type": "sftp_download",
                "error": str(e)
            }
    
    async def _execute_email_job(self, job_config: Dict[str, Any]) -> Dict[str, Any]:
        """Execute email processing job"""
        
        try:
            # Create or get cached email service
            service_key = f"email_{hash(json.dumps(job_config, sort_keys=True))}"
            
            if service_key not in self.service_cache:
                email_config = EmailConfig(**job_config["email_config"])
                self.service_cache[service_key] = EmailProcessingService(email_config)
            
            email_service = self.service_cache[service_key]
            
            # Execute email processing
            result = await email_service.process_new_emails()
            
            return {
                "success": True,
                "job_type": "email_processing",
                "messages_processed": result.get("summary", {}).get("total_messages", 0),
                "attachments_downloaded": result.get("summary", {}).get("total_attachments", 0),
                "data_files": result.get("summary", {}).get("data_file_attachments", 0)
            }
            
        except Exception as e:
            return {
                "success": False,
                "job_type": "email_processing",
                "error": str(e)
            }
    
    async def _execute_workflow_job(self, job_config: Dict[str, Any]) -> Dict[str, Any]:
        """Execute full workflow job"""
        
        try:
            # Create automation configuration
            if job_config.get("environment") == "production":
                config = AutomationConfigFactory.create_production_config()
            elif job_config.get("environment") == "enterprise":
                config = AutomationConfigFactory.create_enterprise_config()
            else:
                config = AutomationConfigFactory.create_development_config()
            
            # Create orchestrator
            orchestrator = AutomationOrchestrator(config)
            
            # Execute workflow
            result = await orchestrator._run_daily_workflow()
            
            return {
                "success": result.get("success", False),
                "job_type": "full_workflow",
                "summary": result.get("summary", {}),
                "error": result.get("error")
            }
            
        except Exception as e:
            return {
                "success": False,
                "job_type": "full_workflow",
                "error": str(e)
            }
    
    async def _execute_daily_report_job(self, job_config: Dict[str, Any]) -> Dict[str, Any]:
        """Execute daily report generation job"""
        
        try:
            if not self.notification_service:
                return {
                    "success": False,
                    "job_type": "daily_report",
                    "error": "Notification service not configured"
                }
            
            # Generate report based on recent job history
            recent_cutoff = datetime.now() - timedelta(hours=24)
            recent_executions = [
                exec_record for exec_record in self.job_execution_history
                if exec_record.get("started_at", datetime.min) > recent_cutoff
            ]
            
            # Create processing summary
            from app.automation.notification_service import ProcessingSummary
            
            total_jobs = len(recent_executions)
            successful_jobs = len([e for e in recent_executions if e.get("status") == "completed"])
            failed_jobs = len([e for e in recent_executions if e.get("status") == "failed"])
            
            summary = ProcessingSummary(
                process_name="Daily Scheduled Jobs Report",
                start_time=recent_cutoff,
                end_time=datetime.now(),
                total_files=total_jobs,
                successful_files=successful_jobs,
                failed_files=failed_jobs,
                total_records=0,  # Could aggregate from job results
                successful_records=0,
                failed_records=0,
                errors=[
                    {
                        "timestamp": e.get("completed_at", datetime.now()).isoformat(),
                        "source": e.get("job_name", "Unknown"),
                        "error_type": "Job Failure",
                        "description": e.get("error", "Unknown error")
                    }
                    for e in recent_executions if e.get("status") == "failed"
                ],
                warnings=[],
                performance_metrics={
                    "total_scheduled_jobs": total_jobs,
                    "success_rate": (successful_jobs / total_jobs * 100) if total_jobs > 0 else 0
                }
            )
            
            # Send daily report
            result = await self.notification_service.send_daily_report(summary)
            
            return {
                "success": result.get("success", False),
                "job_type": "daily_report",
                "recipients_count": result.get("recipients_count", 0),
                "jobs_in_report": total_jobs
            }
            
        except Exception as e:
            return {
                "success": False,
                "job_type": "daily_report",
                "error": str(e)
            }
    
    async def _execute_cleanup_job(self, job_config: Dict[str, Any]) -> Dict[str, Any]:
        """Execute file cleanup job"""
        
        try:
            retention_days = job_config.get("retention_days", 30)
            
            # Cleanup job execution history
            cutoff_date = datetime.now() - timedelta(days=retention_days)
            initial_count = len(self.job_execution_history)
            
            self.job_execution_history = [
                record for record in self.job_execution_history
                if record.get("started_at", datetime.max) > cutoff_date
            ]
            
            cleaned_count = initial_count - len(self.job_execution_history)
            
            return {
                "success": True,
                "job_type": "file_cleanup",
                "records_cleaned": cleaned_count,
                "retention_days": retention_days
            }
            
        except Exception as e:
            return {
                "success": False,
                "job_type": "file_cleanup",
                "error": str(e)
            }
    
    async def _execute_health_check_job(self, job_config: Dict[str, Any]) -> Dict[str, Any]:
        """Execute system health check job"""
        
        try:
            # Basic health metrics
            health_status = {
                "scheduler_running": self.scheduler.running,
                "active_jobs": len(self.scheduler.get_jobs()),
                "execution_history_size": len(self.job_execution_history),
                "service_cache_size": len(self.service_cache)
            }
            
            # Check recent job success rate
            recent_cutoff = datetime.now() - timedelta(hours=1)
            recent_jobs = [
                record for record in self.job_execution_history
                if record.get("started_at", datetime.min) > recent_cutoff
            ]
            
            if recent_jobs:
                successful_recent = len([j for j in recent_jobs if j.get("status") == "completed"])
                health_status["recent_success_rate"] = successful_recent / len(recent_jobs) * 100
            else:
                health_status["recent_success_rate"] = 100
            
            # Determine overall health
            is_healthy = (
                health_status["scheduler_running"] and
                health_status["recent_success_rate"] >= 80  # 80% success rate threshold
            )
            
            # Send alert if unhealthy
            if not is_healthy and self.notification_service:
                await self.notification_service.send_error_alert(
                    process_name="Scheduled Job System",
                    error_type="Health Check Failed",
                    description=f"System health check failed. Recent success rate: {health_status['recent_success_rate']:.1f}%",
                    details=json.dumps(health_status, indent=2),
                    alert_level=NotificationLevel.WARNING
                )
            
            return {
                "success": True,
                "job_type": "health_check",
                "health_status": health_status,
                "is_healthy": is_healthy
            }
            
        except Exception as e:
            return {
                "success": False,
                "job_type": "health_check",
                "error": str(e)
            }
    
    async def _send_job_notification(self, job_config: ScheduledJobConfig, execution_record: Dict[str, Any]):
        """Send notification for job completion"""
        
        try:
            if not self.notification_service:
                return
            
            status = execution_record.get("status")
            
            # Send success notification if configured
            if status == "completed" and job_config.notification_on_success:
                await self.notification_service.send_success_notification(
                    process_name=job_config.job_name,
                    files_processed=execution_record.get("result", {}).get("download_count", 0),
                    records_uploaded=0,  # Could be extracted from result
                    processing_time=f"{execution_record.get('execution_time_seconds', 0):.2f} seconds",
                    success_rate=100.0
                )
            
            # Send failure notification if configured
            elif status == "failed" and job_config.notification_on_failure:
                await self.notification_service.send_error_alert(
                    process_name=job_config.job_name,
                    error_type="Scheduled Job Failure",
                    description=execution_record.get("error", "Unknown error"),
                    alert_level=NotificationLevel.ERROR
                )
                
        except Exception as e:
            logger.warning(f"Failed to send job notification: {str(e)}")
    
    async def remove_scheduled_job(self, job_id: str) -> bool:
        """Remove a scheduled job"""
        
        try:
            # Remove from scheduler
            self.scheduler.remove_job(job_id)
            
            # Remove from job tracking
            if job_id in self.scheduled_jobs:
                del self.scheduled_jobs[job_id]
            
            logger.info(f"Removed scheduled job: {job_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to remove scheduled job {job_id}: {str(e)}")
            return False
    
    def get_scheduled_jobs(self) -> List[Dict[str, Any]]:
        """Get list of all scheduled jobs"""
        
        jobs = []
        
        for job_id, job_config in self.scheduled_jobs.items():
            scheduler_job = self.scheduler.get_job(job_id)
            
            job_info = {
                "job_id": job_id,
                "job_name": job_config.job_name,
                "job_type": job_config.job_type,
                "frequency": job_config.frequency,
                "enabled": job_config.enabled,
                "next_run": scheduler_job.next_run_time if scheduler_job else None,
                "max_retries": job_config.max_retries,
                "notification_on_success": job_config.notification_on_success,
                "notification_on_failure": job_config.notification_on_failure
            }
            
            jobs.append(job_info)
        
        return jobs
    
    def get_job_execution_history(self, job_id: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """Get job execution history"""
        
        history = self.job_execution_history
        
        # Filter by job_id if provided
        if job_id:
            history = [record for record in history if record.get("job_id") == job_id]
        
        # Sort by start time (newest first) and apply limit
        history.sort(key=lambda x: x.get("started_at", datetime.min), reverse=True)
        
        return history[:limit]
    
    async def _add_default_system_jobs(self):
        """Add default system maintenance jobs"""
        
        try:
            # Daily cleanup job
            cleanup_job = ScheduledJobConfig(
                job_id="system_cleanup_daily",
                job_name="Daily System Cleanup",
                job_type=JobType.FILE_CLEANUP,
                frequency=JobFrequency.DAILY,
                schedule_params={"hour": 1, "minute": 0},  # 1:00 AM daily
                job_config={"retention_days": 30},
                notification_on_success=False,
                notification_on_failure=True
            )
            
            await self.add_scheduled_job(cleanup_job)
            
            # Hourly health check
            health_job = ScheduledJobConfig(
                job_id="system_health_hourly",
                job_name="Hourly Health Check",
                job_type=JobType.HEALTH_CHECK,
                frequency=JobFrequency.HOURLY,
                schedule_params={"hours": 1},
                job_config={},
                notification_on_success=False,
                notification_on_failure=True
            )
            
            await self.add_scheduled_job(health_job)
            
            logger.info("✅ Default system jobs added successfully")
            
        except Exception as e:
            logger.error(f"Failed to add default system jobs: {str(e)}")

# Global instance
job_manager = ScheduledJobManager()

async def startup_job_manager():
    """Startup function for the job manager"""
    try:
        await job_manager.start_scheduler()
        logger.info("✅ Job manager started successfully")
    except Exception as e:
        logger.error(f"❌ Failed to start job manager: {e}")
        raise

async def shutdown_job_manager():
    """Shutdown function for the job manager"""
    try:
        await job_manager.stop_scheduler()
        logger.info("✅ Job manager stopped successfully")
    except Exception as e:
        logger.error(f"❌ Failed to stop job manager: {e}")
        raise