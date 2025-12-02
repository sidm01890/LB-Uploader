"""
Automation API Routes for Smart Uploader Platform
Provides REST API endpoints for automation services and job management
"""

from fastapi import APIRouter, HTTPException, BackgroundTasks, Depends, Query
from fastapi.responses import JSONResponse
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from pydantic import BaseModel, Field
import asyncio
import logging
import json
from enum import Enum

# Import automation services
from app.automation.sftp_service import SFTPAutomationService, SFTPConfig, SFTPConfigFactory
from app.automation.email_service import EmailProcessingService, EmailConfig, EmailConfigFactory
from app.automation.notification_service import (
    NotificationService, 
    NotificationRecipient, 
    NotificationType, 
    NotificationLevel,
    ProcessingSummary
)
from app.automation.orchestrator import AutomationOrchestrator, AutomationConfig
from app.automation.config_factory import AutomationConfigFactory

logger = logging.getLogger(__name__)

# Create router
automation_router = APIRouter(prefix="/api/v1/automation", tags=["Automation"])

# Pydantic models for API requests/responses
class SFTPConnectionConfig(BaseModel):
    host: str = Field(..., description="SFTP server hostname")
    port: int = Field(22, description="SFTP server port")
    username: str = Field(..., description="SFTP username")
    password: str = Field(..., description="SFTP password")
    remote_directories: List[str] = Field(..., description="Remote directories to monitor")
    file_patterns: List[str] = Field(["*"], description="File patterns to download")
    download_path: str = Field("/tmp/sftp_downloads", description="Local download path")
    connection_type: str = Field("sftp", description="Connection type: sftp, ftp, or ftps")

class EmailConnectionConfig(BaseModel):
    provider: str = Field(..., description="Email provider: gmail, outlook, exchange")
    host: str = Field(..., description="IMAP server host")
    port: int = Field(993, description="IMAP server port")
    username: str = Field(..., description="Email username")
    password: str = Field(..., description="Email password")
    allowed_senders: List[str] = Field([], description="Allowed sender email addresses")
    download_path: str = Field("/tmp/email_downloads", description="Attachment download path")

class NotificationRecipientConfig(BaseModel):
    name: str = Field(..., description="Recipient name")
    email: str = Field(..., description="Recipient email")
    notification_types: List[str] = Field(..., description="Types of notifications to receive")
    level_threshold: str = Field("info", description="Minimum notification level")

class JobScheduleConfig(BaseModel):
    schedule_type: str = Field(..., description="Schedule type: daily, hourly, custom")
    schedule_time: Optional[str] = Field(None, description="Time for daily schedule (HH:MM)")
    interval_minutes: Optional[int] = Field(None, description="Interval for custom schedule")
    enabled: bool = Field(True, description="Whether the job is enabled")

class AutomationJobRequest(BaseModel):
    job_name: str = Field(..., description="Name of the automation job")
    job_type: str = Field(..., description="Type: sftp_download, email_processing, full_workflow")
    sftp_config: Optional[SFTPConnectionConfig] = None
    email_config: Optional[EmailConnectionConfig] = None
    schedule_config: JobScheduleConfig
    auto_mapping: bool = Field(True, description="Enable automatic column mapping")
    auto_upload: bool = Field(True, description="Enable automatic data upload")
    notification_recipients: List[NotificationRecipientConfig] = Field([], description="Notification recipients")

class JobStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

class JobExecutionResponse(BaseModel):
    job_id: str
    job_name: str
    status: JobStatus
    started_at: datetime
    completed_at: Optional[datetime] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None

# Global job storage (in production, use Redis or database)
active_jobs: Dict[str, Dict[str, Any]] = {}
job_history: List[Dict[str, Any]] = []
automation_orchestrators: Dict[str, AutomationOrchestrator] = {}

# Utility functions
def generate_job_id() -> str:
    """Generate unique job ID"""
    import uuid
    return f"job_{uuid.uuid4().hex[:8]}"

async def execute_sftp_job(job_id: str, config: SFTPConnectionConfig) -> Dict[str, Any]:
    """Execute SFTP download job"""
    
    try:
        # Create SFTP configuration
        sftp_config = SFTPConfig(
            host=config.host,
            port=config.port,
            username=config.username,
            password=config.password,
            remote_directories=config.remote_directories,
            file_patterns=config.file_patterns,
            download_path=config.download_path,
            connection_type=config.connection_type
        )
        
        # Initialize SFTP service
        sftp_service = SFTPAutomationService(sftp_config)
        
        # Execute download
        result = await sftp_service.download_files()
        
        return {
            "success": True,
            "job_id": job_id,
            "files_downloaded": result.get("files_downloaded", []),
            "download_count": result.get("download_count", 0),
            "total_size_mb": result.get("total_size_mb", 0),
            "execution_time_seconds": result.get("execution_time_seconds", 0)
        }
        
    except Exception as e:
        logger.error(f"SFTP job {job_id} failed: {str(e)}", exc_info=True)
        return {
            "success": False,
            "job_id": job_id,
            "error": str(e)
        }

async def execute_email_job(job_id: str, config: EmailConnectionConfig) -> Dict[str, Any]:
    """Execute email processing job"""
    
    try:
        # Create email configuration
        if config.provider == "gmail":
            email_config = EmailConfigFactory.create_gmail_config(
                username=config.username,
                password=config.password,
                allowed_senders=config.allowed_senders,
                download_path=config.download_path
            )
        elif config.provider == "outlook":
            email_config = EmailConfigFactory.create_outlook_config(
                username=config.username,
                password=config.password,
                allowed_senders=config.allowed_senders,
                download_path=config.download_path
            )
        else:
            raise ValueError(f"Unsupported email provider: {config.provider}")
        
        # Initialize email service
        email_service = EmailProcessingService(email_config)
        
        # Execute email processing
        result = await email_service.process_new_emails()
        
        return {
            "success": True,
            "job_id": job_id,
            "messages_processed": result.get("summary", {}).get("total_messages", 0),
            "attachments_downloaded": result.get("summary", {}).get("total_attachments", 0),
            "data_files": result.get("summary", {}).get("data_file_attachments", 0),
            "downloaded_files": result.get("downloaded_files", []),
            "processing_time_seconds": result.get("summary", {}).get("processing_time_seconds", 0)
        }
        
    except Exception as e:
        logger.error(f"Email job {job_id} failed: {str(e)}", exc_info=True)
        return {
            "success": False,
            "job_id": job_id,
            "error": str(e)
        }

# API Endpoints

@automation_router.post("/jobs/create", response_model=Dict[str, Any])
async def create_automation_job(
    job_request: AutomationJobRequest,
    background_tasks: BackgroundTasks
) -> Dict[str, Any]:
    """
    Create and schedule a new automation job
    """
    
    try:
        job_id = generate_job_id()
        
        # Validate job configuration
        if job_request.job_type == "sftp_download" and not job_request.sftp_config:
            raise HTTPException(status_code=400, detail="SFTP configuration required for SFTP download job")
        
        if job_request.job_type == "email_processing" and not job_request.email_config:
            raise HTTPException(status_code=400, detail="Email configuration required for email processing job")
        
        # Create job record
        job_record = {
            "job_id": job_id,
            "job_name": job_request.job_name,
            "job_type": job_request.job_type,
            "status": JobStatus.PENDING,
            "created_at": datetime.now(),
            "config": job_request.dict(),
            "result": None,
            "error": None
        }
        
        active_jobs[job_id] = job_record
        
        # Schedule job execution
        if job_request.job_type == "sftp_download":
            background_tasks.add_task(
                execute_and_update_job,
                job_id,
                execute_sftp_job,
                job_request.sftp_config
            )
        elif job_request.job_type == "email_processing":
            background_tasks.add_task(
                execute_and_update_job,
                job_id,
                execute_email_job,
                job_request.email_config
            )
        elif job_request.job_type == "full_workflow":
            background_tasks.add_task(
                execute_full_workflow_job,
                job_id,
                job_request
            )
        
        logger.info(f"Created automation job {job_id}: {job_request.job_name}")
        
        return {
            "success": True,
            "job_id": job_id,
            "job_name": job_request.job_name,
            "status": JobStatus.PENDING,
            "message": f"Job {job_id} created and scheduled for execution"
        }
        
    except Exception as e:
        logger.error(f"Failed to create automation job: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to create job: {str(e)}")

async def execute_and_update_job(job_id: str, executor_func, config):
    """Execute job and update status"""
    
    try:
        # Update job status to running
        if job_id in active_jobs:
            active_jobs[job_id]["status"] = JobStatus.RUNNING
            active_jobs[job_id]["started_at"] = datetime.now()
        
        # Execute the job
        result = await executor_func(job_id, config)
        
        # Update job with result
        if job_id in active_jobs:
            active_jobs[job_id]["status"] = JobStatus.COMPLETED if result.get("success") else JobStatus.FAILED
            active_jobs[job_id]["completed_at"] = datetime.now()
            active_jobs[job_id]["result"] = result
            
            if not result.get("success"):
                active_jobs[job_id]["error"] = result.get("error")
            
            # Move to history
            job_history.append(active_jobs[job_id].copy())
            
    except Exception as e:
        logger.error(f"Job execution failed for {job_id}: {str(e)}", exc_info=True)
        
        if job_id in active_jobs:
            active_jobs[job_id]["status"] = JobStatus.FAILED
            active_jobs[job_id]["completed_at"] = datetime.now()
            active_jobs[job_id]["error"] = str(e)
            
            # Move to history
            job_history.append(active_jobs[job_id].copy())

async def execute_full_workflow_job(job_id: str, job_request: AutomationJobRequest):
    """Execute complete automation workflow"""
    
    try:
        # Update job status
        if job_id in active_jobs:
            active_jobs[job_id]["status"] = JobStatus.RUNNING
            active_jobs[job_id]["started_at"] = datetime.now()
        
        # Create automation configuration
        config = AutomationConfigFactory.create_development_config()  # Base config
        
        # Customize configuration based on request
        if job_request.sftp_config:
            sftp_config = SFTPConfig(**job_request.sftp_config.dict())
            config.sftp_configs = [sftp_config]
        
        if job_request.email_config:
            if job_request.email_config.provider == "gmail":
                email_config = EmailConfigFactory.create_gmail_config(
                    username=job_request.email_config.username,
                    password=job_request.email_config.password,
                    allowed_senders=job_request.email_config.allowed_senders,
                    download_path=job_request.email_config.download_path
                )
            else:
                # Default to development config email
                email_config = config.email_config
            
            config.email_config = email_config
        
        # Create orchestrator
        orchestrator = AutomationOrchestrator(config)
        automation_orchestrators[job_id] = orchestrator
        
        # Execute workflow
        result = await orchestrator._run_daily_workflow()
        
        # Update job with result
        if job_id in active_jobs:
            active_jobs[job_id]["status"] = JobStatus.COMPLETED if result.get("success") else JobStatus.FAILED
            active_jobs[job_id]["completed_at"] = datetime.now()
            active_jobs[job_id]["result"] = result
            
            if not result.get("success"):
                active_jobs[job_id]["error"] = result.get("error")
            
            # Move to history
            job_history.append(active_jobs[job_id].copy())
        
        # Cleanup orchestrator
        if job_id in automation_orchestrators:
            del automation_orchestrators[job_id]
            
    except Exception as e:
        logger.error(f"Full workflow job {job_id} failed: {str(e)}", exc_info=True)
        
        if job_id in active_jobs:
            active_jobs[job_id]["status"] = JobStatus.FAILED
            active_jobs[job_id]["completed_at"] = datetime.now()
            active_jobs[job_id]["error"] = str(e)
            
            # Move to history
            job_history.append(active_jobs[job_id].copy())
        
        # Cleanup orchestrator
        if job_id in automation_orchestrators:
            del automation_orchestrators[job_id]

@automation_router.get("/jobs", response_model=List[Dict[str, Any]])
async def list_jobs(
    status: Optional[str] = Query(None, description="Filter by job status"),
    limit: int = Query(50, description="Maximum number of jobs to return")
) -> List[Dict[str, Any]]:
    """
    List automation jobs with optional status filter
    """
    
    try:
        # Combine active and historical jobs
        all_jobs = list(active_jobs.values()) + job_history
        
        # Filter by status if provided
        if status:
            all_jobs = [job for job in all_jobs if job.get("status") == status]
        
        # Sort by creation time (newest first)
        all_jobs.sort(key=lambda x: x.get("created_at", datetime.min), reverse=True)
        
        # Apply limit
        limited_jobs = all_jobs[:limit]
        
        # Format response
        formatted_jobs = []
        for job in limited_jobs:
            formatted_job = {
                "job_id": job.get("job_id"),
                "job_name": job.get("job_name"),
                "job_type": job.get("job_type"),
                "status": job.get("status"),
                "created_at": job.get("created_at"),
                "started_at": job.get("started_at"),
                "completed_at": job.get("completed_at"),
                "duration_seconds": (
                    (job.get("completed_at", datetime.now()) - job.get("started_at")).total_seconds()
                    if job.get("started_at") else None
                ),
                "success": job.get("result", {}).get("success") if job.get("result") else None,
                "error": job.get("error")
            }
            formatted_jobs.append(formatted_job)
        
        return formatted_jobs
        
    except Exception as e:
        logger.error(f"Failed to list jobs: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to list jobs: {str(e)}")

@automation_router.get("/jobs/{job_id}", response_model=Dict[str, Any])
async def get_job_details(job_id: str) -> Dict[str, Any]:
    """
    Get detailed information about a specific job
    """
    
    try:
        # Check active jobs first
        if job_id in active_jobs:
            job = active_jobs[job_id]
        else:
            # Search in history
            job = next((j for j in job_history if j.get("job_id") == job_id), None)
        
        if not job:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
        
        # Format detailed response
        response = {
            "job_id": job.get("job_id"),
            "job_name": job.get("job_name"),
            "job_type": job.get("job_type"),
            "status": job.get("status"),
            "created_at": job.get("created_at"),
            "started_at": job.get("started_at"),
            "completed_at": job.get("completed_at"),
            "configuration": job.get("config"),
            "result": job.get("result"),
            "error": job.get("error"),
            "logs": []  # Could add job-specific logs here
        }
        
        # Add execution time if available
        if job.get("started_at") and job.get("completed_at"):
            response["execution_time_seconds"] = (
                job["completed_at"] - job["started_at"]
            ).total_seconds()
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get job details for {job_id}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get job details: {str(e)}")

@automation_router.post("/jobs/{job_id}/cancel", response_model=Dict[str, Any])
async def cancel_job(job_id: str) -> Dict[str, Any]:
    """
    Cancel a running or pending job
    """
    
    try:
        if job_id not in active_jobs:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
        
        job = active_jobs[job_id]
        
        if job["status"] in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]:
            raise HTTPException(status_code=400, detail=f"Job {job_id} cannot be cancelled (status: {job['status']})")
        
        # Update job status
        active_jobs[job_id]["status"] = JobStatus.CANCELLED
        active_jobs[job_id]["completed_at"] = datetime.now()
        active_jobs[job_id]["error"] = "Job cancelled by user"
        
        # Stop orchestrator if running
        if job_id in automation_orchestrators:
            automation_orchestrators[job_id].stop()
            del automation_orchestrators[job_id]
        
        # Move to history
        job_history.append(active_jobs[job_id].copy())
        del active_jobs[job_id]
        
        logger.info(f"Job {job_id} cancelled successfully")
        
        return {
            "success": True,
            "job_id": job_id,
            "message": f"Job {job_id} cancelled successfully"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to cancel job {job_id}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to cancel job: {str(e)}")

@automation_router.post("/sftp/test-connection", response_model=Dict[str, Any])
async def test_sftp_connection(config: SFTPConnectionConfig) -> Dict[str, Any]:
    """
    Test SFTP connection without downloading files
    """
    
    try:
        # Create SFTP configuration
        sftp_config = SFTPConfig(
            host=config.host,
            port=config.port,
            username=config.username,
            password=config.password,
            remote_directories=config.remote_directories,
            file_patterns=config.file_patterns,
            download_path="/tmp/test_connection",  # Temporary path
            connection_type=config.connection_type
        )
        
        # Initialize SFTP service
        sftp_service = SFTPAutomationService(sftp_config)
        
        # Test connection
        connection_stats = sftp_service.get_connection_stats()
        
        return {
            "success": True,
            "message": "SFTP connection successful",
            "connection_stats": connection_stats,
            "server_info": {
                "host": config.host,
                "port": config.port,
                "connection_type": config.connection_type
            }
        }
        
    except Exception as e:
        logger.error(f"SFTP connection test failed: {str(e)}", exc_info=True)
        return {
            "success": False,
            "error": str(e),
            "message": "SFTP connection failed"
        }

@automation_router.post("/email/test-connection", response_model=Dict[str, Any])
async def test_email_connection(config: EmailConnectionConfig) -> Dict[str, Any]:
    """
    Test email connection without processing emails
    """
    
    try:
        # Create email configuration
        if config.provider == "gmail":
            email_config = EmailConfigFactory.create_gmail_config(
                username=config.username,
                password=config.password,
                allowed_senders=config.allowed_senders,
                download_path="/tmp/test_email"
            )
        elif config.provider == "outlook":
            email_config = EmailConfigFactory.create_outlook_config(
                username=config.username,
                password=config.password,
                allowed_senders=config.allowed_senders,
                download_path="/tmp/test_email"
            )
        else:
            raise ValueError(f"Unsupported email provider: {config.provider}")
        
        # Initialize email service
        email_service = EmailProcessingService(email_config)
        
        # Test connection (basic statistics check)
        stats = email_service.get_processing_statistics()
        
        return {
            "success": True,
            "message": "Email connection successful",
            "provider": config.provider,
            "statistics": stats,
            "connection_info": {
                "host": config.host,
                "port": config.port,
                "username": config.username
            }
        }
        
    except Exception as e:
        logger.error(f"Email connection test failed: {str(e)}", exc_info=True)
        return {
            "success": False,
            "error": str(e),
            "message": "Email connection failed"
        }

@automation_router.get("/stats", response_model=Dict[str, Any])
async def get_automation_stats() -> Dict[str, Any]:
    """
    Get automation system statistics
    """
    
    try:
        # Calculate job statistics
        total_jobs = len(active_jobs) + len(job_history)
        
        job_status_counts = {}
        for status in JobStatus:
            job_status_counts[status.value] = 0
        
        # Count active job statuses
        for job in active_jobs.values():
            status = job.get("status", "unknown")
            if status in job_status_counts:
                job_status_counts[status] += 1
        
        # Count historical job statuses
        for job in job_history:
            status = job.get("status", "unknown")
            if status in job_status_counts:
                job_status_counts[status] += 1
        
        # Calculate success rate
        completed_jobs = job_status_counts.get("completed", 0)
        failed_jobs = job_status_counts.get("failed", 0)
        total_finished = completed_jobs + failed_jobs
        
        success_rate = (completed_jobs / total_finished * 100) if total_finished > 0 else 0
        
        # Get recent activity (last 24 hours)
        recent_cutoff = datetime.now() - timedelta(hours=24)
        recent_jobs = [
            job for job in job_history 
            if job.get("created_at", datetime.min) > recent_cutoff
        ]
        
        return {
            "total_jobs": total_jobs,
            "active_jobs": len(active_jobs),
            "completed_jobs": len(job_history),
            "job_status_counts": job_status_counts,
            "success_rate_percent": round(success_rate, 2),
            "recent_activity_24h": len(recent_jobs),
            "active_orchestrators": len(automation_orchestrators),
            "system_status": "healthy" if len(active_jobs) < 10 else "busy",  # Simple health check
            "last_updated": datetime.now()
        }
        
    except Exception as e:
        logger.error(f"Failed to get automation stats: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get stats: {str(e)}")

@automation_router.delete("/jobs/cleanup", response_model=Dict[str, Any])
async def cleanup_job_history(
    older_than_days: int = Query(30, description="Delete jobs older than this many days")
) -> Dict[str, Any]:
    """
    Clean up old job history records
    """
    
    try:
        global job_history
        cutoff_date = datetime.now() - timedelta(days=older_than_days)
        
        initial_count = len(job_history)
        
        # Filter out old jobs
        job_history = [
            job for job in job_history 
            if job.get("created_at", datetime.max) > cutoff_date
        ]
        
        deleted_count = initial_count - len(job_history)
        
        logger.info(f"Cleaned up {deleted_count} job history records older than {older_than_days} days")
        
        return {
            "success": True,
            "deleted_count": deleted_count,
            "remaining_count": len(job_history),
            "message": f"Deleted {deleted_count} job records older than {older_than_days} days"
        }
        
    except Exception as e:
        logger.error(f"Failed to cleanup job history: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to cleanup: {str(e)}")

# Health check endpoint
@automation_router.get("/health", response_model=Dict[str, Any])
async def automation_health_check() -> Dict[str, Any]:
    """
    Check the health of automation services
    """
    
    return {
        "status": "healthy",
        "timestamp": datetime.now(),
        "services": {
            "automation_api": "active",
            "job_scheduler": "active",
            "active_jobs": len(active_jobs),
            "job_history_size": len(job_history)
        },
        "version": "1.0.0"
    }