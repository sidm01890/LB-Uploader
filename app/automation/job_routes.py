"""
Scheduled Job Routes for Smart Uploader Platform
REST API endpoints for managing scheduled jobs
"""

from fastapi import APIRouter, HTTPException, Query, Depends
from fastapi.responses import JSONResponse
from typing import Dict, List, Any, Optional
from datetime import datetime
from pydantic import BaseModel, Field
import logging

# Import job management
from app.automation.job_manager import (
    ScheduledJobManager, 
    ScheduledJobConfig,
    JobType,
    JobFrequency,
    job_manager
)
from app.automation.routes import (
    SFTPConnectionConfig,
    EmailConnectionConfig, 
    NotificationRecipientConfig
)

logger = logging.getLogger(__name__)

# Create router
jobs_router = APIRouter(prefix="/api/v1/jobs", tags=["Scheduled Jobs"])

# Pydantic models for scheduled jobs
class ScheduleConfig(BaseModel):
    frequency: str = Field(..., description="Job frequency: once, minutely, hourly, daily, weekly, monthly, custom_cron")
    schedule_params: Dict[str, Any] = Field(..., description="Schedule parameters based on frequency")

class CreateScheduledJobRequest(BaseModel):
    job_name: str = Field(..., description="Human-readable job name")
    job_type: str = Field(..., description="Job type: sftp_download, email_processing, full_workflow")
    schedule_config: ScheduleConfig = Field(..., description="Schedule configuration")
    job_config: Dict[str, Any] = Field(..., description="Job-specific configuration")
    enabled: bool = Field(True, description="Whether job is enabled")
    max_retries: int = Field(3, description="Maximum retry attempts")
    timeout_minutes: int = Field(60, description="Job timeout in minutes")
    notification_on_success: bool = Field(False, description="Send notification on success")
    notification_on_failure: bool = Field(True, description="Send notification on failure")

class UpdateScheduledJobRequest(BaseModel):
    job_name: Optional[str] = None
    schedule_config: Optional[ScheduleConfig] = None
    job_config: Optional[Dict[str, Any]] = None
    enabled: Optional[bool] = None
    max_retries: Optional[int] = None
    timeout_minutes: Optional[int] = None
    notification_on_success: Optional[bool] = None
    notification_on_failure: Optional[bool] = None

# Startup/shutdown handlers for job manager
async def startup_job_manager():
    """Initialize and start the job manager"""
    
    try:
        await job_manager.start_scheduler()
        logger.info("✅ Job manager started successfully")
    except Exception as e:
        logger.error(f"❌ Failed to start job manager: {str(e)}", exc_info=True)

async def shutdown_job_manager():
    """Stop the job manager"""
    
    try:
        await job_manager.stop_scheduler()
        logger.info("Job manager stopped")
    except Exception as e:
        logger.error(f"Error stopping job manager: {str(e)}")

# API Endpoints

@jobs_router.post("/scheduled", response_model=Dict[str, Any])
async def create_scheduled_job(request: CreateScheduledJobRequest) -> Dict[str, Any]:
    """
    Create a new scheduled job
    """
    
    try:
        # Generate unique job ID
        import uuid
        job_id = f"job_{uuid.uuid4().hex[:12]}"
        
        # Validate job type
        try:
            job_type = JobType(request.job_type)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid job type: {request.job_type}")
        
        # Validate frequency
        try:
            frequency = JobFrequency(request.schedule_config.frequency)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid frequency: {request.schedule_config.frequency}")
        
        # Create scheduled job configuration
        job_config = ScheduledJobConfig(
            job_id=job_id,
            job_name=request.job_name,
            job_type=job_type,
            frequency=frequency,
            schedule_params=request.schedule_config.schedule_params,
            job_config=request.job_config,
            enabled=request.enabled,
            max_retries=request.max_retries,
            timeout_minutes=request.timeout_minutes,
            notification_on_success=request.notification_on_success,
            notification_on_failure=request.notification_on_failure
        )
        
        # Add to scheduler
        await job_manager.add_scheduled_job(job_config)
        
        logger.info(f"Created scheduled job: {request.job_name} ({job_id})")
        
        return {
            "success": True,
            "job_id": job_id,
            "job_name": request.job_name,
            "job_type": request.job_type,
            "frequency": request.schedule_config.frequency,
            "enabled": request.enabled,
            "message": f"Scheduled job '{request.job_name}' created successfully"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create scheduled job: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to create scheduled job: {str(e)}")

@jobs_router.get("/scheduled", response_model=List[Dict[str, Any]])
async def list_scheduled_jobs() -> List[Dict[str, Any]]:
    """
    List all scheduled jobs
    """
    
    try:
        jobs = job_manager.get_scheduled_jobs()
        
        # Format response
        formatted_jobs = []
        for job in jobs:
            formatted_job = {
                "job_id": job["job_id"],
                "job_name": job["job_name"], 
                "job_type": job["job_type"],
                "frequency": job["frequency"],
                "enabled": job["enabled"],
                "next_run": job["next_run"].isoformat() if job["next_run"] else None,
                "max_retries": job["max_retries"],
                "notification_on_success": job["notification_on_success"],
                "notification_on_failure": job["notification_on_failure"]
            }
            formatted_jobs.append(formatted_job)
        
        return formatted_jobs
        
    except Exception as e:
        logger.error(f"Failed to list scheduled jobs: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to list jobs: {str(e)}")

@jobs_router.get("/scheduled/{job_id}", response_model=Dict[str, Any])
async def get_scheduled_job(job_id: str) -> Dict[str, Any]:
    """
    Get details of a specific scheduled job
    """
    
    try:
        # Get job from manager
        if job_id not in job_manager.scheduled_jobs:
            raise HTTPException(status_code=404, detail=f"Scheduled job {job_id} not found")
        
        job_config = job_manager.scheduled_jobs[job_id]
        
        # Get scheduler job info
        scheduler_job = job_manager.scheduler.get_job(job_id)
        
        # Get recent execution history
        recent_executions = job_manager.get_job_execution_history(job_id, limit=10)
        
        return {
            "job_id": job_config.job_id,
            "job_name": job_config.job_name,
            "job_type": job_config.job_type.value,
            "frequency": job_config.frequency.value,
            "schedule_params": job_config.schedule_params,
            "job_config": job_config.job_config,
            "enabled": job_config.enabled,
            "max_retries": job_config.max_retries,
            "timeout_minutes": job_config.timeout_minutes,
            "notification_on_success": job_config.notification_on_success,
            "notification_on_failure": job_config.notification_on_failure,
            "next_run": scheduler_job.next_run_time.isoformat() if scheduler_job and scheduler_job.next_run_time else None,
            "recent_executions": recent_executions
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get scheduled job {job_id}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get job: {str(e)}")

@jobs_router.put("/scheduled/{job_id}", response_model=Dict[str, Any])
async def update_scheduled_job(job_id: str, request: UpdateScheduledJobRequest) -> Dict[str, Any]:
    """
    Update a scheduled job
    """
    
    try:
        # Check if job exists
        if job_id not in job_manager.scheduled_jobs:
            raise HTTPException(status_code=404, detail=f"Scheduled job {job_id} not found")
        
        job_config = job_manager.scheduled_jobs[job_id]
        
        # Update job configuration
        if request.job_name is not None:
            job_config.job_name = request.job_name
        
        if request.schedule_config is not None:
            try:
                job_config.frequency = JobFrequency(request.schedule_config.frequency)
                job_config.schedule_params = request.schedule_config.schedule_params
            except ValueError:
                raise HTTPException(status_code=400, detail=f"Invalid frequency: {request.schedule_config.frequency}")
        
        if request.job_config is not None:
            job_config.job_config = request.job_config
        
        if request.enabled is not None:
            job_config.enabled = request.enabled
        
        if request.max_retries is not None:
            job_config.max_retries = request.max_retries
        
        if request.timeout_minutes is not None:
            job_config.timeout_minutes = request.timeout_minutes
        
        if request.notification_on_success is not None:
            job_config.notification_on_success = request.notification_on_success
        
        if request.notification_on_failure is not None:
            job_config.notification_on_failure = request.notification_on_failure
        
        # Re-add job to scheduler with updated configuration
        await job_manager.add_scheduled_job(job_config)
        
        logger.info(f"Updated scheduled job: {job_config.job_name} ({job_id})")
        
        return {
            "success": True,
            "job_id": job_id,
            "message": f"Scheduled job '{job_config.job_name}' updated successfully"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to update scheduled job {job_id}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to update job: {str(e)}")

@jobs_router.delete("/scheduled/{job_id}", response_model=Dict[str, Any])
async def delete_scheduled_job(job_id: str) -> Dict[str, Any]:
    """
    Delete a scheduled job
    """
    
    try:
        # Check if job exists
        if job_id not in job_manager.scheduled_jobs:
            raise HTTPException(status_code=404, detail=f"Scheduled job {job_id} not found")
        
        job_name = job_manager.scheduled_jobs[job_id].job_name
        
        # Remove from scheduler
        success = await job_manager.remove_scheduled_job(job_id)
        
        if not success:
            raise HTTPException(status_code=500, detail=f"Failed to remove job from scheduler")
        
        logger.info(f"Deleted scheduled job: {job_name} ({job_id})")
        
        return {
            "success": True,
            "job_id": job_id,
            "message": f"Scheduled job '{job_name}' deleted successfully"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete scheduled job {job_id}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to delete job: {str(e)}")

@jobs_router.post("/scheduled/{job_id}/pause", response_model=Dict[str, Any])
async def pause_scheduled_job(job_id: str) -> Dict[str, Any]:
    """
    Pause a scheduled job
    """
    
    try:
        # Check if job exists
        if job_id not in job_manager.scheduled_jobs:
            raise HTTPException(status_code=404, detail=f"Scheduled job {job_id} not found")
        
        # Pause in scheduler
        job_manager.scheduler.pause_job(job_id)
        
        # Update configuration
        job_manager.scheduled_jobs[job_id].enabled = False
        
        job_name = job_manager.scheduled_jobs[job_id].job_name
        
        logger.info(f"Paused scheduled job: {job_name} ({job_id})")
        
        return {
            "success": True,
            "job_id": job_id,
            "message": f"Scheduled job '{job_name}' paused successfully"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to pause scheduled job {job_id}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to pause job: {str(e)}")

@jobs_router.post("/scheduled/{job_id}/resume", response_model=Dict[str, Any])
async def resume_scheduled_job(job_id: str) -> Dict[str, Any]:
    """
    Resume a paused scheduled job
    """
    
    try:
        # Check if job exists
        if job_id not in job_manager.scheduled_jobs:
            raise HTTPException(status_code=404, detail=f"Scheduled job {job_id} not found")
        
        # Resume in scheduler
        job_manager.scheduler.resume_job(job_id)
        
        # Update configuration
        job_manager.scheduled_jobs[job_id].enabled = True
        
        job_name = job_manager.scheduled_jobs[job_id].job_name
        
        logger.info(f"Resumed scheduled job: {job_name} ({job_id})")
        
        return {
            "success": True,
            "job_id": job_id,
            "message": f"Scheduled job '{job_name}' resumed successfully"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to resume scheduled job {job_id}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to resume job: {str(e)}")

@jobs_router.post("/scheduled/{job_id}/run", response_model=Dict[str, Any])
async def run_scheduled_job_now(job_id: str) -> Dict[str, Any]:
    """
    Trigger a scheduled job to run immediately
    """
    
    try:
        # Check if job exists
        if job_id not in job_manager.scheduled_jobs:
            raise HTTPException(status_code=404, detail=f"Scheduled job {job_id} not found")
        
        # Trigger job execution
        job_manager.scheduler.modify_job(job_id, next_run_time=datetime.now())
        
        job_name = job_manager.scheduled_jobs[job_id].job_name
        
        logger.info(f"Triggered immediate execution of scheduled job: {job_name} ({job_id})")
        
        return {
            "success": True,
            "job_id": job_id,
            "message": f"Scheduled job '{job_name}' triggered for immediate execution"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to run scheduled job {job_id}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to run job: {str(e)}")

@jobs_router.get("/executions", response_model=List[Dict[str, Any]])
async def list_job_executions(
    job_id: Optional[str] = Query(None, description="Filter by job ID"),
    limit: int = Query(100, description="Maximum number of executions to return")
) -> List[Dict[str, Any]]:
    """
    List job execution history
    """
    
    try:
        executions = job_manager.get_job_execution_history(job_id=job_id, limit=limit)
        
        # Format response
        formatted_executions = []
        for execution in executions:
            formatted_execution = {
                "execution_id": execution.get("execution_id"),
                "job_id": execution.get("job_id"),
                "job_name": execution.get("job_name"),
                "job_type": execution.get("job_type"),
                "status": execution.get("status"),
                "started_at": execution.get("started_at").isoformat() if execution.get("started_at") else None,
                "completed_at": execution.get("completed_at").isoformat() if execution.get("completed_at") else None,
                "execution_time_seconds": execution.get("execution_time_seconds"),
                "retry_count": execution.get("retry_count", 0),
                "success": execution.get("result", {}).get("success") if execution.get("result") else None,
                "error": execution.get("error")
            }
            formatted_executions.append(formatted_execution)
        
        return formatted_executions
        
    except Exception as e:
        logger.error(f"Failed to list job executions: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to list executions: {str(e)}")

@jobs_router.get("/executions/{execution_id}", response_model=Dict[str, Any])
async def get_job_execution_details(execution_id: str) -> Dict[str, Any]:
    """
    Get detailed information about a specific job execution
    """
    
    try:
        # Find execution in history
        execution = None
        for record in job_manager.job_execution_history:
            if record.get("execution_id") == execution_id:
                execution = record
                break
        
        if not execution:
            raise HTTPException(status_code=404, detail=f"Job execution {execution_id} not found")
        
        return {
            "execution_id": execution.get("execution_id"),
            "job_id": execution.get("job_id"),
            "job_name": execution.get("job_name"),
            "job_type": execution.get("job_type"),
            "status": execution.get("status"),
            "started_at": execution.get("started_at").isoformat() if execution.get("started_at") else None,
            "completed_at": execution.get("completed_at").isoformat() if execution.get("completed_at") else None,
            "execution_time_seconds": execution.get("execution_time_seconds"),
            "retry_count": execution.get("retry_count", 0),
            "result": execution.get("result"),
            "error": execution.get("error")
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get job execution {execution_id}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get execution: {str(e)}")

@jobs_router.get("/stats", response_model=Dict[str, Any])
async def get_job_stats() -> Dict[str, Any]:
    """
    Get job scheduling and execution statistics
    """
    
    try:
        # Get basic stats
        scheduled_jobs = job_manager.get_scheduled_jobs()
        all_executions = job_manager.get_job_execution_history(limit=1000)
        
        # Calculate statistics
        total_scheduled = len(scheduled_jobs)
        enabled_jobs = len([job for job in scheduled_jobs if job["enabled"]])
        
        # Execution statistics
        total_executions = len(all_executions)
        
        status_counts = {}
        for execution in all_executions:
            status = execution.get("status", "unknown")
            status_counts[status] = status_counts.get(status, 0) + 1
        
        # Success rate
        completed = status_counts.get("completed", 0)
        failed = status_counts.get("failed", 0)
        total_finished = completed + failed
        
        success_rate = (completed / total_finished * 100) if total_finished > 0 else 0
        
        # Recent activity (last 24 hours)
        from datetime import timedelta
        recent_cutoff = datetime.now() - timedelta(hours=24)
        recent_executions = [
            exec_record for exec_record in all_executions
            if exec_record.get("started_at", datetime.min) > recent_cutoff
        ]
        
        return {
            "scheduler_status": "running" if job_manager.scheduler.running else "stopped",
            "scheduled_jobs": {
                "total": total_scheduled,
                "enabled": enabled_jobs,
                "disabled": total_scheduled - enabled_jobs
            },
            "executions": {
                "total": total_executions,
                "status_breakdown": status_counts,
                "success_rate_percent": round(success_rate, 2),
                "recent_24h": len(recent_executions)
            },
            "system_health": {
                "scheduler_running": job_manager.scheduler.running,
                "active_jobs_count": len(job_manager.scheduler.get_jobs()),
                "execution_history_size": len(job_manager.job_execution_history),
                "service_cache_size": len(job_manager.service_cache)
            },
            "last_updated": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to get job stats: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get stats: {str(e)}")

# Health check
@jobs_router.get("/health", response_model=Dict[str, Any])
async def jobs_health_check() -> Dict[str, Any]:
    """
    Check the health of job scheduling system
    """
    
    return {
        "status": "healthy",
        "scheduler_running": job_manager.scheduler.running,
        "active_jobs": len(job_manager.scheduler.get_jobs()),
        "timestamp": datetime.now().isoformat(),
        "version": "1.0.0"
    }