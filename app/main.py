from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
import time
import logging
import os
from app.routes import router
from app.logging_config import setup_logging, request_logger
from app.core.config import config
from app.core.environment import get_environment

# Import scheduler and controller for scheduled jobs
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.executors.asyncio import AsyncIOExecutor
from app.controllers.scheduled_jobs_controller import ScheduledJobsController

# Setup logging with environment-aware level
setup_logging(config.log_level)

# Initialize scheduler with proper configuration
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

scheduler = AsyncIOScheduler(
    jobstores=jobstores,
    executors=executors,
    job_defaults=job_defaults,
    timezone=timezone.utc  # Use UTC timezone for consistency
)
scheduled_jobs_controller = ScheduledJobsController()

async def run_scheduled_processing_job():
    """
    Scheduled function that runs the collection data processing job
    This function processes all collections based on field mappings
    """
    try:
        logging.info("🔄 Starting scheduled collection data processing job...")
        result = await scheduled_jobs_controller.process_collection_data()
        
        if result.get("status") == 200:
            data = result.get("data", {})
            collections_processed = data.get("collections_processed", 0)
            total_documents = data.get("total_documents_processed", 0)
            logging.info(
                f"✅ Scheduled job completed: {collections_processed} collection(s) processed, "
                f"{total_documents} document(s) processed"
            )
        else:
            logging.warning(f"⚠️ Scheduled job completed with status: {result.get('status')}")
            
    except Exception as e:
        logging.error(f"❌ Error in scheduled processing job: {e}", exc_info=True)

async def run_scheduled_formula_calculation_job():
    """
    Scheduled function that runs the formula calculation job
    This function processes all reports based on formulas collection
    Processes large datasets (800K+ records) in memory-efficient batches
    """
    import gc
    import time
    
    start_time = time.time()
    
    try:
        logging.info("🔄 Starting scheduled formula calculation job...")
        logging.info("💾 Memory-efficient batch processing enabled for large datasets")
        
        # Check MongoDB connection before processing
        from app.services.mongodb_service import mongodb_service
        if not mongodb_service.is_connected():
            logging.error("❌ MongoDB is not connected. Skipping formula calculation job.")
            logging.warning("⚠️ Please check MongoDB connection and network settings.")
            return
        
        # Force garbage collection before starting to free any unused memory
        gc.collect()
        
        result = await scheduled_jobs_controller.process_formula_calculations()
        
        # Force garbage collection after processing to free memory
        gc.collect()
        
        elapsed_time = time.time() - start_time
        
        if result.get("status") == 200:
            data = result.get("data", {})
            reports_processed = data.get("reports_processed", 0)
            total_documents = data.get("total_documents_processed", 0)
            logging.info(
                f"✅ Formula calculation job completed: {reports_processed} report(s) processed, "
                f"{total_documents:,} document(s) processed in {elapsed_time:.2f} seconds"
            )
        else:
            logging.warning(
                f"⚠️ Formula calculation job completed with status: {result.get('status')} "
                f"in {elapsed_time:.2f} seconds"
            )
            
    except ConnectionError as e:
        logging.error(f"❌ Network/Connection error in scheduled formula calculation job: {e}", exc_info=True)
        logging.warning("⚠️ This might be a network issue. Check MongoDB connectivity.")
    except Exception as e:
        logging.error(f"❌ Error in scheduled formula calculation job: {e}", exc_info=True)
    finally:
        # Final memory cleanup
        gc.collect()
        logging.debug("🧹 Final memory cleanup completed after formula calculation job")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    env = get_environment()
    logging.info(f"File Upload Service API starting up...")
    logging.info(f"Environment: {env.value.upper()}")
    if config.enable_docs:
        logging.info("API Documentation available at /docs")
    
    # Start scheduled job processing
    # Formula calculation schedule:
    # - First run: 2 minutes after startup (configurable via FORMULA_JOB_FIRST_RUN_DELAY_MINUTES)
    # - Subsequent runs: every 2 hours (configurable via FORMULA_JOB_INTERVAL_HOURS)
    first_run_delay_minutes = int(os.getenv("FORMULA_JOB_FIRST_RUN_DELAY_MINUTES", "1"))
    formula_job_interval_hours = float(os.getenv("FORMULA_JOB_INTERVAL_HOURS", "2"))
    
    # Start the scheduler FIRST (before adding jobs)
    try:
        if not scheduler.running:
            scheduler.start()
            logging.info("✅ Scheduler started successfully")
        else:
            logging.warning("⚠️ Scheduler is already running")
    except Exception as e:
        logging.error(f"❌ Failed to start scheduler: {e}", exc_info=True)
        raise
    
    # Use timezone-aware datetime for scheduler
    current_time = datetime.now(timezone.utc)
    first_run_time = current_time + timedelta(minutes=first_run_delay_minutes)
    
    # Ensure first_run_time is at least a few seconds in the future
    if first_run_time <= current_time:
        first_run_time = current_time + timedelta(seconds=10)
        logging.warning(f"⚠️ Adjusted first_run_time to be in the future: {first_run_time}")
    
    logging.info(f"Current time (UTC): {current_time}")
    logging.info(f"First run time (UTC): {first_run_time}")
    logging.info(f"Delay configured: {first_run_delay_minutes} minute(s)")
    logging.info(f"Time until first run: {(first_run_time - current_time).total_seconds()} seconds")
    
    try:
        # Remove existing job if it exists (extra safety)
        try:
            scheduler.remove_job("formula_calculation")
            logging.info("Removed existing formula_calculation job")
        except Exception:
            pass  # Job doesn't exist, that's fine
        
        scheduler.add_job(
            run_scheduled_formula_calculation_job,
            trigger=IntervalTrigger(hours=formula_job_interval_hours, start_date=first_run_time),
            id="formula_calculation",
            name="Formula Calculation Job",
            replace_existing=True
        )
        logging.info("✅ Formula calculation job added to scheduler")
    except Exception as e:
        logging.error(f"❌ Failed to add formula calculation job: {e}", exc_info=True)
        raise
    
    # Verify scheduler is running and log job details
    if scheduler.running:
        logging.info("✅ Scheduled job processing started:")
        logging.info(f"   - Formula Calculation: first run in {first_run_delay_minutes} minute(s), then every {formula_job_interval_hours} hour(s)")
        logging.info("   - Collection Data Processing: runs immediately after file upload")
        # Log scheduled jobs with detailed info
        jobs = scheduler.get_jobs()
        logging.info(f"   - Total scheduled jobs: {len(jobs)}")
        for job in jobs:
            next_run = job.next_run_time
            if next_run:
                time_until_run = (next_run - datetime.now(timezone.utc)).total_seconds()
                logging.info(f"      * {job.name} (id: {job.id})")
                logging.info(f"        Next run: {next_run} (in {time_until_run:.1f} seconds / {time_until_run/60:.1f} minutes)")
            else:
                logging.info(f"      * {job.name} (id: {job.id}, next run: None)")
    else:
        logging.error("❌ Scheduler is not running after startup")
    
    yield
    
    # Shutdown
    logging.info("File Upload Service API shutting down...")
    
    # Shutdown scheduler
    if scheduler.running:
        scheduler.shutdown()
        logging.info("✅ Scheduler stopped")

# Determine docs URLs based on environment
docs_url = "/docs" if config.enable_docs else None
redoc_url = "/redoc" if config.enable_docs else None

app = FastAPI(
    title="File Upload Service API",
    description="Service for uploading Excel/CSV files and storing them on the server for further processing",
    version="2.0.0",
    docs_url=docs_url,
    redoc_url=redoc_url,
    lifespan=lifespan,
    debug=config.debug
)

# CORS middleware with environment-aware origins
# NOTE: CORS is handled by nginx for staging/production, FastAPI CORS is for dev only
cors_origins = config.cors_allowed_origins.split(",") if config.cors_allowed_origins else ["*"]
if "*" in cors_origins and get_environment().is_production:
    logging.warning("⚠️  CORS is set to '*' in production! This is a security risk.")

# Only add CORS middleware if not in staging/production (nginx handles it there)
if get_environment().is_development:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_origins,
        allow_methods=["*"],
        allow_headers=["*"],
        allow_credentials=False,  # Set to False to avoid conflicts with nginx
    )
else:
    logging.info("🌐 CORS handled by nginx reverse proxy - FastAPI CORS middleware disabled")

# Timeout middleware for large file uploads (disables buffering for streaming)
from starlette.middleware.base import BaseHTTPMiddleware

class UploadTimeoutMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        try:
            # Disable buffering for upload endpoints to allow streaming
            if request.url.path.startswith("/api/upload"):
                response = await call_next(request)
                response.headers["X-Accel-Buffering"] = "no"  # Disable nginx buffering
                return response
            return await call_next(request)
        except Exception as e:
            logging.error(f"❌ Error in UploadTimeoutMiddleware: {e}", exc_info=True)
            raise

app.add_middleware(UploadTimeoutMiddleware)

# Request timing middleware
@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.time()
    try:
        response = await call_next(request)
        process_time = time.time() - start_time
        response.headers["X-Process-Time"] = str(process_time)
        
        # Log request/response
        request_logger.log_response(
            endpoint=request.url.path,
            status_code=response.status_code,
            response_time=process_time,
            method=request.method
        )
        
        return response
    except Exception as e:
        process_time = time.time() - start_time
        logging.error(f"❌ Error processing request {request.url.path}: {e}", exc_info=True)
        raise

# Global exception handlers
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    request_logger.log_error(
        endpoint=request.url.path,
        error=exc,
        status_code=exc.status_code
    )
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.detail,
            "status_code": exc.status_code,
            "endpoint": request.url.path
        }
    )

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    request_logger.log_error(
        endpoint=request.url.path,
        error=exc,
        validation_errors=exc.errors()
    )
    return JSONResponse(
        status_code=422,
        content={
            "error": "Validation error",
            "details": exc.errors(),
            "endpoint": request.url.path
        }
    )

@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    request_logger.log_error(
        endpoint=request.url.path,
        error=exc
    )
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "message": "An unexpected error occurred",
            "endpoint": request.url.path
        }
    )

# Include routers
# All routes are now in routes.py
app.include_router(router, prefix="/api", tags=["File Upload"])

# Lifespan events handled above in the lifespan function

