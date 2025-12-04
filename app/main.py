from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from contextlib import asynccontextmanager
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
from app.controllers.scheduled_jobs_controller import ScheduledJobsController

# Setup logging with environment-aware level
setup_logging(config.log_level)

# Initialize scheduler and controller
scheduler = AsyncIOScheduler()
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

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    env = get_environment()
    logging.info(f"File Upload Service API starting up...")
    logging.info(f"Environment: {env.value.upper()}")
    if config.enable_docs:
        logging.info("API Documentation available at /docs")
    
    # Start scheduled job processing
    # Get interval from config or default to 5 minutes
    # You can configure this via environment variable: SCHEDULED_JOB_INTERVAL_MINUTES
    job_interval_minutes = int(os.getenv("SCHEDULED_JOB_INTERVAL_MINUTES", "5"))
    
    # Add scheduled job to run at specified interval
    scheduler.add_job(
        run_scheduled_processing_job,
        trigger=IntervalTrigger(minutes=job_interval_minutes),
        id="collection_data_processing",
        name="Collection Data Processing Job",
        replace_existing=True
    )
    
    # Start the scheduler
    # scheduler.start()
    logging.info(f"✅ Scheduled job processing started - running every {job_interval_minutes} minute(s)")
    
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

