from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from contextlib import asynccontextmanager
import time
import logging
from app.routes import router
from app.logging_config import setup_logging, request_logger
from app.core.config import config
from app.core.environment import get_environment

# Setup logging with environment-aware level
setup_logging(config.log_level)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    env = get_environment()
    logging.info(f"Financial Reconciliation Platform API starting up...")
    logging.info(f"Environment: {env.value.upper()}")
    if config.enable_docs:
        logging.info("API Documentation available at /docs")
    
    # Initialize automation system if available
    try:
        from app.automation.job_manager import startup_job_manager
        await startup_job_manager()
        logging.info("✅ Automation system initialized")
    except Exception as e:
        logging.warning(f"⚠️ Automation system initialization failed: {e}")
    
    yield
    
    # Shutdown
    logging.info("Smart Column Mapper API shutting down...")
    
    # Shutdown automation system if available
    try:
        from app.automation.job_manager import shutdown_job_manager
        await shutdown_job_manager()
        logging.info("Automation system stopped")
    except Exception as e:
        logging.warning(f"Automation system shutdown failed: {e}")

# Determine docs URLs based on environment
docs_url = "/docs" if config.enable_docs else None
redoc_url = "/redoc" if config.enable_docs else None

app = FastAPI(
    title="Financial Reconciliation Platform API",
    description="AI-powered financial reconciliation and intelligent column mapping service for restaurant operations",
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
        # Disable buffering for upload endpoints to allow streaming
        if request.url.path.startswith(("/devyani-service/api/upload", "/api/uploader/upload")):
            response = await call_next(request)
            response.headers["X-Accel-Buffering"] = "no"  # Disable nginx buffering
            return response
        return await call_next(request)

app.add_middleware(UploadTimeoutMiddleware)

# Request timing middleware
@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.time()
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
app.include_router(router, prefix="/api", tags=["Smart Uploader"])

# Include devyani-style upload routes
try:
    from app.routes_devyani import router as devyani_router, alias_router
    app.include_router(devyani_router)
    app.include_router(alias_router)  # Add alias routes for frontend compatibility
    logging.info("✅ Devyani-style upload endpoints loaded successfully")
except ImportError as e:
    logging.warning(f"⚠️ Devyani-style upload endpoints not available: {e}")

# Include automation routers
try:
    from app.automation.routes import automation_router
    from app.automation.job_routes import jobs_router
    
    app.include_router(automation_router, tags=["Automation"])
    app.include_router(jobs_router, tags=["Scheduled Jobs"])
    
    logging.info("✅ Automation endpoints loaded successfully")
except ImportError as e:
    logging.warning(f"⚠️ Automation endpoints not available: {e}")

# Include financial reconciliation routers
try:
    from app.financial_routes import financial_router
    
    app.include_router(financial_router, prefix="/api", tags=["Financial Reconciliation"])
    
    logging.info("✅ Financial reconciliation endpoints loaded successfully")
except ImportError as e:
    logging.warning(f"⚠️ Financial reconciliation endpoints not available: {e}")

# Lifespan events handled above in the lifespan function

