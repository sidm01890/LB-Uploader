"""
Gunicorn Configuration for Production Deployment
Use this for production deployments with gunicorn
"""
import multiprocessing
import os
from app.core.environment import get_environment
from app.core.properties_loader import get_active_profile

# Environment and Profile
env = get_environment()
profile = get_active_profile()

# Server socket
bind = f"0.0.0.0:{os.getenv('UVICORN_PORT', '8010')}"
backlog = 2048

# Worker processes
workers = int(os.getenv('GUNICORN_WORKERS', multiprocessing.cpu_count() * 2 + 1))
worker_class = "uvicorn.workers.UvicornWorker"
worker_connections = 1000
timeout = 30
keepalive = 2

# Logging
accesslog = f"logs/gunicorn_access_{profile}.log"
errorlog = f"logs/gunicorn_error_{profile}.log"
loglevel = "info" if env.is_production else "debug"
access_log_format = '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s" %(D)s'

# Process naming
proc_name = f"financial_reconciliation_api_{profile}"

# Server mechanics
daemon = False
pidfile = f"gunicorn_{env.value}.pid"
umask = 0
user = None
group = None
tmp_upload_dir = None

# SSL (if needed)
keyfile = os.getenv('SSL_KEYFILE', None)
certfile = os.getenv('SSL_CERTFILE', None)

# Performance tuning
max_requests = 1000
max_requests_jitter = 50
preload_app = True

# Graceful timeout
graceful_timeout = 30

def when_ready(server):
    """Called just after the server is started"""
    server.log.info(f"Server is ready. Spawning {workers} workers for {profile} profile ({env.value} environment)")

def on_exit(server):
    """Called just before exiting"""
    server.log.info(f"Shutting down: {proc_name}")

