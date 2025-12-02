# Uploader Dockerfile for LaughingBuddha Project
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    default-libmysqlclient-dev \
    pkg-config \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Note: Java/PySpark dependencies removed for now - install separately if needed

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
# Note: Some packages like PySpark may take time to install
RUN pip install --no-cache-dir -r requirements.txt || \
    (echo "Some optional packages failed to install, continuing..." && \
     pip install --no-cache-dir fastapi uvicorn python-multipart python-dotenv pydantic-settings pyyaml sqlalchemy aiomysql pymysql mysql-connector-python pandas openpyxl)

# Copy application code
COPY . .

# Create necessary directories
RUN mkdir -p /app/data/uploaded_files \
    /app/data/processed \
    /app/data/failed \
    /app/data/temp \
    /app/logs \
    /app/reports

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV SPRING_PROFILES_ACTIVE=stage
ENV APP_ENV=staging
ENV APP_PROFILE=stage

# Expose port
EXPOSE 8010

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD python -c "import requests; requests.get('http://localhost:8010/api/health')" || exit 1

# Run the application
CMD ["python", "-m", "app.cli.run", "--profile", "stage", "--host", "0.0.0.0", "--port", "8010", "--no-reload"]

