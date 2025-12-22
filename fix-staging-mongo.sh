#!/bin/bash
# Script to fix MongoDB configuration for staging on server
# Run this on the server: ssh ubuntu@65.0.236.144

set -e

PROJECT_DIR="/home/ubuntu/LaughingBuddha"
UPLOADER_DIR="${PROJECT_DIR}/Uploader"
STAGING_MONGO_HOST="172.31.13.243"

echo "🔍 Checking current MongoDB configuration..."

# Check application-stage.properties
if [ -f "${UPLOADER_DIR}/application-stage.properties" ]; then
    echo "📄 Checking application-stage.properties..."
    CURRENT_HOST=$(grep "^mongo.host=" "${UPLOADER_DIR}/application-stage.properties" | cut -d'=' -f2)
    echo "   Current mongo.host: ${CURRENT_HOST}"
    
    if [ "${CURRENT_HOST}" != "${STAGING_MONGO_HOST}" ]; then
        echo "⚠️  MongoDB host is NOT set to staging (${STAGING_MONGO_HOST})"
        echo "🔧 Fixing application-stage.properties..."
        sed -i "s|^mongo.host=.*|mongo.host=${STAGING_MONGO_HOST}|" "${UPLOADER_DIR}/application-stage.properties"
        echo "✅ Updated mongo.host to ${STAGING_MONGO_HOST}"
    else
        echo "✅ MongoDB host is correctly set to ${STAGING_MONGO_HOST}"
    fi
else
    echo "❌ application-stage.properties not found at ${UPLOADER_DIR}/application-stage.properties"
    exit 1
fi

# Check docker-compose.staging.yml
if [ -f "${PROJECT_DIR}/docker-compose.staging.yml" ]; then
    echo "📄 Checking docker-compose.staging.yml..."
    CURRENT_DOCKER_HOST=$(grep "MONGO_HOST=" "${PROJECT_DIR}/docker-compose.staging.yml" | grep -v "^#" | head -1 | cut -d'=' -f2)
    echo "   Current MONGO_HOST in docker-compose: ${CURRENT_DOCKER_HOST}"
    
    if [ "${CURRENT_DOCKER_HOST}" != "${STAGING_MONGO_HOST}" ]; then
        echo "⚠️  MONGO_HOST in docker-compose is NOT set to staging"
        echo "🔧 Fixing docker-compose.staging.yml..."
        sed -i "s|MONGO_HOST=.*|MONGO_HOST=${STAGING_MONGO_HOST}|" "${PROJECT_DIR}/docker-compose.staging.yml"
        echo "✅ Updated MONGO_HOST to ${STAGING_MONGO_HOST}"
    else
        echo "✅ MONGO_HOST in docker-compose is correctly set to ${STAGING_MONGO_HOST}"
    fi
else
    echo "❌ docker-compose.staging.yml not found at ${PROJECT_DIR}/docker-compose.staging.yml"
    exit 1
fi

# Verify SPRING_PROFILES_ACTIVE is set to stage
echo "📄 Checking SPRING_PROFILES_ACTIVE in docker-compose..."
if grep -q "SPRING_PROFILES_ACTIVE=stage" "${PROJECT_DIR}/docker-compose.staging.yml"; then
    echo "✅ SPRING_PROFILES_ACTIVE is set to 'stage'"
else
    echo "⚠️  SPRING_PROFILES_ACTIVE might not be set to 'stage'"
fi

echo ""
echo "🔄 Restarting uploader container to apply changes..."
cd "${PROJECT_DIR}"
docker compose -f docker-compose.staging.yml stop uploader || echo "Container not running"
docker compose -f docker-compose.staging.yml rm -f uploader || echo "Container not found"
docker compose -f docker-compose.staging.yml up -d uploader

echo ""
echo "⏳ Waiting for container to start (10 seconds)..."
sleep 10

echo ""
echo "✅ Verifying MongoDB connection in container..."
docker exec laughingbuddha-uploader python3 -c "
from app.core.config import config
print(f'MongoDB Host: {config.mongodb.host}')
print(f'MongoDB Port: {config.mongodb.port}')
print(f'MongoDB Database: {config.mongodb.database}')
if config.mongodb.host == '${STAGING_MONGO_HOST}':
    print('✅ MongoDB is configured for STAGING')
else:
    print('❌ MongoDB is NOT configured for staging!')
    exit(1)
" || echo "⚠️  Could not verify MongoDB config in container"

echo ""
echo "📋 Container logs (last 20 lines):"
docker logs laughingbuddha-uploader --tail 20

echo ""
echo "✅ Done! MongoDB configuration should now be set to staging (${STAGING_MONGO_HOST})"

