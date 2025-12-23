#!/bin/bash
# MongoDB Connection Fix Script
# This script fixes the MongoDB connection issue for Docker containers

set -e

echo "=========================================="
echo "MongoDB Connection Fix Script"
echo "=========================================="
echo ""

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Check if running on server
if [ ! -d "/home/ubuntu/LaughingBuddha" ]; then
    echo -e "${RED}Error: This script should be run on the server${NC}"
    exit 1
fi

cd ~/LaughingBuddha/Uploader

echo -e "${YELLOW}Step 1: Updating application-stage.properties with MongoDB host...${NC}"

# Check if MongoDB config already exists
if grep -q "mongo.host=172.31.13.243" application-stage.properties; then
    echo -e "${GREEN}✓ MongoDB configuration already exists in application-stage.properties${NC}"
else
    # Add MongoDB configuration after CORS line
    if [ -f application-stage.properties ]; then
        # Find the line number after CORS configuration
        CORS_LINE=$(grep -n "cors.allowed.origins" application-stage.properties | cut -d: -f1)
        if [ -n "$CORS_LINE" ]; then
            # Insert MongoDB config after CORS line
            sed -i "${CORS_LINE}a\\
\\
# ============================================================================\\
# MongoDB Configuration (for Docker containers to access host MongoDB)\\
# ============================================================================\\
mongo.host=172.31.13.243\\
mongo.port=27017\\
mongo.database=devyani_mongo\\
mongo.username=\\
mongo.password=\\
mongo.auth.source=admin\\
" application-stage.properties
            echo -e "${GREEN}✓ Added MongoDB configuration to application-stage.properties${NC}"
        else
            # Append at the end if CORS line not found
            cat >> application-stage.properties << 'EOF'

# ============================================================================
# MongoDB Configuration (for Docker containers to access host MongoDB)
# ============================================================================
mongo.host=172.31.13.243
mongo.port=27017
mongo.database=devyani_mongo
mongo.username=
mongo.password=
mongo.auth.source=admin
EOF
            echo -e "${GREEN}✓ Appended MongoDB configuration to application-stage.properties${NC}"
        fi
    else
        echo -e "${RED}Error: application-stage.properties not found${NC}"
        exit 1
    fi
fi

echo ""
echo -e "${YELLOW}Step 2: Stopping the uploader container...${NC}"
docker stop laughingbuddha-uploader 2>/dev/null || echo "Container not running"

echo ""
echo -e "${YELLOW}Step 3: Removing the old container...${NC}"
docker rm laughingbuddha-uploader 2>/dev/null || echo "Container not found"

echo ""
echo -e "${YELLOW}Step 4: Starting container with updated configuration...${NC}"

cd ~/LaughingBuddha

# Start container with MongoDB host environment variable and mounted properties files
docker run -d \
  --name laughingbuddha-uploader \
  --restart unless-stopped \
  -e SPRING_PROFILES_ACTIVE=stage \
  -e APP_ENV=staging \
  -e APP_PROFILE=stage \
  -e MONGO_HOST=172.31.13.243 \
  -e MONGO_PORT=27017 \
  -e MONGO_DATABASE=devyani_mongo \
  -e MYSQL_HOST=coreco-mysql.cpzxmgfkrh6g.ap-south-1.rds.amazonaws.com \
  -e MYSQL_PORT=3306 \
  -e MYSQL_USER=admin \
  -e MYSQL_PASSWORD='One4the$#' \
  -e MYSQL_DB=devyani \
  -e UVICORN_HOST=0.0.0.0 \
  -e UVICORN_PORT=8010 \
  -e UVICORN_RELOAD=false \
  -e CORS_ALLOWED_ORIGINS='https://reconciistage.corepeelers.com,http://reconciistage.corepeelers.com' \
  -p 8033:8010 \
  -v ./Uploader/data:/app/data \
  -v ./Uploader/logs:/app/logs \
  -v ./Uploader/reports:/app/reports \
  -v ./Uploader/application.properties:/app/application.properties:ro \
  -v ./Uploader/application-stage.properties:/app/application-stage.properties:ro \
  --network laughingbuddha-network \
  laughingbuddha-uploader:latest

echo ""
echo -e "${GREEN}✓ Container started successfully${NC}"

echo ""
echo -e "${YELLOW}Step 5: Waiting for container to start...${NC}"
sleep 5

echo ""
echo -e "${YELLOW}Step 6: Checking MongoDB connection status...${NC}"

# Check logs for MongoDB connection
if docker logs laughingbuddha-uploader 2>&1 | grep -q "✅ MongoDB connection established successfully"; then
    echo -e "${GREEN}✓ MongoDB connection established successfully!${NC}"
    SUCCESS=true
elif docker logs laughingbuddha-uploader 2>&1 | grep -q "❌ Failed to connect to MongoDB"; then
    echo -e "${RED}✗ MongoDB connection failed${NC}"
    echo ""
    echo "Recent MongoDB-related logs:"
    docker logs laughingbuddha-uploader 2>&1 | grep -i mongo | tail -5
    SUCCESS=false
else
    echo -e "${YELLOW}⚠ Could not determine connection status from logs${NC}"
    echo "Showing recent logs:"
    docker logs --tail 20 laughingbuddha-uploader
    SUCCESS=false
fi

echo ""
echo -e "${YELLOW}Step 7: Testing API endpoint...${NC}"

# Test the API endpoint
sleep 2
RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8010/api/uploader/reports/formulas/all || echo "000")

if [ "$RESPONSE" = "200" ]; then
    echo -e "${GREEN}✓ API endpoint is working (HTTP 200)${NC}"
    echo ""
    echo "Sample response:"
    curl -s http://localhost:8010/api/uploader/reports/formulas/all | head -20
elif [ "$RESPONSE" = "503" ]; then
    echo -e "${RED}✗ API endpoint still returning 503 (MongoDB not connected)${NC}"
    echo ""
    echo "Please check:"
    echo "1. MongoDB is running: sudo systemctl status mongod"
    echo "2. MongoDB is accessible: sudo netstat -tlnp | grep 27017"
    echo "3. Container logs: docker logs -f laughingbuddha-uploader"
else
    echo -e "${YELLOW}⚠ API endpoint returned HTTP $RESPONSE${NC}"
fi

echo ""
echo "=========================================="
if [ "$SUCCESS" = true ] && [ "$RESPONSE" = "200" ]; then
    echo -e "${GREEN}✓ Fix applied successfully!${NC}"
    echo ""
    echo "MongoDB connection is working."
    echo "API endpoint: http://localhost:8010/api/uploader/reports/formulas/all"
else
    echo -e "${YELLOW}⚠ Please review the output above${NC}"
    echo ""
    echo "To check logs: docker logs -f laughingbuddha-uploader"
    echo "To test API: curl http://localhost:8010/api/uploader/reports/formulas/all"
fi
echo "=========================================="

