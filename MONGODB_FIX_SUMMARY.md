# MongoDB Connection Fix - Implementation Summary

## Problem
The Docker container `laughingbuddha-uploader` was trying to connect to MongoDB at `localhost:27017`, but from inside a Docker container, `localhost` refers to the container itself, not the host machine where MongoDB is actually running.

## Solution Implemented

### 1. Updated `application-stage.properties`
Added MongoDB configuration with the host IP address:
```properties
mongo.host=172.31.13.243
mongo.port=27017
mongo.database=devyani_mongo
mongo.username=
mongo.password=
mongo.auth.source=admin
```

### 2. Updated `docker-compose.staging.yml`
- Changed `MONGO_HOST` from `host.docker.internal` to `172.31.13.243` (direct host IP)
- Added volume mounts for properties files so changes take effect without rebuilding:
  ```yaml
  volumes:
    - ./Uploader/application.properties:/app/application.properties:ro
    - ./Uploader/application-stage.properties:/app/application-stage.properties:ro
  ```

## Quick Fix (Automated Script)

Run the automated fix script on the server:

```bash
# Copy the script to server (from your local machine)
scp -i "/Users/siddharthmishra/Downloads/core 2.pem" \
  fix-mongodb-connection.sh \
  ubuntu@65.0.236.144:~/LaughingBuddha/

# SSH into server
ssh -i "/Users/siddharthmishra/Downloads/core 2.pem" ubuntu@65.0.236.144

# Run the script
cd ~/LaughingBuddha
chmod +x fix-mongodb-connection.sh
./fix-mongodb-connection.sh
```

## Manual Fix Steps

If you prefer to run commands manually:

### Step 1: Update Properties File on Server

```bash
cd ~/LaughingBuddha/Uploader

# Add MongoDB configuration
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
```

### Step 2: Restart Container with Updated Configuration

```bash
cd ~/LaughingBuddha

# Stop and remove old container
docker stop laughingbuddha-uploader
docker rm laughingbuddha-uploader

# Start with MongoDB host and mounted properties files
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
```

### Step 3: Verify the Fix

```bash
# Check MongoDB connection in logs
docker logs laughingbuddha-uploader | grep -i mongo

# You should see:
# ✅ MongoDB connection established successfully

# Test the API endpoint
curl http://localhost:8010/api/uploader/reports/formulas/all

# Should return data instead of 503 error
```

## Verification

After applying the fix, verify:

1. **MongoDB Connection Status:**
   ```bash
   docker logs laughingbuddha-uploader | grep -i "MongoDB connection"
   ```
   Expected: `✅ MongoDB connection established successfully`

2. **API Endpoint Test:**
   ```bash
   curl http://localhost:8010/api/uploader/reports/formulas/all
   ```
   Expected: JSON response with data (not 503 error)

3. **Health Check:**
   ```bash
   curl http://localhost:8010/api/health
   ```
   Expected: MongoDB status should show "connected"

## Files Modified

1. ✅ `application-stage.properties` - Added MongoDB host configuration
2. ✅ `docker-compose.staging.yml` - Updated MongoDB host and added volume mounts
3. ✅ `fix-mongodb-connection.sh` - Automated fix script (new file)

## Why This Works

1. **Properties File**: The application reads MongoDB configuration from `application-stage.properties`. By setting `mongo.host=172.31.13.243`, the container can reach MongoDB on the host.

2. **Environment Variables**: Setting `MONGO_HOST=172.31.13.243` as an environment variable ensures it takes precedence over any default values.

3. **Volume Mounts**: Mounting the properties files as volumes allows changes to take effect without rebuilding the Docker image.

4. **Direct IP**: Using `172.31.13.243` (the host's private IP) is more reliable than `host.docker.internal` on Linux systems.

## Troubleshooting

If the connection still fails:

1. **Check MongoDB is running:**
   ```bash
   sudo systemctl status mongod
   ```

2. **Verify MongoDB is listening on all interfaces:**
   ```bash
   sudo netstat -tlnp | grep 27017
   ```
   Should show: `0.0.0.0:27017` (not `127.0.0.1:27017`)

3. **Check container can reach host:**
   ```bash
   docker exec laughingbuddha-uploader python -c "import socket; s = socket.socket(); s.connect(('172.31.13.243', 27017)); print('Connected!')"
   ```

4. **Check firewall rules:**
   ```bash
   sudo ufw status
   sudo iptables -L -n | grep 27017
   ```

## Next Steps

After the fix is applied:
- Monitor the logs for a few minutes to ensure stability
- Test all MongoDB-dependent endpoints
- Consider setting up MongoDB connection monitoring/alerts

