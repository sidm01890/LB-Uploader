# Fix MongoDB Configuration for Staging

## Problem
MongoDB was manually changed to use localhost instead of staging host (172.31.13.243).

## Quick Fix Script

### Option 1: Run the fix script on server

1. **Copy the script to server:**
   ```bash
   scp -i "/Users/siddharthmishra/Downloads/core 2.pem" \
       fix-staging-mongo.sh \
       ubuntu@65.0.236.144:~/fix-staging-mongo.sh
   ```

2. **SSH into server and run:**
   ```bash
   ssh -i "/Users/siddharthmishra/Downloads/core 2.pem" ubuntu@65.0.236.144
   chmod +x ~/fix-staging-mongo.sh
   ~/fix-staging-mongo.sh
   ```

### Option 2: Manual fix steps

1. **SSH into server:**
   ```bash
   ssh -i "/Users/siddharthmishra/Downloads/core 2.pem" ubuntu@65.0.236.144
   ```

2. **Check current MongoDB config:**
   ```bash
   cd ~/LaughingBuddha/Uploader
   grep "mongo.host" application-stage.properties
   ```

3. **Fix application-stage.properties:**
   ```bash
   sed -i 's|^mongo.host=.*|mongo.host=172.31.13.243|' application-stage.properties
   cat application-stage.properties | grep mongo.host
   ```

4. **Check docker-compose.staging.yml:**
   ```bash
   cd ~/LaughingBuddha
   grep "MONGO_HOST" docker-compose.staging.yml
   ```

5. **Fix docker-compose.staging.yml (if needed):**
   ```bash
   sed -i 's|MONGO_HOST=.*|MONGO_HOST=172.31.13.243|' docker-compose.staging.yml
   ```

6. **Restart uploader container:**
   ```bash
   cd ~/LaughingBuddha
   docker compose -f docker-compose.staging.yml stop uploader
   docker compose -f docker-compose.staging.yml rm -f uploader
   docker compose -f docker-compose.staging.yml up -d uploader
   ```

7. **Verify MongoDB connection:**
   ```bash
   sleep 10
   docker exec laughingbuddha-uploader python3 -c "
   from app.core.config import config
   print(f'MongoDB Host: {config.mongodb.host}')
   print(f'MongoDB Port: {config.mongodb.port}')
   print(f'MongoDB Database: {config.mongodb.database}')
   "
   ```

8. **Check container logs:**
   ```bash
   docker logs laughingbuddha-uploader --tail 30
   ```

## Expected Configuration

### application-stage.properties
```properties
mongo.host=172.31.13.243
mongo.port=27017
mongo.database=devyani_mongo
```

### docker-compose.staging.yml
```yaml
environment:
  - SPRING_PROFILES_ACTIVE=stage
  - MONGO_HOST=172.31.13.243
  - MONGO_PORT=27017
  - MONGO_DATABASE=devyani_mongo
```

## Verification

After fixing, verify the container is using staging MongoDB:

```bash
docker exec laughingbuddha-uploader python3 -c "
from app.core.config import config
assert config.mongodb.host == '172.31.13.243', 'MongoDB host is not staging!'
print('✅ MongoDB is correctly configured for staging')
"
```

## Why 172.31.13.243?

- This is the **host machine's private IP** (where MongoDB is running)
- Docker containers need to use this IP to access MongoDB on the host
- `localhost` inside a container refers to the container itself, not the host

