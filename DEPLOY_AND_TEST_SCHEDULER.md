# Deployment and Scheduler Testing Guide

## 🚀 Deployment Steps

### Option 1: Deploy via Jenkins (Recommended)
1. Go to Jenkins pipeline: `Jenkinsfile.uploader`
2. Select branch: **`main`**
3. Run the pipeline
4. Wait for deployment to complete

### Option 2: Manual Deployment via SSH
```bash
# SSH to server
ssh ubuntu@65.0.236.144

# Navigate to project directory
cd /home/ubuntu/LaughingBuddha

# Pull latest code
cd Uploader
git checkout main
git pull origin main

# Restart the service
docker compose -f docker-compose.staging.yml stop uploader
docker compose -f docker-compose.staging.yml build uploader
docker compose -f docker-compose.staging.yml up -d uploader

# Wait for startup
sleep 25
```

---

## 🔄 Restart Service Commands

```bash
# SSH to server
ssh ubuntu@65.0.236.144

# Navigate to project
cd /home/ubuntu/LaughingBuddha

# Restart uploader service
docker compose -f docker-compose.staging.yml restart uploader

# OR stop and start fresh
docker compose -f docker-compose.staging.yml stop uploader
docker compose -f docker-compose.staging.yml up -d uploader
```

---

## 📋 Check Scheduler Logs

### Real-time Log Monitoring
```bash
# SSH to server
ssh ubuntu@65.0.236.144

# Navigate to project
cd /home/ubuntu/LaughingBuddha

# Follow logs in real-time
docker compose -f docker-compose.staging.yml logs -f uploader

# OR check last 100 lines
docker compose -f docker-compose.staging.yml logs --tail=100 uploader

# OR check logs from last 10 minutes
docker compose -f docker-compose.staging.yml logs --since=10m uploader
```

### Filter Logs for Scheduler
```bash
# Filter for scheduler-related logs
docker compose -f docker-compose.staging.yml logs uploader | grep -i "scheduler\|formula\|calculation"

# Filter for formula calculation job
docker compose -f docker-compose.staging.yml logs uploader | grep -i "formula calculation"

# Filter for scheduled job processing
docker compose -f docker-compose.staging.yml logs uploader | grep -i "scheduled.*job"
```

---

## ✅ What to Look For in Logs

### 1. **Scheduler Startup (Immediate after restart)**
Look for these log messages:
```
✅ Scheduler started successfully
✅ Formula calculation job added to scheduler
✅ Scheduled job processing started:
   - Formula Calculation: first run in 1 minute(s), then every 2 hour(s)
   - Collection Data Processing: runs immediately after file upload
   - Total scheduled jobs: 1
      * Formula Calculation Job (id: formula_calculation)
        Next run: [timestamp] (in [seconds] seconds / [minutes] minutes)
```

### 2. **First Formula Calculation Run (1 minute after startup)**
Look for these log messages:
```
🔄 Starting scheduled formula calculation job...
💾 Memory-efficient batch processing enabled for large datasets
📋 Processing [X] formula(s) for report '[report_name]'
📋 Processing collection '[collection_name]': Found [X] formula(s)
🔄 Processing batch [X]/[Y] for collection '[collection_name]'
✅ Batch [X]/[Y] completed. Total processed so far: [count], errors: [count]
✅ Formula calculation job completed: [X] report(s) processed, [Y] document(s) processed in [Z] seconds
🧹 Final memory cleanup completed after formula calculation job
```

### 3. **Subsequent Runs (Every 2 hours)**
Same log pattern as above, repeating every 2 hours.

### 4. **Error Messages to Watch For**
```
❌ MongoDB is not connected. Skipping formula calculation job.
❌ Network/Connection error in scheduled formula calculation job
❌ Error in scheduled formula calculation job
❌ Error processing report '[report_name]'
```

---

## 🧪 Test the Scheduler

### Test 1: Verify Scheduler Started
```bash
# Check if scheduler is running
docker compose -f docker-compose.staging.yml logs uploader | grep "Scheduler started"
```

### Test 2: Wait for First Run (1 minute)
```bash
# Monitor logs for first formula calculation
docker compose -f docker-compose.staging.yml logs -f uploader | grep -i "formula calculation"
```

### Test 3: Verify Formula Processing
```bash
# Check if formulas were processed
docker compose -f docker-compose.staging.yml logs uploader | grep "Formula calculation job completed"
```

### Test 4: Check MongoDB Collections
```bash
# Connect to MongoDB and verify report collections were created/updated
# Reports should be in collections named after report_name from formulas collection
```

---

## 🔧 Configuration

The scheduler uses these environment variables (set in docker-compose or .env):

- `FORMULA_JOB_FIRST_RUN_DELAY_MINUTES` (default: 1 minute)
- `FORMULA_JOB_INTERVAL_HOURS` (default: 2 hours)
- `SCHEDULED_JOB_BATCH_SIZE` (default: 5000)
- `FORMULA_JOB_BATCH_SIZE` (default: 1000)

To change the interval, add to `docker-compose.staging.yml`:
```yaml
environment:
  - FORMULA_JOB_FIRST_RUN_DELAY_MINUTES=1
  - FORMULA_JOB_INTERVAL_HOURS=2
```

---

## 📊 Expected Behavior

1. **On Startup:**
   - Scheduler starts immediately
   - Formula calculation job is scheduled
   - First run happens after 1 minute (default)

2. **Formula Calculation:**
   - Reads from `formulas` collection in MongoDB
   - Processes each report's formulas
   - Reads from `*_processed` collections
   - Calculates formulas and saves to report collections
   - Handles large datasets with batch processing

3. **Scheduling:**
   - Runs every 2 hours (configurable)
   - Processes all reports found in formulas collection
   - Logs detailed progress for each batch

---

## 🐛 Troubleshooting

### Scheduler Not Starting
```bash
# Check for errors in logs
docker compose -f docker-compose.staging.yml logs uploader | grep -i "error\|failed"

# Verify MongoDB connection
docker compose -f docker-compose.staging.yml exec uploader python3 -c "from app.services.mongodb_service import mongodb_service; print('Connected:', mongodb_service.is_connected())"
```

### Formulas Not Processing
```bash
# Check if formulas collection exists and has data
# Check MongoDB connection
# Verify logs for specific errors
docker compose -f docker-compose.staging.yml logs uploader | grep -i "formula\|error"
```

### Service Not Restarting
```bash
# Check container status
docker compose -f docker-compose.staging.yml ps uploader

# Check container logs
docker compose -f docker-compose.staging.yml logs uploader --tail=50
```

