#!/bin/bash
# Deployment and Scheduler Testing Script
# This script deploys the uploader service and tests the scheduler

set -e

SERVER_HOST="65.0.236.144"
SERVER_USER="ubuntu"
PROJECT_DIR="/home/ubuntu/LaughingBuddha"
SERVICE_NAME="uploader"

echo "🚀 Starting Deployment and Scheduler Test..."
echo "=============================================="
echo ""

# Function to run commands on remote server
run_remote() {
    ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${SERVER_USER}@${SERVER_HOST} "$@"
}

echo "📦 Step 1: Pulling latest code from main branch..."
run_remote "cd ${PROJECT_DIR}/Uploader && git checkout main && git pull origin main"

echo ""
echo "🛑 Step 2: Stopping existing container..."
run_remote "cd ${PROJECT_DIR} && docker compose -f docker-compose.staging.yml stop ${SERVICE_NAME} || echo 'Container not running'"

echo ""
echo "🔨 Step 3: Rebuilding Docker image..."
run_remote "cd ${PROJECT_DIR} && docker compose -f docker-compose.staging.yml build ${SERVICE_NAME}"

echo ""
echo "🚀 Step 4: Starting container..."
run_remote "cd ${PROJECT_DIR} && docker compose -f docker-compose.staging.yml up -d ${SERVICE_NAME}"

echo ""
echo "⏳ Step 5: Waiting for service to start (30 seconds)..."
sleep 30

echo ""
echo "📊 Step 6: Checking container status..."
run_remote "cd ${PROJECT_DIR} && docker compose -f docker-compose.staging.yml ps ${SERVICE_NAME}"

echo ""
echo "🔍 Step 7: Checking scheduler startup logs..."
echo "--------------------------------------------"
run_remote "cd ${PROJECT_DIR} && docker compose -f docker-compose.staging.yml logs --tail=50 ${SERVICE_NAME} | grep -i 'scheduler\|formula' || echo 'No scheduler logs found yet'"

echo ""
echo "⏳ Step 8: Waiting for first formula calculation run (90 seconds)..."
echo "   (First run happens 1 minute after startup)"
sleep 90

echo ""
echo "📋 Step 9: Checking formula calculation logs..."
echo "-----------------------------------------------"
run_remote "cd ${PROJECT_DIR} && docker compose -f docker-compose.staging.yml logs --tail=100 ${SERVICE_NAME} | grep -i 'formula calculation\|scheduled.*job\|formula.*processed' | tail -30"

echo ""
echo "✅ Step 10: Final status check..."
echo "---------------------------------"
run_remote "cd ${PROJECT_DIR} && echo 'Container Status:' && docker compose -f docker-compose.staging.yml ps ${SERVICE_NAME} && echo '' && echo 'Recent Scheduler Activity:' && docker compose -f docker-compose.staging.yml logs --tail=20 ${SERVICE_NAME} | grep -i 'scheduler\|formula' | tail -10"

echo ""
echo "=============================================="
echo "✅ Deployment and initial test complete!"
echo ""
echo "📝 To monitor logs in real-time, run:"
echo "   ssh ${SERVER_USER}@${SERVER_HOST}"
echo "   cd ${PROJECT_DIR}"
echo "   docker compose -f docker-compose.staging.yml logs -f ${SERVICE_NAME} | grep -i 'formula\|scheduler'"
echo ""
echo "📊 To check scheduler status anytime:"
echo "   ./check_scheduler_logs.sh"
echo ""

