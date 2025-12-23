#!/bin/bash
# Deploy the fixed scheduled_jobs_controller.py to server

SSH_KEY="/Users/siddharthmishra/Downloads/core 2.pem"
SERVER_HOST="65.0.236.144"
SERVER_USER="ubuntu"
PROJECT_DIR="/home/ubuntu/LaughingBuddha"
UPLOADER_DIR="$PROJECT_DIR/Uploader"

echo "🚀 Deploying fix to server..."
echo "=============================="
echo ""

# Copy the fixed file to server
echo "📤 Copying fixed file to server..."
scp -i "$SSH_KEY" \
    -o StrictHostKeyChecking=no \
    -o UserKnownHostsFile=/dev/null \
    app/controllers/scheduled_jobs_controller.py \
    ${SERVER_USER}@${SERVER_HOST}:${UPLOADER_DIR}/app/controllers/scheduled_jobs_controller.py

if [ $? -eq 0 ]; then
    echo "✅ File copied successfully"
else
    echo "❌ Failed to copy file"
    exit 1
fi

echo ""
echo "🔄 Restarting Docker container..."
echo ""

# SSH and restart
ssh -i "$SSH_KEY" \
    -o StrictHostKeyChecking=no \
    -o UserKnownHostsFile=/dev/null \
    ${SERVER_USER}@${SERVER_HOST} << 'SSH_EOF'
cd /home/ubuntu/LaughingBuddha

echo "🛑 Stopping container..."
docker compose -f docker-compose.staging.yml stop uploader

echo "🔨 Rebuilding container..."
docker compose -f docker-compose.staging.yml build uploader

echo "🚀 Starting container..."
docker compose -f docker-compose.staging.yml up -d uploader

echo "⏳ Waiting for startup (30 seconds)..."
sleep 30

echo ""
echo "✅ Deployment complete!"
echo ""
echo "📊 Container status:"
docker compose -f docker-compose.staging.yml ps uploader

echo ""
echo "📋 Recent logs:"
docker compose -f docker-compose.staging.yml logs --tail=20 uploader | grep -i "scheduler\|formula"
SSH_EOF

echo ""
echo "✅ Fix deployed! Monitor logs with:"
echo "   docker compose -f docker-compose.staging.yml logs -f uploader | grep -i formula"

