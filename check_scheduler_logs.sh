#!/bin/bash
# Quick script to check scheduler logs on staging server

echo "🔍 Checking Scheduler Logs..."
echo "================================"
echo ""

# SSH command to check logs
ssh ubuntu@65.0.236.144 << 'SSH_EOF'
cd /home/ubuntu/LaughingBuddha

echo "📋 Recent Scheduler Logs:"
echo "---------------------------"
docker compose -f docker-compose.staging.yml logs --tail=50 uploader | grep -i "scheduler\|formula\|calculation" | tail -20

echo ""
echo "📊 Container Status:"
echo "-------------------"
docker compose -f docker-compose.staging.yml ps uploader

echo ""
echo "🔄 Last Formula Calculation Run:"
echo "----------------------------------"
docker compose -f docker-compose.staging.yml logs uploader | grep -i "formula calculation job completed" | tail -1

echo ""
echo "✅ Scheduler Startup Status:"
echo "----------------------------"
docker compose -f docker-compose.staging.yml logs uploader | grep -i "scheduler started\|formula calculation job added" | tail -2
SSH_EOF

echo ""
echo "✅ Log check complete!"
