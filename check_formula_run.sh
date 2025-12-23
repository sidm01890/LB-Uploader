#!/bin/bash
# Quick script to check if formula calculation job ran

echo "🔍 Checking Formula Calculation Job Status..."
echo "=============================================="
echo ""

ssh -i "/Users/siddharthmishra/Downloads/core 2.pem" ubuntu@65.0.236.144 << 'SSH_EOF'
cd /home/ubuntu/LaughingBuddha

echo "📋 Recent Scheduler & Formula Logs:"
echo "------------------------------------"
docker compose -f docker-compose.staging.yml logs --tail=100 uploader | grep -i "scheduler\|formula\|calculation" | tail -30

echo ""
echo "🔄 Formula Calculation Job Status:"
echo "-----------------------------------"
echo "Last completed run:"
docker compose -f docker-compose.staging.yml logs uploader | grep -i "formula calculation job completed" | tail -1

echo ""
echo "Last started run:"
docker compose -f docker-compose.staging.yml logs uploader | grep -i "starting scheduled formula calculation" | tail -1

echo ""
echo "⚠️ Any errors or warnings:"
docker compose -f docker-compose.staging.yml logs --tail=200 uploader | grep -i "error\|warning" | grep -i "formula\|scheduler" | tail -10

echo ""
echo "📊 Container Status:"
echo "-------------------"
docker compose -f docker-compose.staging.yml ps uploader
SSH_EOF

echo ""
echo "✅ Check complete!"

