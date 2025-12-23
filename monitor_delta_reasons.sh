#!/bin/bash
# Monitor delta columns, reasons, and reconciliation status processing

SSH_KEY="/Users/siddharthmishra/Downloads/core 2.pem"
SERVER_HOST="65.0.236.144"
SERVER_USER="ubuntu"

echo "🔍 Monitoring Delta Columns, Reasons, and Reconciliation Status..."
echo "Press Ctrl+C to stop"
echo ""

ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${SERVER_USER}@${SERVER_HOST} \
  "cd /home/ubuntu/LaughingBuddha && docker compose -f docker-compose.staging.yml logs -f uploader" | \
  grep -E --line-buffered "Calculating delta|delta/reason|Processed.*batch.*delta|reconciliation_status|Formula calculation job completed|Processing.*record.*delta"

