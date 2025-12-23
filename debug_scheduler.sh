#!/bin/bash
# Comprehensive scheduler debugging script

echo "🔍 Scheduler Debugging Tool"
echo "============================"
echo ""

SSH_KEY="/Users/siddharthmishra/Downloads/core 2.pem"
SERVER_HOST="65.0.236.144"
SERVER_USER="ubuntu"
PROJECT_DIR="/home/ubuntu/LaughingBuddha"

echo "📋 Step 1: Checking recent scheduler logs..."
echo "--------------------------------------------"
ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${SERVER_USER}@${SERVER_HOST} << 'SSH_EOF'
cd /home/ubuntu/LaughingBuddha

echo "Recent scheduler activity (last 50 lines):"
docker compose -f docker-compose.staging.yml logs --tail=50 uploader | grep -i "scheduler\|formula\|calculation" | tail -20

echo ""
echo "Last formula calculation job run:"
docker compose -f docker-compose.staging.yml logs uploader | grep -i "formula calculation job" | tail -5

echo ""
echo "Any warnings about skipping formulas:"
docker compose -f docker-compose.staging.yml logs uploader | grep -i "skipping formula" | tail -10
SSH_EOF

echo ""
echo "📊 Step 2: Checking MongoDB formula documents..."
echo "------------------------------------------------"
echo "Running check_formula_document.py on server..."

ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${SERVER_USER}@${SERVER_HOST} << 'SSH_EOF'
cd /home/ubuntu/LaughingBuddha/Uploader

# Check if script exists, if not create it
if [ ! -f "check_formula_document.py" ]; then
    echo "⚠️ check_formula_document.py not found on server"
    echo "Checking MongoDB directly via Python..."
    docker compose -f ../docker-compose.staging.yml exec -T uploader python3 << 'PYTHON_EOF'
import sys
sys.path.insert(0, '/app')
from app.services.mongodb_service import mongodb_service
from app.core.config import config

if mongodb_service.is_connected() and mongodb_service.db:
    formulas_collection = mongodb_service.db.get_collection("formulas")
    docs = list(formulas_collection.find({}))
    print(f"Found {len(docs)} formula document(s)")
    for doc in docs:
        print(f"\nDocument ID: {doc.get('_id')}")
        print(f"  report_name: {doc.get('report_name')}")
        print(f"  formulas: {len(doc.get('formulas', []))} formula(s)")
        if doc.get('formulas'):
            for f in doc.get('formulas', [])[:3]:
                print(f"    - {f.get('logicNameKey', 'N/A')} = {f.get('formulaText', 'N/A')[:50]}")
else:
    print("❌ MongoDB not connected")
PYTHON_EOF
else
    python3 check_formula_document.py
fi
SSH_EOF

echo ""
echo "✅ Debugging complete!"
echo ""
echo "💡 Next steps:"
echo "   1. If formulas are missing 'report_name' or 'formulas' fields, fix the document structure"
echo "   2. Wait for the next scheduler run (every 2 hours, or restart service to trigger immediately)"
echo "   3. Monitor logs: docker compose -f docker-compose.staging.yml logs -f uploader | grep -i formula"

