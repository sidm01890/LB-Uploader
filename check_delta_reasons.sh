#!/bin/bash
# Check if formula document has delta_columns and reasons configured

SSH_KEY="/Users/siddharthmishra/Downloads/core 2.pem"
SERVER_HOST="65.0.236.144"
SERVER_USER="ubuntu"

echo "Checking formula document structure for delta_columns and reasons..."
echo ""

ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${SERVER_USER}@${SERVER_HOST} << 'SSH_EOF'
docker compose -f /home/ubuntu/LaughingBuddha/docker-compose.staging.yml exec -T uploader python3 << 'PYTHON_EOF'
from app.services.mongodb_service import mongodb_service
import json

# Connect to MongoDB
mongodb_service.connect()

# Get formula document
formula_doc = mongodb_service.get_report_formulas("zomato_vs_pos")

if formula_doc:
    print("📋 Formula Document Structure:")
    print("=" * 60)
    print(f"Report Name: {formula_doc.get('report_name')}")
    print(f"Formulas Count: {len(formula_doc.get('formulas', []))}")
    print(f"Delta Columns Count: {len(formula_doc.get('delta_columns', []))}")
    print(f"Reasons Count: {len(formula_doc.get('reasons', []))}")
    print("")
    
    if formula_doc.get('delta_columns'):
        print("✅ Delta Columns Configured:")
        for i, dc in enumerate(formula_doc.get('delta_columns', []), 1):
            print(f"  {i}. {dc.get('delta_column_name')} = {dc.get('value', 'N/A')}")
    else:
        print("❌ No Delta Columns Configured")
    
    print("")
    
    if formula_doc.get('reasons'):
        print("✅ Reasons Configured:")
        for i, reason in enumerate(formula_doc.get('reasons', []), 1):
            print(f"  {i}. {reason.get('reason')} (delta: {reason.get('delta_column')}, threshold: {reason.get('threshold')})")
    else:
        print("❌ No Reasons Configured")
    
    print("")
    print("=" * 60)
else:
    print("❌ Formula document not found")
PYTHON_EOF
SSH_EOF
