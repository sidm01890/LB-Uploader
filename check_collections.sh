#!/bin/bash
# Check MongoDB collections for formula processing

SSH_KEY="/Users/siddharthmishra/Downloads/core 2.pem"
SERVER_HOST="65.0.236.144"
SERVER_USER="ubuntu"

echo "🔍 Checking MongoDB Collections..."
echo "===================================="
echo ""

ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${SERVER_USER}@${SERVER_HOST} << 'SSH_EOF'
cd /home/ubuntu/LaughingBuddha

echo "📊 Checking collections related to zomato_vs_pos report..."
echo ""

docker compose -f docker-compose.staging.yml exec -T uploader python3 << 'PYTHON_EOF'
import sys
sys.path.insert(0, '/app')
from app.services.mongodb_service import mongodb_service
from app.core.config import config

if not mongodb_service.is_connected():
    print("❌ MongoDB not connected")
    sys.exit(1)

if mongodb_service.db is None:
    print("❌ Database not available")
    sys.exit(1)

print("✅ MongoDB connected")
print()

# Check formulas collection
print("📋 Formulas Collection:")
print("-" * 50)
formulas_collection = mongodb_service.db.get_collection("formulas")
formula_docs = list(formulas_collection.find({}))
print(f"Total formula documents: {len(formula_docs)}")

for doc in formula_docs:
    print(f"\n  Document ID: {doc.get('_id')}")
    print(f"  report_name: {doc.get('report_name')}")
    formulas = doc.get('formulas', [])
    print(f"  formulas count: {len(formulas)}")
    if formulas:
        for f in formulas[:3]:
            print(f"    - {f.get('logicNameKey', 'N/A')} = {f.get('formulaText', 'N/A')[:60]}")
    mapping_keys = doc.get('mapping_keys', {})
    print(f"  mapping_keys: {list(mapping_keys.keys()) if mapping_keys else 'None'}")

print()
print("📊 Source Collections:")
print("-" * 50)

# Check zomato_processed collection
collections_to_check = ['zomato_processed', 'zomato', 'zomato_vs_pos']

for coll_name in collections_to_check:
    if coll_name in mongodb_service.db.list_collection_names():
        collection = mongodb_service.db.get_collection(coll_name)
        count = collection.count_documents({})
        print(f"  {coll_name}: {count:,} document(s)")
        
        if count > 0 and coll_name == 'zomato_processed':
            # Show sample document structure
            sample = collection.find_one({})
            if sample:
                print(f"    Sample fields: {list(sample.keys())[:10]}")
    else:
        print(f"  {coll_name}: Collection does not exist")

print()
print("✅ Check complete!")
PYTHON_EOF

SSH_EOF

echo ""
echo "💡 Analysis:"
echo "   - If 'zomato_processed' has 0 documents, you need to process raw data first"
echo "   - If 'zomato_processed' has documents, check if they match the formula's field requirements"

