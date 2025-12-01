#!/usr/bin/env python3
"""
Test script to verify upload saves to MongoDB
Run this after uploading a file to check MongoDB
"""

import sys
import os
from datetime import datetime, timedelta

# Add Uploader directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.services.mongodb_service import (
    list_uploads,
    get_uploaded_sheets_collection,
    get_mongodb_database,
    get_collection_name
)
from app.core.config import config


def test_recent_uploads():
    """Check recent uploads in MongoDB"""
    print("=" * 60)
    print("Checking Recent Uploads in MongoDB")
    print("=" * 60)
    print()
    
    try:
        # Get recent uploads (last 10)
        uploads = list_uploads(limit=10)
        
        if not uploads:
            print("❌ No uploads found in MongoDB")
            print()
            print("This means either:")
            print("  1. No files have been uploaded yet")
            print("  2. MongoDB save failed (check logs)")
            print("  3. MongoDB connection issue")
            return False
        
        print(f"✅ Found {len(uploads)} recent upload(s):")
        print()
        
        for i, upload in enumerate(uploads, 1):
            print(f"{i}. Upload ID: {upload.get('upload_id')}")
            print(f"   Filename: {upload.get('filename')}")
            print(f"   Datasource: {upload.get('datasource')}")
            print(f"   Table: {upload.get('table_name')}")
            print(f"   Status: {upload.get('status')}")
            print(f"   Rows: {upload.get('row_count')}")
            print(f"   Uploaded: {upload.get('uploaded_at')}")
            print(f"   Headers: {len(upload.get('headers', []))} columns")
            
            if upload.get('raw_data'):
                print(f"   Raw Data: ✅ {len(upload.get('raw_data'))} rows stored")
            else:
                print(f"   Raw Data: ❌ Not stored")
            
            if upload.get('validation_results'):
                print(f"   Validation: ✅ Results stored")
            
            if upload.get('mysql_upload_results'):
                results = upload.get('mysql_upload_results', {})
                print(f"   MySQL Upload: ✅ {results.get('successful_rows', 0)} rows uploaded")
            
            if upload.get('error'):
                print(f"   Error: ⚠️ {upload.get('error')}")
            
            print()
        
        return True
        
    except Exception as e:
        print(f"❌ Error checking uploads: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_collection_stats():
    """Get collection statistics"""
    print("=" * 60)
    print("MongoDB Collection Statistics")
    print("=" * 60)
    print()
    
    try:
        db = get_mongodb_database()
        
        # Get all collections (datasource-specific + default)
        all_collections = db.list_collection_names()
        upload_collections = [c for c in all_collections if c.startswith(('zomato', 'trm', 'pos_', 'mpr_', 'uploaded_sheets'))]
        
        if not upload_collections:
            upload_collections = ['uploaded_sheets']  # Fallback to default
        
        total_count = 0
        total_processing = 0
        total_completed = 0
        total_failed = 0
        
        print("Collections Found:")
        for coll_name in upload_collections:
            collection = db[coll_name]
            count = collection.count_documents({})
            if count > 0:
                print(f"  - {coll_name}: {count} documents")
                total_count += count
                
                processing = collection.count_documents({"status": "processing"})
                completed = collection.count_documents({"status": "completed"})
                failed = collection.count_documents({"status": "failed"})
                
                total_processing += processing
                total_completed += completed
                total_failed += failed
        
        print()
        print(f"Total Uploads (all collections): {total_count}")
        print(f"  - Processing: {total_processing}")
        print(f"  - Completed: {total_completed}")
        print(f"  - Failed: {total_failed}")
        
        # Count by datasource across all collections
        print()
        print("By Datasource (across all collections):")
        datasource_counts = {}
        for coll_name in upload_collections:
            collection = db[coll_name]
            pipeline = [
                {"$group": {
                    "_id": "$datasource",
                    "count": {"$sum": 1}
                }},
                {"$sort": {"count": -1}}
            ]
            
            for item in collection.aggregate(pipeline):
                ds = item['_id'] or 'UNKNOWN'
                datasource_counts[ds] = datasource_counts.get(ds, 0) + item['count']
        
        for ds, count in sorted(datasource_counts.items(), key=lambda x: x[1], reverse=True):
            collection_name = get_collection_name(ds)
            print(f"  - {ds} (collection: {collection_name}): {count}")
        
        # Database info
        print()
        print(f"Database: {db.name}")
        print(f"Upload Collections: {', '.join(upload_collections)}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error getting stats: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_sample_data():
    """Show sample data from most recent upload"""
    print("=" * 60)
    print("Sample Data from Most Recent Upload")
    print("=" * 60)
    print()
    
    try:
        uploads = list_uploads(limit=1)
        
        if not uploads:
            print("No uploads found")
            return False
        
        upload = uploads[0]
        raw_data = upload.get('raw_data', [])
        
        if not raw_data:
            print("No raw data stored")
            return False
        
        print(f"Upload ID: {upload.get('upload_id')}")
        print(f"Filename: {upload.get('filename')}")
        print(f"Total Rows: {len(raw_data)}")
        print()
        print("First 3 rows of raw data:")
        print()
        
        for i, row in enumerate(raw_data[:3], 1):
            print(f"Row {i}:")
            for key, value in list(row.items())[:5]:  # Show first 5 columns
                print(f"  {key}: {value}")
            if len(row) > 5:
                print(f"  ... and {len(row) - 5} more columns")
            print()
        
        return True
        
    except Exception as e:
        print(f"❌ Error showing sample: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all tests"""
    print("\n" + "=" * 60)
    print("MongoDB Upload Verification Test")
    print("=" * 60)
    print()
    print(f"Database: {config.mongodb.database}")
    print(f"Collection: uploaded_sheets")
    print()
    
    # Test 1: Check recent uploads
    has_uploads = test_recent_uploads()
    
    if has_uploads:
        # Test 2: Collection stats
        print()
        test_collection_stats()
        
        # Test 3: Sample data
        print()
        test_sample_data()
    
    print()
    print("=" * 60)
    print("✅ Test Complete!")
    print("=" * 60)
    print()
    print("To view in MongoDB Compass:")
    print(f"  1. Connect to: mongodb://localhost:27017/")
    print(f"  2. Open database: {config.mongodb.database}")
    print(f"  3. Open collection:")
    print(f"     - 'zomato' for ZOMATO uploads")
    print(f"     - 'trm' for TRM uploads")
    print(f"     - 'pos_orders' for POS_ORDERS uploads")
    print(f"     - 'uploaded_sheets' for uploads without datasource")
    print()


if __name__ == "__main__":
    main()

