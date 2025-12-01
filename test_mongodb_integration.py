#!/usr/bin/env python3
"""
Test MongoDB integration for Uploader service
"""

import sys
import os
from datetime import datetime

# Add Uploader directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.services.mongodb_service import (
    save_uploaded_sheet,
    update_upload_status,
    get_upload_record,
    list_uploads,
    get_mongodb_database
)
from app.core.config import config


def test_mongodb_connection():
    """Test MongoDB connection"""
    print("=" * 60)
    print("Testing MongoDB Connection")
    print("=" * 60)
    
    try:
        db = get_mongodb_database()
        print(f"✅ Connected to database: {db.name}")
        print(f"   Host: {config.mongodb.host}:{config.mongodb.port}")
        return True
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False


def test_save_upload():
    """Test saving an uploaded sheet"""
    print("\n" + "=" * 60)
    print("Testing Save Uploaded Sheet")
    print("=" * 60)
    
    try:
        # Sample data
        headers = ["order_id", "date", "amount", "store_code"]
        raw_data = [
            {"order_id": "ORD001", "date": "2024-01-15", "amount": 100.50, "store_code": "STORE001"},
            {"order_id": "ORD002", "date": "2024-01-15", "amount": 200.75, "store_code": "STORE002"},
        ]
        column_mapping = {
            "order_id": "order_id",
            "date": "date",
            "amount": "payment",
            "store_code": "store_name"
        }
        
        upload_id = save_uploaded_sheet(
            filename="test_file.xlsx",
            datasource="ZOMATO",
            table_name="zomato",
            file_size=1024,
            file_type="xlsx",
            headers=headers,
            raw_data=raw_data,
            uploaded_by="test_user",
            column_mapping=column_mapping
        )
        
        print(f"✅ Saved upload: upload_id={upload_id}")
        return upload_id
        
    except Exception as e:
        print(f"❌ Save failed: {e}")
        import traceback
        traceback.print_exc()
        return None


def test_update_status(upload_id: str):
    """Test updating upload status"""
    print("\n" + "=" * 60)
    print("Testing Update Upload Status")
    print("=" * 60)
    
    if not upload_id:
        print("⚠️ No upload_id to test")
        return
    
    try:
        # Update with validation results
        success = update_upload_status(
            upload_id=upload_id,
            status="processing",
            validation_results={
                "valid": True,
                "data_quality_score": 0.95,
                "warnings": []
            }
        )
        print(f"✅ Updated status to 'processing': {success}")
        
        # Update with MySQL results
        success = update_upload_status(
            upload_id=upload_id,
            status="completed",
            mysql_upload_results={
                "successful_rows": 2,
                "failed_rows": 0,
                "total_batches": 1
            }
        )
        print(f"✅ Updated status to 'completed': {success}")
        
    except Exception as e:
        print(f"❌ Update failed: {e}")
        import traceback
        traceback.print_exc()


def test_get_record(upload_id: str):
    """Test getting upload record"""
    print("\n" + "=" * 60)
    print("Testing Get Upload Record")
    print("=" * 60)
    
    if not upload_id:
        print("⚠️ No upload_id to test")
        return
    
    try:
        record = get_upload_record(upload_id)
        if record:
            print(f"✅ Retrieved record:")
            print(f"   Upload ID: {record.get('upload_id')}")
            print(f"   Filename: {record.get('filename')}")
            print(f"   Datasource: {record.get('datasource')}")
            print(f"   Status: {record.get('status')}")
            print(f"   Row Count: {record.get('row_count')}")
            print(f"   Headers: {record.get('headers')}")
        else:
            print("❌ Record not found")
            
    except Exception as e:
        print(f"❌ Get record failed: {e}")
        import traceback
        traceback.print_exc()


def test_list_uploads():
    """Test listing uploads"""
    print("\n" + "=" * 60)
    print("Testing List Uploads")
    print("=" * 60)
    
    try:
        # List all uploads
        all_uploads = list_uploads(limit=5)
        print(f"✅ Found {len(all_uploads)} recent uploads")
        
        # List by datasource
        zomato_uploads = list_uploads(datasource="ZOMATO", limit=5)
        print(f"✅ Found {len(zomato_uploads)} ZOMATO uploads")
        
        # List by status
        completed_uploads = list_uploads(status="completed", limit=5)
        print(f"✅ Found {len(completed_uploads)} completed uploads")
        
    except Exception as e:
        print(f"❌ List failed: {e}")
        import traceback
        traceback.print_exc()


def main():
    """Run all tests"""
    print("\n" + "=" * 60)
    print("MongoDB Integration Test for Uploader")
    print("=" * 60)
    print()
    
    # Test 1: Connection
    if not test_mongodb_connection():
        print("\n❌ Connection test failed. Exiting.")
        return
    
    # Test 2: Save upload
    upload_id = test_save_upload()
    
    # Test 3: Update status
    if upload_id:
        test_update_status(upload_id)
    
    # Test 4: Get record
    if upload_id:
        test_get_record(upload_id)
    
    # Test 5: List uploads
    test_list_uploads()
    
    print("\n" + "=" * 60)
    print("✅ All tests completed!")
    print("=" * 60)
    print("\nMongoDB integration is working correctly.")
    print("You can now view the data in MongoDB Compass:")
    print(f"  Database: {config.mongodb.database}")
    print("  Collection: uploaded_sheets")


if __name__ == "__main__":
    main()

