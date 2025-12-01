#!/usr/bin/env python3
"""
Test script to upload Zomato file and verify MongoDB storage
"""

import sys
import os
import asyncio
from pathlib import Path

# Add Uploader directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from fastapi import UploadFile
from app.services.upload_service import uploader
from app.services.mongodb_service import (
    get_upload_record,
    list_uploads,
    get_uploaded_sheets_collection,
    get_collection_name
)
from app.core.config import config


async def test_zomato_upload():
    """Test uploading Zomato file and verify MongoDB storage"""
    print("=" * 60)
    print("Testing Zomato File Upload to MongoDB")
    print("=" * 60)
    print()
    
    file_path = "/Users/siddharthmishra/Downloads/Zomato_Devyani_Store141.xlsx"
    
    # Check if file exists
    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        return False
    
    print(f"📁 File: {file_path}")
    print(f"   Size: {os.path.getsize(file_path)} bytes")
    print()
    
    try:
        # Read file
        with open(file_path, 'rb') as f:
            file_content = f.read()
        
        # Create UploadFile object
        file = UploadFile(
            filename="Zomato_Devyani_Store141.xlsx",
            file=open(file_path, 'rb')
        )
        
        # Define column mapping (simplified - you may need to adjust)
        # This is a basic mapping - the actual upload will use AI mapping
        column_mapping = {
            # Add your column mappings here if needed
            # For now, let's use empty mapping and let AI handle it
        }
        
        print("🚀 Starting upload...")
        print("   Datasource: ZOMATO")
        print("   Table: zomato")
        print()
        
        # Process upload
        result = await uploader.process_upload(
            file=file,
            table_name="zomato",
            mapping=column_mapping,
            datasource="ZOMATO",
            uploaded_by="test_script"
        )
        
        print("=" * 60)
        print("Upload Result")
        print("=" * 60)
        print()
        
        if result.get("success"):
            print("✅ Upload successful!")
            upload_id = result.get("upload_id")
            print(f"   Upload ID: {upload_id}")
            print()
            
            summary = result.get("summary", {})
            print("Summary:")
            print(f"   Total Rows: {summary.get('total_rows', 0)}")
            print(f"   Successful Rows: {summary.get('successful_rows', 0)}")
            print(f"   Failed Rows: {summary.get('failed_rows', 0)}")
            print()
            
            # Verify MongoDB storage
            print("=" * 60)
            print("Verifying MongoDB Storage")
            print("=" * 60)
            print()
            
            if upload_id:
                # Get record from MongoDB
                record = get_upload_record(upload_id, datasource="ZOMATO")
                
                if record:
                    collection_name = get_collection_name("ZOMATO")
                    print(f"✅ Found in MongoDB collection: {collection_name}")
                    print()
                    print("Record Details:")
                    print(f"   Upload ID: {record.get('upload_id')}")
                    print(f"   Filename: {record.get('filename')}")
                    print(f"   Datasource: {record.get('datasource')}")
                    print(f"   Status: {record.get('status')}")
                    print(f"   Row Count: {record.get('row_count')}")
                    print(f"   Headers: {len(record.get('headers', []))} columns")
                    
                    if record.get('raw_data'):
                        print(f"   Raw Data: ✅ {len(record.get('raw_data'))} rows stored")
                    else:
                        print(f"   Raw Data: ❌ Not stored")
                    
                    if record.get('validation_results'):
                        print(f"   Validation: ✅ Results stored")
                    
                    if record.get('mysql_upload_results'):
                        mysql_results = record.get('mysql_upload_results', {})
                        print(f"   MySQL Upload: ✅ {mysql_results.get('successful_rows', 0)} rows uploaded")
                    
                    print()
                    print("First 3 rows of raw data:")
                    raw_data = record.get('raw_data', [])
                    for i, row in enumerate(raw_data[:3], 1):
                        print(f"   Row {i}: {list(row.keys())[:5]}...")  # Show first 5 column names
                    
                else:
                    print(f"❌ Record not found in MongoDB with upload_id: {upload_id}")
                    print("   Checking all collections...")
                    
                    # Check zomato collection
                    collection = get_uploaded_sheets_collection("ZOMATO")
                    count = collection.count_documents({})
                    print(f"   zomato collection has {count} documents")
                    
                    # List recent uploads
                    recent = list_uploads(datasource="ZOMATO", limit=5)
                    print(f"   Recent ZOMATO uploads: {len(recent)}")
            else:
                print("⚠️ No upload_id returned")
        else:
            print("❌ Upload failed!")
            print(f"   Error: {result.get('error')}")
        
        # Close file
        file.file.close()
        
        return result.get("success", False)
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run the test"""
    print()
    success = asyncio.run(test_zomato_upload())
    print()
    print("=" * 60)
    if success:
        print("✅ Test completed successfully!")
        print()
        print("Check MongoDB Compass:")
        print("  Database: devyani_mongo")
        print("  Collection: zomato")
    else:
        print("❌ Test failed!")
    print("=" * 60)
    print()


if __name__ == "__main__":
    main()

