#!/usr/bin/env python3
"""
Script to check documents in MongoDB collections
"""

import sys
import os
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from app.core.config import config

# Collections to check
COLLECTIONS = [
    "collection_field_mappings",
    "formulas",
    "pos",
    "pos_backup",
    "pos_processed",
    "raw_data_collection",
    "uploaded_files",
    "zomato",
    "zomato_backup",
    "zomato_processed"
]


def check_collections():
    """Check document counts and show sample documents for each collection"""
    try:
        # Connect to MongoDB
        connection_string = config.mongodb.get_connection_string()
        print(f"🔌 Connecting to MongoDB: {config.mongodb.host}:{config.mongodb.port}/{config.mongodb.database}")
        
        client = MongoClient(connection_string)
        client.admin.command('ping')
        db = client[config.mongodb.database]
        
        print("✅ MongoDB connection established successfully\n")
        print("=" * 80)
        print("COLLECTION DOCUMENT COUNTS")
        print("=" * 80)
        
        results = {}
        
        for collection_name in COLLECTIONS:
            try:
                collection = db[collection_name]
                count = collection.count_documents({})
                results[collection_name] = count
                
                # Show count
                print(f"\n📊 Collection: {collection_name}")
                print(f"   Document Count: {count:,}")
                
                # Show sample document if count > 0
                if count > 0:
                    sample = collection.find_one()
                    if sample:
                        print(f"   Sample Document Keys: {list(sample.keys())}")
                        # Show first few fields (excluding _id)
                        sample_fields = {k: v for k, v in list(sample.items())[:5] if k != '_id'}
                        if sample_fields:
                            print(f"   Sample Fields: {sample_fields}")
                else:
                    print(f"   ⚠️  Collection is empty")
                    
            except Exception as e:
                print(f"\n❌ Error checking collection '{collection_name}': {e}")
                results[collection_name] = None
        
        print("\n" + "=" * 80)
        print("SUMMARY")
        print("=" * 80)
        print("\nCollection Name                    | Document Count")
        print("-" * 60)
        for collection_name, count in results.items():
            if count is not None:
                print(f"{collection_name:35} | {count:>15,}")
            else:
                print(f"{collection_name:35} | {'ERROR':>15}")
        
        # Close connection
        client.close()
        
    except ConnectionFailure as e:
        print(f"❌ Failed to connect to MongoDB: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    check_collections()

