#!/usr/bin/env python3
"""
Script to check formula documents in MongoDB and verify they match expected structure
"""

import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.services.mongodb_service import mongodb_service
from app.core.config import config
import json

def check_formula_documents():
    """Check formula documents in MongoDB"""
    
    print("🔍 Checking Formula Documents in MongoDB...")
    print("=" * 60)
    
    # Connect to MongoDB
    if not mongodb_service.is_connected():
        print("❌ MongoDB is not connected!")
        print(f"   Connection string: {config.mongodb.connection_string}")
        return
    
    print("✅ MongoDB connected")
    print()
    
    # Get formulas collection
    if not mongodb_service.db:
        print("❌ Database not available")
        return
    
    formulas_collection = mongodb_service.db.get_collection("formulas")
    
    # Count documents
    total_count = formulas_collection.count_documents({})
    print(f"📊 Total formula documents: {total_count}")
    print()
    
    if total_count == 0:
        print("⚠️ No formula documents found in 'formulas' collection")
        return
    
    # Get all documents
    all_docs = list(formulas_collection.find({}))
    
    print("📋 Formula Documents:")
    print("-" * 60)
    
    valid_docs = 0
    invalid_docs = 0
    
    for idx, doc in enumerate(all_docs, 1):
        doc_id = doc.get('_id')
        report_name = doc.get('report_name')
        formulas = doc.get('formulas', [])
        mapping_keys = doc.get('mapping_keys', {})
        conditions = doc.get('conditions', {})
        
        print(f"\n📄 Document {idx} (ID: {doc_id}):")
        print(f"   report_name: {report_name}")
        print(f"   formulas: {len(formulas) if isinstance(formulas, list) else 'NOT A LIST'} formula(s)")
        print(f"   mapping_keys: {len(mapping_keys) if isinstance(mapping_keys, dict) else 'NOT A DICT'} collection(s)")
        print(f"   conditions: {len(conditions) if isinstance(conditions, dict) else 'NOT A DICT'} collection(s)")
        
        # Validate structure
        is_valid = True
        issues = []
        
        if not report_name:
            is_valid = False
            issues.append("❌ Missing 'report_name' field")
        elif not isinstance(report_name, str):
            is_valid = False
            issues.append(f"❌ 'report_name' is not a string: {type(report_name)}")
        
        if not formulas:
            is_valid = False
            issues.append("❌ Missing or empty 'formulas' field")
        elif not isinstance(formulas, list):
            is_valid = False
            issues.append(f"❌ 'formulas' is not a list: {type(formulas)}")
        elif len(formulas) == 0:
            is_valid = False
            issues.append("❌ 'formulas' is an empty list")
        
        if issues:
            print("   ⚠️ Issues found:")
            for issue in issues:
                print(f"      {issue}")
            invalid_docs += 1
        else:
            print("   ✅ Document structure is valid")
            valid_docs += 1
            
            # Show formula details
            if formulas:
                print(f"   📝 Formulas:")
                for f_idx, formula in enumerate(formulas[:5], 1):  # Show first 5
                    logic_name_key = formula.get('logicNameKey', 'N/A')
                    formula_text = formula.get('formulaText', 'N/A')
                    print(f"      {f_idx}. {logic_name_key} = {formula_text}")
                if len(formulas) > 5:
                    print(f"      ... and {len(formulas) - 5} more formula(s)")
    
    print()
    print("=" * 60)
    print(f"📊 Summary:")
    print(f"   ✅ Valid documents: {valid_docs}")
    print(f"   ❌ Invalid documents: {invalid_docs}")
    print(f"   📋 Total documents: {total_count}")
    print()
    
    if invalid_docs > 0:
        print("⚠️ Some documents have issues. The scheduler will skip them.")
        print("   Fix the document structure to enable formula processing.")
    elif valid_docs > 0:
        print("✅ All documents are valid! The scheduler should process them.")
    else:
        print("⚠️ No valid documents found.")

if __name__ == "__main__":
    try:
        check_formula_documents()
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

