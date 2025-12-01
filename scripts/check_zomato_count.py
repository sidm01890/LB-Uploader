#!/usr/bin/env python3
"""Quick script to check Zomato table row count"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from app.core.database import db_manager

try:
    with db_manager.get_mysql_connector() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM zomato")
        count = cursor.fetchone()[0]
        print(f"✅ Zomato table row count: {count:,}")
        cursor.close()
except Exception as e:
    print(f"❌ Error: {e}")

