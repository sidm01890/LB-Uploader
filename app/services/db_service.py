from sqlalchemy import text
from app.db import get_mysql_connection

def get_table_columns(table_name: str):
    """
    Fetch column metadata (column name, data type) for a MySQL table.
    Returns list of dicts.
    """
    engine = get_mysql_connection()
    query = text("""
        SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_KEY
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = :db AND TABLE_NAME = :table
        ORDER BY ORDINAL_POSITION
    """)
    with engine.connect() as conn:
        result = conn.execute(query, {"db": engine.url.database, "table": table_name})
        columns = [dict(row._mapping) for row in result]
    return columns

