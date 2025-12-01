# Datasource-Specific MongoDB Collections

## Overview

When files are uploaded from the frontend with a datasource parameter, they are automatically saved to datasource-specific MongoDB collections.

## Collection Naming

Collections are named based on the datasource parameter:
- **ZOMATO** → `zomato` collection
- **TRM** → `trm` collection
- **POS_ORDERS** → `pos_orders` collection
- **MPR** → `mpr` collection
- **Unknown/No datasource** → `uploaded_sheets` (default collection)

## Examples

### ZOMATO Upload
```python
# Frontend sends: datasource=ZOMATO
# File is saved to: zomato collection
```

### TRM Upload
```python
# Frontend sends: datasource=TRM
# File is saved to: trm collection
```

## Collection Structure

All collections have the same structure:

```json
{
  "_id": ObjectId,
  "upload_id": "unique-uuid",
  "filename": "file.xlsx",
  "datasource": "ZOMATO",
  "table_name": "zomato",
  "file_size": 1024000,
  "file_type": "xlsx",
  "uploaded_at": ISODate,
  "uploaded_by": "username",
  "status": "processing|completed|failed",
  "headers": [...],
  "row_count": 1000,
  "raw_data": [...],  // Full sheet data
  "column_mapping": {...},
  "validation_results": {...},
  "mysql_upload_results": {...},
  "error": null
}
```

## Benefits

1. **Organized Data**: Each datasource has its own collection
2. **Easy Querying**: Query ZOMATO uploads from `zomato` collection
3. **Scalability**: Separate collections for better performance
4. **Clean Separation**: Different datasources don't mix

## Querying

### Get all ZOMATO uploads
```python
from app.services.mongodb_service import list_uploads

zomato_uploads = list_uploads(datasource="ZOMATO")
# Searches in 'zomato' collection
```

### Get specific upload
```python
from app.services.mongodb_service import get_upload_record

upload = get_upload_record(upload_id, datasource="ZOMATO")
# Searches in 'zomato' collection
```

## MongoDB Compass

In MongoDB Compass, you'll see:
- `zomato` collection (for ZOMATO uploads)
- `trm` collection (for TRM uploads)
- `pos_orders` collection (for POS_ORDERS uploads)
- `uploaded_sheets` collection (for uploads without datasource)

## Collection Name Normalization

The datasource name is normalized to create a valid collection name:
- Converted to lowercase
- Spaces and special characters replaced with underscores
- Multiple underscores collapsed
- Leading/trailing underscores removed

Examples:
- `ZOMATO` → `zomato`
- `POS_ORDERS` → `pos_orders`
- `TRM` → `trm`
- `MPR-UPI` → `mpr_upi`


