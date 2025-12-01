# Uploader Flow Analysis

## Current Upload Flow

### Entry Points
1. **`/api/upload`** - Main upload endpoint with datasource parameter
2. **`/api/upload-data`** - Direct upload with table name
3. **`/devyani-service/api/upload`** - Devyani-style upload endpoint

### Current Process Flow

```
1. File Upload (FastAPI UploadFile)
   ↓
2. Extract Headers (extract_headers)
   ↓
3. Column Mapping (AI or forced mappings)
   ↓
4. Extract File Data (pandas DataFrame)
   ↓
5. Validate Data (validation_service)
   ↓
6. Upload to MySQL Database
   ↓
7. Archive File (processed/failed folder)
```

### Key Components

#### 1. Routes (`app/routes.py`)
- `upload_by_datasource()` - Main upload handler
- Handles datasource lookup, column mapping, file processing

#### 2. Upload Service (`app/services/upload_service.py`)
- `DataUploader.process_upload()` - Main processing method
- `_extract_file_data()` - Reads file into pandas DataFrame
- `_validate_upload_data()` - Validates before upload
- `_upload_to_database()` - Inserts into MySQL

#### 3. Devyani Routes (`app/routes_devyani.py`)
- `_upload_data_impl()` - Devyani-style upload
- Saves files first, then processes in background

### Data Flow

```
UploadFile → pandas DataFrame → MySQL Table
```

## Proposed MongoDB Integration

### New Flow with MongoDB

```
1. File Upload
   ↓
2. **SAVE TO MONGODB** (metadata + raw data)
   ↓
3. Extract Headers
   ↓
4. Column Mapping
   ↓
5. Extract File Data (DataFrame)
   ↓
6. **SAVE PROCESSED DATA TO MONGODB** (optional)
   ↓
7. Validate Data
   ↓
8. Upload to MySQL
   ↓
9. **UPDATE MONGODB** (upload status, results)
   ↓
10. Archive File
```

### MongoDB Collections Needed

#### 1. `uploaded_sheets` - Store incoming sheet metadata and raw data
```json
{
  "_id": ObjectId,
  "upload_id": "unique_upload_id",
  "filename": "file.xlsx",
  "datasource": "ZOMATO",
  "table_name": "zomato",
  "file_size": 1024000,
  "file_type": "xlsx",
  "uploaded_at": ISODate,
  "uploaded_by": "username",
  "status": "processing|completed|failed",
  "raw_data": [...],  // Raw sheet data as JSON
  "headers": [...],
  "row_count": 1000,
  "column_mapping": {...},
  "validation_results": {...},
  "mysql_upload_results": {...},
  "error": null
}
```

#### 2. `processed_data` - Store processed/transformed data (optional)
```json
{
  "_id": ObjectId,
  "upload_id": "reference_to_uploaded_sheets",
  "table_name": "zomato",
  "processed_at": ISODate,
  "data": [...],  // Processed DataFrame as JSON
  "mapping_applied": {...}
}
```

### Benefits

1. **Audit Trail**: Complete history of all uploads
2. **Data Recovery**: Can reprocess from MongoDB if MySQL fails
3. **Analytics**: Track upload patterns, success rates
4. **Debugging**: Full context of what was uploaded
5. **Reprocessing**: Re-run uploads without original file

## Implementation Plan

### Step 1: Add MongoDB Configuration
- Add MongoDB config to `app/core/config.py`
- Use same pattern as Backend MongoDB config

### Step 2: Create MongoDB Service
- `app/services/mongodb_service.py`
- Functions to save/retrieve upload records

### Step 3: Integrate into Upload Flow
- Modify `upload_service.py` to save to MongoDB
- Save before processing, update after completion

### Step 4: Add Indexes
- Index on `upload_id`, `datasource`, `uploaded_at`, `status`

