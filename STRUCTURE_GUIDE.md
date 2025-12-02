# Project Structure Guide - Node.js Style

This project has been refactored to follow a **Node.js-style architecture** for easier understanding if you're coming from a Node.js/Express background.

## 📁 Directory Structure

```
app/
├── routes.py              # Routes (like Express routes) - Only defines endpoints
├── controllers/          # Controllers (like Express controllers) - Business logic
│   ├── __init__.py
│   ├── upload_controller.py
│   └── mapping_controller.py
├── services/             # Services (like Node.js services) - Data access & complex logic
│   ├── upload_service.py
│   ├── mapping_service.py
│   └── ...
└── main.py               # App entry point (like Express app.js)
```

## 🔄 Comparison: Node.js vs Python/FastAPI

### Node.js (Express) Structure:

```javascript
// routes/upload.js
const express = require("express");
const router = express.Router();
const uploadController = require("../controllers/uploadController");

router.post("/upload", uploadController.uploadFile);

module.exports = router;
```

```javascript
// controllers/uploadController.js
const uploadService = require("../services/uploadService");

exports.uploadFile = async (req, res) => {
  try {
    const result = await uploadService.processUpload(req.file);
    res.json(result);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};
```

### Python/FastAPI Structure (Now Similar!):

```python
# routes.py
from fastapi import APIRouter, UploadFile
from app.controllers.upload_controller import UploadController

router = APIRouter()
upload_controller = UploadController()

@router.post("/upload")
async def upload_file(file: UploadFile):
    return await upload_controller.upload_file(file)
```

```python
# controllers/upload_controller.py
from app.services.upload_service import uploader

class UploadController:
    async def upload_file(self, file):
        try:
            result = await uploader.process_upload(file)
            return result
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
```

## 📋 Layer Responsibilities

### 1. **Routes** (`routes.py`)

- ✅ Define HTTP endpoints (GET, POST, PUT, DELETE)
- ✅ Handle request/response format (FastAPI decorators)
- ✅ Call controller methods
- ❌ NO business logic
- ❌ NO data processing

**Example:**

```python
@router.post("/upload")
async def upload_file(file: UploadFile):
    # Just call the controller - that's it!
    return await upload_controller.upload_file(file)
```

### 2. **Controllers** (`controllers/`)

- ✅ Handle request/response logic
- ✅ Validate input parameters
- ✅ Call services
- ✅ Format responses
- ❌ NO direct database access
- ❌ NO complex business logic (delegate to services)

**Example:**

```python
class UploadController:
    async def upload_file(self, file: UploadFile):
        # Validate input
        if not file:
            raise HTTPException(status_code=400, detail="File required")

        # Call service
        result = await upload_service.process_upload(file)

        # Format response
        return {
            "success": True,
            "data": result
        }
```

### 3. **Services** (`services/`)

- ✅ Business logic
- ✅ Data processing
- ✅ Database operations
- ✅ External API calls
- ✅ Complex algorithms

**Example:**

```python
class UploadService:
    async def process_upload(self, file):
        # Complex business logic here
        data = await self._extract_data(file)
        validated = await self._validate(data)
        result = await self._save_to_database(validated)
        return result
```

## 🎯 Key Differences from Node.js

### 1. **Class-based Controllers**

- **Node.js**: Usually functions (`exports.uploadFile = async (req, res) => {}`)
- **Python**: Classes with methods (`class UploadController: async def upload_file()`)

### 2. **Async/Await**

- Both use async/await, but Python uses `async def` instead of `async function`

### 3. **Error Handling**

- **Node.js**: `res.status(500).json({ error })`
- **Python**: `raise HTTPException(status_code=500, detail="error")`

### 4. **Request/Response**

- **Node.js**: `req.body`, `req.file`, `res.json()`
- **Python**: FastAPI automatically parses request body, returns dict as JSON

## 📝 Example Flow

### Request Flow:

```
1. HTTP Request → routes.py
2. routes.py → controllers/upload_controller.py
3. controllers → services/upload_service.py
4. services → database/external APIs
5. Response flows back: services → controllers → routes → HTTP Response
```

### Example: Upload File

**1. Route** (`routes.py`):

```python
@router.post("/upload")
async def upload(file: UploadFile):
    return await upload_controller.upload_file(file)
```

**2. Controller** (`controllers/upload_controller.py`):

```python
class UploadController:
    async def upload_file(self, file: UploadFile):
        # Validate
        if not file.filename:
            raise HTTPException(status_code=400, detail="No file")

        # Call service
        result = await upload_service.process_upload(file)
        return result
```

**3. Service** (`services/upload_service.py`):

```python
class UploadService:
    async def process_upload(self, file):
        # Extract data
        data = await self._extract_file_data(file)

        # Validate
        validated = await validator.validate(data)

        # Save to database
        saved = await self._save_to_database(validated)

        return saved
```

## ✅ Benefits of This Structure

1. **Separation of Concerns**: Each layer has a clear responsibility
2. **Testability**: Easy to test controllers and services independently
3. **Maintainability**: Changes in one layer don't affect others
4. **Familiarity**: Node.js developers feel at home
5. **Scalability**: Easy to add new endpoints/features

## 🚀 Adding New Features

### To add a new endpoint:

1. **Add route** in `routes.py`:

```python
@router.get("/new-endpoint")
async def new_endpoint(param: str):
    return await controller.new_method(param)
```

2. **Add controller method** in `controllers/`:

```python
class Controller:
    async def new_method(self, param: str):
        # Handle request logic
        result = await service.do_something(param)
        return result
```

3. **Add service method** in `services/` (if needed):

```python
class Service:
    async def do_something(self, param: str):
        # Business logic here
        return {"result": "success"}
```

## 📚 Additional Notes

- **Models**: Use Pydantic models for request/response validation (similar to Joi/Zod in Node.js)
- **Middleware**: Defined in `main.py` (similar to Express middleware)
- **Error Handling**: Use FastAPI's `HTTPException` (similar to Express error handlers)

## 🔍 Quick Reference

| Node.js           | Python/FastAPI                         |
| ----------------- | -------------------------------------- |
| `router.post()`   | `@router.post()`                       |
| `req.body`        | Function parameter                     |
| `res.json()`      | Return dict                            |
| `res.status(500)` | `raise HTTPException(status_code=500)` |
| `async function`  | `async def`                            |
| `module.exports`  | Class methods                          |
| `require()`       | `from ... import`                      |

---

**Happy Coding!** 🎉
