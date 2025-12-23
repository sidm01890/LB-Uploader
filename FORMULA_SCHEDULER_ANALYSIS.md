# Formula Scheduler Analysis

## 🔍 Problem Summary

The formula scheduler is **running successfully** but processing **0 documents** despite:
- ✅ Scheduler started correctly
- ✅ Formula document found in MongoDB (`zomato_vs_pos` with 1 formula)
- ✅ Source collection `zomato_processed` has **788,224 documents**
- ❌ **0 documents processed** in the formula calculation job

## 📊 Current Status

### What's Working:
1. **Scheduler Initialization**: ✅
   - Scheduler starts on application startup
   - Formula calculation job is registered
   - Runs every 2 hours (first run after 1 minute)

2. **Formula Discovery**: ✅
   - Successfully reads from `formulas` collection
   - Finds formula document: `zomato_vs_pos`
   - Parses formula: `NET_AMOUNT = zomato.bill_subtotal - zomato.mvd + zomato.merchant_pack_charge`
   - Identifies source collection: `zomato_processed`

3. **Collection Access**: ✅
   - Can access `zomato_processed` collection
   - Document count: 788,224 documents
   - Documents have required fields (`bill_subtotal`, `mvd`, `merchant_pack_charge`)

### What's NOT Working:
1. **Document Processing**: ❌
   - **0 documents processed** despite 788K documents available
   - Log shows: `✅ Formula calculation job completed: 1 report(s) processed, 0 document(s) processed`

## 🐛 Root Cause Analysis

### The Problem: Empty `mapping_keys`

**Location**: `app/controllers/scheduled_jobs_controller.py`

**Flow**:
1. Line 1301: `mapping_keys = formula_doc.get('mapping_keys', {}) or {}`
   - Current MongoDB document has `mapping_keys: None`
   - This becomes an empty dict `{}`

2. Line 1490: `mapping_key_fields = mapping_keys.get(base_name, []) or []`
   - `base_name = "zomato"` (from `zomato_processed`)
   - `mapping_keys.get("zomato", [])` returns `[]` (empty list)
   - Result: `mapping_key_fields = []`

3. Line 1527: `mapping_key_value = self._build_mapping_key(doc, mapping_key_fields)`
   - Called with empty list: `_build_mapping_key(doc, [])`

4. Line 973-974 in `_build_mapping_key`:
   ```python
   if not key_fields:
       return None  # ← Returns None when mapping_keys is empty!
   ```

5. Line 1528-1529:
   ```python
   if mapping_key_value is None:
       continue  # ← Skips ALL documents!
   ```

**Result**: **ALL 788,224 documents are skipped** because `mapping_key_value` is always `None` when `mapping_keys` is empty.

## 📋 Code Comparison: `vikas_dev` vs `main`

### Finding:
- **No differences** between `vikas_dev` and `main` branches for:
  - `app/controllers/scheduled_jobs_controller.py`
  - `app/main.py`

Both branches have the **same code** with the same bug.

## 🔧 Required Fixes

### Fix 1: Handle Empty `mapping_keys`

When `mapping_keys` is empty/None, we need a fallback strategy:

**Option A**: Use document `_id` as mapping key
```python
if mapping_key_value is None:
    # Fallback: use _id if mapping_keys not provided
    doc_id = doc.get('_id')
    if doc_id:
        mapping_key_value = str(doc_id)
    else:
        continue  # Skip if no _id
```

**Option B**: Use a default field combination
```python
if mapping_key_value is None:
    # Fallback: try to use unique_id if available
    mapping_key_value = doc.get('unique_id')
    if not mapping_key_value:
        # Last resort: use _id
        doc_id = doc.get('_id')
        mapping_key_value = str(doc_id) if doc_id else None
    if mapping_key_value is None:
        continue
```

**Option C**: Generate a hash-based key
```python
if mapping_key_value is None:
    # Fallback: generate a hash from document content
    import hashlib
    doc_str = str(sorted(doc.items()))
    mapping_key_value = hashlib.md5(doc_str.encode()).hexdigest()
```

### Fix 2: Update `_build_mapping_key` to Handle Empty Keys

Modify `_build_mapping_key` to return a default value instead of `None`:

```python
def _build_mapping_key(self, document: Dict[str, Any], key_fields: List[str]) -> Optional[str]:
    """
    Build a composite mapping key from the given document using provided fields.
    Returns None if any field is missing or empty.
    If key_fields is empty, returns a default key (document _id or unique_id).
    """
    if not key_fields:
        # Fallback: use unique_id or _id if no mapping keys specified
        return document.get('unique_id') or str(document.get('_id', ''))
    
    # ... rest of existing logic
```

### Fix 3: Add Logging for Debugging

Add logging to understand why documents are skipped:

```python
mapping_key_value = self._build_mapping_key(doc, mapping_key_fields)
if mapping_key_value is None:
    if batch_number == 1 and len(batch_docs) == 0:
        logger.warning(
            f"⚠️ Skipping document: mapping_key_fields={mapping_key_fields}, "
            f"doc has unique_id={doc.get('unique_id')}, _id={doc.get('_id')}"
        )
    continue
```

## 🎯 Expected Behavior After Fix

1. **When `mapping_keys` is provided**:
   - Use specified fields to build mapping key
   - Process documents normally

2. **When `mapping_keys` is empty/None**:
   - Use fallback strategy (e.g., `unique_id` or `_id`)
   - Process all documents instead of skipping them
   - Create/update documents in target collection (`zomato_vs_pos`)

3. **Result**:
   - All 788,224 documents should be processed
   - Formula calculations should execute for each document
   - Results should be saved to `zomato_vs_pos` collection

## 📝 Additional Observations

### Current Formula Document Structure:
```json
{
  "_id": "694234f65cad54887e9faf0f",
  "report_name": "zomato_vs_pos",
  "formulas": [
    {
      "logicNameKey": "NET_AMOUNT",
      "formulaText": "zomato.bill_subtotal - zomato.mvd + zomato.merchant_pack_charge"
    }
  ],
  "mapping_keys": null,  // ← This is the problem!
  "conditions": null
}
```

### What Should Happen:
1. Read all documents from `zomato_processed`
2. For each document, evaluate formula: `bill_subtotal - mvd + merchant_pack_charge`
3. Save result to `zomato_vs_pos` collection with field `net_amount`
4. Use a mapping key to identify documents (even if it's just `_id`)

### Current Behavior:
1. ✅ Reads documents from `zomato_processed`
2. ❌ Skips ALL documents because `mapping_key_value` is `None`
3. ❌ No documents saved to `zomato_vs_pos`

## 🚀 Implementation Priority

1. **HIGH**: Fix `_build_mapping_key` to handle empty `mapping_keys`
2. **HIGH**: Add fallback logic when `mapping_key_value` is `None`
3. **MEDIUM**: Add detailed logging for debugging
4. **LOW**: Update documentation about `mapping_keys` requirement

## ✅ Verification Steps After Fix

1. Check logs for: `✅ Formula calculation job completed: 1 report(s) processed, 788,224 document(s) processed`
2. Verify `zomato_vs_pos` collection is created
3. Verify documents in `zomato_vs_pos` have `net_amount` field calculated
4. Check that formula values match expected calculations

