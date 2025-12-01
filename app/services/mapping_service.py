import json
import logging
import re
from typing import List, Dict, Any, Optional
from datetime import datetime, date

logger = logging.getLogger(__name__)

# Try to import OpenAI - make it optional
try:
    import openai
    from app.config import OPENAI_API_KEY, MODEL_NAME
    # Initialize OpenAI client
    client = openai.OpenAI(api_key=OPENAI_API_KEY)
    OPENAI_AVAILABLE = True
except (ImportError, Exception) as e:
    logger.warning(f"OpenAI not available: {e}. AI mapping features will be disabled.")
    client = None
    OPENAI_AVAILABLE = False
    OPENAI_API_KEY = None
    MODEL_NAME = None

def convert_to_json_serializable(obj: Any) -> Any:
    """
    Convert non-JSON-serializable objects (like Timestamp, datetime) to strings.
    Recursively processes dictionaries and lists.
    """
    # Check for pandas Timestamp (without importing pandas directly)
    if hasattr(obj, '__class__') and obj.__class__.__name__ == 'Timestamp':
        return str(obj)
    elif isinstance(obj, (datetime, date)):
        return str(obj)
    elif isinstance(obj, dict):
        return {key: convert_to_json_serializable(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_to_json_serializable(item) for item in obj]
    else:
        return obj

async def suggest_mapping(file_headers: List[str], db_columns: List[Dict], sample_data: Optional[List[Dict]] = None) -> Dict[str, Any]:
    """
    Use GPT model to intelligently map uploaded Excel/CSV headers to MySQL table columns.
    Enhanced with sample data analysis and multiple fallback strategies.
    """
    try:
        db_field_names = [col["COLUMN_NAME"] for col in db_columns]
        db_field_types = {col["COLUMN_NAME"]: col["DATA_TYPE"] for col in db_columns}
        
        # Create enhanced prompt with sample data context
        prompt = create_enhanced_prompt(file_headers, db_columns, sample_data)
        
        # Try primary AI mapping
        mapping = await get_ai_mapping(prompt)
        
        # Apply fallback strategies if AI mapping is incomplete
        if mapping and not mapping.get("error"):
            mapping = apply_fallback_mapping(mapping, file_headers, db_columns)
            
        # Validate and score the mapping
        mapping_score = calculate_mapping_confidence(mapping, file_headers, db_columns)
        
        return {
            "mapping": mapping,
            "confidence_score": mapping_score,
            "unmapped_headers": find_unmapped_headers(mapping, file_headers),
            "suggestions": generate_mapping_suggestions(mapping, file_headers, db_columns)
        }
        
    except Exception as e:
        logger.error(f"Error in AI mapping: {str(e)}")
        return {
            "mapping": {},
            "error": str(e),
            "confidence_score": 0,
            "fallback_mapping": create_fallback_mapping(file_headers, db_columns)
        }

def create_enhanced_prompt(file_headers: List[str], db_columns: List[Dict], sample_data: Optional[List[Dict]] = None) -> str:
    """
    Create an enhanced prompt with context about data types and sample values.
    """
    db_field_names = [col["COLUMN_NAME"] for col in db_columns]
    db_field_types = {col["COLUMN_NAME"]: col["DATA_TYPE"] for col in db_columns}
    
    prompt = f"""
You are an expert data mapping assistant. Your task is to map Excel/CSV column headers to MySQL database columns with high accuracy.

EXCEL/CSV HEADERS:
{json.dumps(file_headers, indent=2)}

MYSQL TABLE COLUMNS:
{json.dumps([{"name": col["COLUMN_NAME"], "type": col["DATA_TYPE"], "nullable": col["IS_NULLABLE"]} for col in db_columns], indent=2)}

MAPPING RULES:
1. Match columns semantically (meaning-based matching)
2. Consider common abbreviations and variations (e.g., "ID" = "id", "Name" = "name", "Email" = "email_address")
3. Handle different naming conventions (camelCase, snake_case, PascalCase)
4. Consider data types when making matches
5. If uncertain, return null for that header
6. Prioritize exact matches over fuzzy matches
7. Consider business context (e.g., "Customer Name" → "customer_name", "Amount" → "amount")

"""
    
    if sample_data:
        # Convert Timestamp/datetime objects to strings for JSON serialization
        serializable_sample_data = convert_to_json_serializable(sample_data[:3])
        prompt += f"""
SAMPLE DATA CONTEXT (first few rows):
{json.dumps(serializable_sample_data, indent=2)}

Use this sample data to better understand the content and make more accurate mappings.
"""
    
    prompt += """
RESPONSE FORMAT:
Return ONLY a valid JSON object with this exact structure:
{
  "mappings": {
    "ExcelHeader1": "mysql_column_name",
    "ExcelHeader2": "mysql_column_name",
    "ExcelHeader3": null
  },
  "confidence": {
    "ExcelHeader1": 0.95,
    "ExcelHeader2": 0.87,
    "ExcelHeader3": 0.0
  },
  "reasoning": {
    "ExcelHeader1": "Exact semantic match with clear business meaning",
    "ExcelHeader2": "Strong semantic match with minor naming differences",
    "ExcelHeader3": "No clear match found in database schema"
  }
}

IMPORTANT: Return ONLY the JSON object, no additional text or formatting.
"""
    
    return prompt

async def get_ai_mapping(prompt: str) -> Dict[str, Any]:
    """
    Get AI mapping with retry logic and error handling.
    """
    if not OPENAI_AVAILABLE or not client:
        return {"error": "OpenAI service not available"}
    
    try:
        response = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,  # Low temperature for consistent results
            max_tokens=2000
        )
        
        content = response.choices[0].message.content.strip()
        
        # Clean the response (remove markdown formatting if present)
        content = re.sub(r'```json\s*', '', content)
        content = re.sub(r'```\s*$', '', content)
        
        try:
            mapping = json.loads(content)
            return mapping
        except json.JSONDecodeError as e:
            logger.warning(f"JSON decode error: {e}, raw content: {content}")
            # Try to extract JSON from the response
            json_match = re.search(r'\{.*\}', content, re.DOTALL)
            if json_match:
                return json.loads(json_match.group())
            else:
                return {"error": "Invalid JSON response", "raw": content}
                
    except Exception as e:
        logger.error(f"OpenAI API error: {str(e)}")
        return {"error": f"AI service error: {str(e)}"}

def apply_fallback_mapping(mapping: Dict, file_headers: List[str], db_columns: List[Dict]) -> Dict:
    """
    Apply fallback mapping strategies for unmapped headers.
    """
    if not mapping or mapping.get("error"):
        return mapping
    
    mappings = mapping.get("mappings", {})
    db_field_names = [col["COLUMN_NAME"] for col in db_columns]
    
    # Apply fuzzy matching for unmapped headers
    for header in file_headers:
        if header not in mappings or mappings[header] is None:
            # Try fuzzy matching
            best_match = find_best_fuzzy_match(header, db_field_names)
            if best_match:
                mappings[header] = best_match
                logger.info(f"Fallback mapping: '{header}' → '{best_match}'")
    
    return mapping

def find_best_fuzzy_match(header: str, db_columns: List[str]) -> Optional[str]:
    """
    Find the best fuzzy match for a header using simple string similarity.
    More aggressive matching to catch common variations.
    """
    header_lower = header.lower().strip()
    header_normalized = header_lower.replace('_', '').replace('-', '').replace(' ', '').replace('.', '')
    
    best_match = None
    best_score = 0
    
    for db_col in db_columns:
        db_col_lower = db_col.lower().strip()
        db_col_normalized = db_col_lower.replace('_', '').replace('-', '').replace(' ', '').replace('.', '')
        
        # Exact match (case-insensitive)
        if header_lower == db_col_lower:
            return db_col
        
        # Normalized exact match
        if header_normalized == db_col_normalized:
            return db_col
        
        # Check if one contains the other (more lenient)
        if header_normalized in db_col_normalized or db_col_normalized in header_normalized:
            score = min(len(header_normalized), len(db_col_normalized)) / max(len(header_normalized), len(db_col_normalized))
            if score > best_score and score > 0.5:  # Lower threshold from 0.6 to 0.5
                best_score = score
                best_match = db_col
        
        # Check for common word matches (e.g., "Order ID" matches "order_id")
        header_words = set(header_lower.split())
        db_col_words = set(db_col_lower.split('_'))
        if header_words and db_col_words:
            common_words = header_words.intersection(db_col_words)
            if len(common_words) >= 1 and len(common_words) / max(len(header_words), len(db_col_words)) > 0.5:
                score = len(common_words) / max(len(header_words), len(db_col_words))
                if score > best_score:
                    best_score = score
                    best_match = db_col
    
    return best_match

def calculate_mapping_confidence(mapping: Dict, file_headers: List[str], db_columns: List[Dict]) -> float:
    """
    Calculate overall confidence score for the mapping.
    """
    if not mapping or mapping.get("error"):
        return 0.0
    
    mappings = mapping.get("mappings", {})
    confidence_scores = mapping.get("confidence", {})
    
    if not mappings:
        return 0.0
    
    total_headers = len(file_headers)
    mapped_headers = sum(1 for h in file_headers if h in mappings and mappings[h] is not None)
    
    # Base score from mapping coverage
    coverage_score = mapped_headers / total_headers if total_headers > 0 else 0
    
    # Average confidence from AI
    avg_confidence = sum(confidence_scores.values()) / len(confidence_scores) if confidence_scores else 0.5
    
    # Combined score
    final_score = (coverage_score * 0.7) + (avg_confidence * 0.3)
    
    return round(final_score, 3)

def find_unmapped_headers(mapping: Dict, file_headers: List[str]) -> List[str]:
    """
    Find headers that couldn't be mapped.
    """
    if not mapping or mapping.get("error"):
        return file_headers
    
    mappings = mapping.get("mappings", {})
    return [h for h in file_headers if h not in mappings or mappings[h] is None]

def generate_mapping_suggestions(mapping: Dict, file_headers: List[str], db_columns: List[Dict]) -> Dict[str, List[str]]:
    """
    Generate alternative mapping suggestions for unmapped headers.
    """
    suggestions = {}
    db_field_names = [col["COLUMN_NAME"] for col in db_columns]
    
    unmapped = find_unmapped_headers(mapping, file_headers)
    
    for header in unmapped:
        # Generate suggestions based on similarity
        similar_fields = []
        header_lower = header.lower()
        
        for db_col in db_field_names:
            db_col_lower = db_col.lower()
            if any(word in db_col_lower for word in header_lower.split() if len(word) > 2):
                similar_fields.append(db_col)
        
        suggestions[header] = similar_fields[:3]  # Top 3 suggestions
    
    return suggestions

def create_fallback_mapping(file_headers: List[str], db_columns: List[Dict]) -> Dict[str, str]:
    """
    Create a basic fallback mapping when AI fails.
    """
    db_field_names = [col["COLUMN_NAME"] for col in db_columns]
    fallback = {}
    
    for header in file_headers:
        best_match = find_best_fuzzy_match(header, db_field_names)
        if best_match:
            fallback[header] = best_match
    
    return fallback

